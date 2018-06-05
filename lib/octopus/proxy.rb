require 'set'
require 'octopus/slave_group'
require 'octopus/load_balancing/round_robin'

module Octopus
  class Proxy
    attr_accessor :proxy_config

    delegate :current_model, :current_model=,
             :current_shard, :current_shard=,
             :current_group, :current_group=,
             :current_load_balance_options, :current_load_balance_options=,
             :in_block?, :in_block=, :fully_replicated?, :has_group?,
             :shard_names, :shards_for_group, :shards, :sharded, :slaves_list,
             :shards_slave_groups, :slave_groups, :replicated, :slaves_load_balancer,
             :config, :initialize_shards, :shard_name, to: :proxy_config, prefix: false

    def initialize(config = Octopus.config)
      self.proxy_config = Octopus::ProxyConfig.new(config)
    end

    # Rails Connection Methods - Those methods are overriden to add custom behavior that helps
    # Octopus introduce Sharding / Replication.
    delegate :adapter_name, :add_transaction_record, :case_sensitive_modifier,
      :type_cast, :to_sql, :quote, :quote_column_name, :quote_table_name,
      :quote_table_name_for_assignment, :supports_migrations?, :table_alias_for,
      :table_exists?, :in_clause_length, :supports_ddl_transactions?,
      :sanitize_limit, :prefetch_primary_key?, :current_database, :initialize_schema_migrations_table,
      :combine_bind_parameters, :empty_insert_statement_value, :assume_migrated_upto_version,
      :schema_cache, :substitute_at, :internal_string_options_for_primary_key, :lookup_cast_type_from_column,
      :supports_advisory_locks?, :get_advisory_lock, :initialize_internal_metadata_table,
      :release_advisory_lock, :prepare_binds_for_database, :cacheable_query, :column_name_for_operation,
      :prepared_statements, :transaction_state, :create_table, to: :select_connection

    # Rails 3.1 sets automatic_reconnect to false when it removes
    # connection pool.  Octopus can potentially retain a reference to a closed
    # connection pool.  Previously, that would work since the pool would just
    # reconnect, but in Rails 3.1 the flag prevents this.
    def safe_connection(connection_pool)
      connection_pool.automatic_reconnect ||= true
      if !connection_pool.connected? && shards[Octopus.master_shard].connection.query_cache_enabled
        connection_pool.connection.enable_query_cache!
      end
      connection_pool.connection
    end

    def select_connection
      safe_connection(shards[shard_name])
    end

    def run_queries_on_shard(shard, args={}, &_block)
      keeping_connection_proxy(shard, args) do
        using_shard(shard) do
          yield
        end
      end
    end

    def send_queries_to_multiple_shards(shards, &block)
      shards.map do |shard|
        run_queries_on_shard(shard, &block)
      end
    end

    def send_queries_to_group(group, &block)
      using_group(group) do
        send_queries_to_multiple_shards(shards_for_group(group), &block)
      end
    end

    def send_queries_to_all_shards(&block)
      send_queries_to_multiple_shards(shard_names.uniq { |shard_name| shards[shard_name] }, &block)
    end

    def clean_connection_proxy
      self.current_shard = Octopus.master_shard
      self.current_model = nil
      self.current_group = nil
      self.block = nil
      self.in_block = false
    end

    def select_db(name)
      slave_connection.select_db(name)
      master_connection.select_db(name) if current_shard != master_shard_name
    end

    def check_schema_migrations(shard)
      OctopusModel.using(shard).connection.table_exists?(
        ActiveRecord::Migrator.schema_migrations_table_name,
      ) || OctopusModel.using(shard).connection.initialize_schema_migrations_table
    end

    def transaction(options = {}, &block)
      if in_block?
        old_in_transaction = @in_transaction
        begin
          @in_transaction = true
          master_connection.transaction(options, &block)
        ensure
          @in_transaction = old_in_transaction
        end
      else
        select_connection.transaction(options, &block)
      end
    end

    def method_missing(method, *args, &block)
      legacy_method_missing_logic(method, *args, &block)
    end

    def respond_to?(method, include_private = false)
      super || select_connection.respond_to?(method, include_private)
    end

    def connection_pool
      shards[current_shard]
    end

    def enable_query_cache!
      clear_query_cache
      with_each_healthy_shard { |v| v.connected? && safe_connection(v).enable_query_cache! }
    end

    def disable_query_cache!
      with_each_healthy_shard { |v| v.connected? && safe_connection(v).disable_query_cache! }
    end

    def clear_query_cache
      with_each_healthy_shard { |v| v.connected? && safe_connection(v).clear_query_cache }
    end

    def clear_active_connections!
      with_each_healthy_shard(&:release_connection)
    end

    def clear_all_connections!
      with_each_healthy_shard(&:disconnect!)
    end

    def connected?
      shards.any? { |_k, v| v.connected? }
    end

    def current_model_replicated?
      replicated && (current_model.try(:replicated) || fully_replicated?)
    end

    protected

    # @thiagopradi - This legacy method missing logic will be keep for a while for compatibility
    # and will be removed when Octopus 1.0 will be released.
    # We are planning to migrate to a much stable logic for the Proxy that doesn't require method missing.
    def legacy_method_missing_logic(method, *args, &block)
      if should_clean_connection_proxy?(method)
        conn = select_connection
        clean_connection_proxy
      elsif in_block?
        conn = should_use_slave?(method) ? slave_connection : master_connection
      else
        conn = select_connection
      end
      conn.send(method, *args, &block)
    end

    # Ensure that a single failing slave doesn't take down the entire application
    def with_each_healthy_shard
      shards.each do |shard_name, v|
        begin
          yield(v)
        rescue => e
          if Octopus.robust_environment?
            Octopus.logger.error "Error on shard #{shard_name}: #{e.message}"
          else
            raise
          end
        end
      end

      ar_pools = ActiveRecord::Base.connection_handler.connection_pool_list

      ar_pools.each do |pool|
        next if pool == shards[:master] # Already handled this

        begin
          yield(pool)
        rescue => e
          if Octopus.robust_environment?
            Octopus.logger.error "Error on pool (spec: #{pool.spec}): #{e.message}"
          else
            raise
          end
        end
      end
    end

    def should_clean_connection_proxy?(method)
      method.to_s =~ /insert|select|execute/ && !in_block?
    end

    # Try to use slaves if and only if `replicated: true` is specified in `shards.yml` and no slaves groups are defined
    def should_send_queries_to_replicated_databases?(method)
      replicated && method.to_s =~ /select/ && !block && !slaves_grouped?
    end

    def send_queries_to_selected_slave(method, *args, &block)
      if current_model.replicated || fully_replicated?
        selected_slave = slaves_load_balancer.next current_load_balance_options
      else
        selected_slave = Octopus.master_shard
      end

      send_queries_to_slave(selected_slave, method, *args, &block)
    end

    # We should use slaves if and only if its safe to do so.
    #
    # We can safely use slaves when:
    # (1) `replicated: true` is specified in `shards.yml`
    # (2) The current model is `replicated()`, or `fully_replicated: true` is specified in `shards.yml` which means that
    #     all the model is `replicated()`
    # (3) It's a SELECT query
    # while ensuring that we revert `current_shard` from the selected slave to the (shard's) master
    # not to make queries other than SELECT leak to the slave.
    def should_use_slaves_for_method?(method)
      current_model_replicated? && method.to_s =~ /select/
    end

    def slaves_grouped?
      slave_groups.present?
    end

    # Temporarily switch `current_shard` to the next slave in a slave group and send queries to it
    # while preserving `current_shard`
    def send_queries_to_balancer(balancer, method, *args, &block)
      send_queries_to_slave(balancer.next(current_load_balance_options), method, *args, &block)
    end

    # Temporarily switch `current_shard` to the specified slave and send queries to it
    # while preserving `current_shard`
    def send_queries_to_slave(slave, method, *args, &block)
      using_shard(slave) do
        val = select_connection.send(method, *args, &block)
        if val.instance_of? ActiveRecord::Result
          val.current_shard = slave
        end
        val
      end
    end

    # Temporarily block cleaning connection proxy and run the block
    #
    # @see Octopus::Proxy#should_clean_connection?
    # @see Octopus::Proxy#clean_connection_proxy
    def keeping_connection_proxy(shard, args, &_block)
      last_in_block = in_block?
      last_master_shard_name = master_shard_name

      begin
        self.in_block = true
        self.master_shard_name = args[:master_shard_name] if args[:master_shard_name]
        yield
      ensure
        self.in_block = last_in_block
        self.master_shard_name = last_master_shard_name
      end
    end

    # Temporarily switch `current_shard` and run the block
    def using_shard(shard, &_block)
      older_shard = current_shard
      older_load_balance_options = current_load_balance_options

      begin
        unless current_model && !current_model.allowed_shard?(shard)
          self.current_shard = shard
        end
        yield
      ensure
        self.current_shard = older_shard
        self.current_load_balance_options = older_load_balance_options
      end
    end

    # Temporarily switch `current_group` and run the block
    def using_group(group, &_block)
      older_group = current_group

      begin
        self.current_group = group
        yield
      ensure
        self.current_group = older_group
      end
    end

    private

    def master_connection
      master_shard_name ? safe_connection(shards[master_shard_name]) : select_connection
    end
          
    def master_shard_name
      Thread.current['octopus.master_shard_name']
    end

    def master_shard_name=(master_shard_name)
      Thread.current['octopus.master_shard_name'] = master_shard_name
    end

    def slave_connection
      select_connection
    end

    def should_use_slave?(method)
      !@in_transaction && method.to_s =~ /select/
    end
  end
end
