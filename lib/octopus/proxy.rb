require 'set'

module Octopus
  class Proxy
    attr_accessor :config, :sharded
    attr_reader :shards

    def initialize(config = Octopus.config)
      initialize_shards(config)
    end

    def initialize_shards(config)
      @shards = HashWithIndifferentAccess.new
      @groups = {}
      @adapters = Set.new
      @config = ActiveRecord::Base.connection_pool_without_octopus.connection.instance_variable_get(:@config)

      unless config.nil?
        @entire_sharded = config['entire_sharded']
        @shards_config = config[Octopus.rails_env]
      end

      @shards_config ||= []

      @shards_config.each do |key, value|
        if value.is_a?(String)
          value = resolve_string_connection(value).merge(:octopus_shard => key)
          initialize_adapter(value['adapter'])
          @shards[key.to_sym] = connection_pool_for(value, "#{value['adapter']}_connection")
        elsif value.is_a?(Hash) && value.key?('adapter')
          value.merge!(:octopus_shard => key)
          initialize_adapter(value['adapter'])
          @shards[key.to_sym] = connection_pool_for(value, "#{value['adapter']}_connection")
        elsif value.is_a?(Hash)
          @groups[key.to_s] = []

          value.each do |k, v|
            fail 'You have duplicated shard names!' if @shards.key?(k.to_sym)

            initialize_adapter(v['adapter'])
            config_with_octopus_shard = v.merge(:octopus_shard => k)

            @shards[k.to_sym] = connection_pool_for(config_with_octopus_shard, "#{v['adapter']}_connection")
            @groups[key.to_s] << k.to_sym
          end
        end
      end

      @shards[:master] ||= ActiveRecord::Base.connection_pool_without_octopus
    end

    def current_model
      Thread.current['octopus.current_model']
    end

    def current_model=(model)
      Thread.current['octopus.current_model'] = model.is_a?(ActiveRecord::Base) ? model.class : model
    end

    def current_shard
      Thread.current['octopus.current_shard'] ||= :master
    end

    def current_shard=(shard_symbol)
      if shard_symbol.is_a?(Array)
        shard_symbol.each { |symbol| fail "Nonexistent Shard Name: #{symbol}" if @shards[symbol].nil? }
      elsif shard_symbol.is_a?(Hash)
        hash = shard_symbol
        shard_symbol = hash[:shard]

        if shard_symbol.nil? && slave_group_symbol.nil?
          fail 'Neither shard or slave group must be specified'
        end

        if shard_symbol.present?
          fail "Nonexistent Shard Name: #{shard_symbol}" if @shards[shard_symbol].nil?
        end
      else
        fail "Nonexistent Shard Name: #{shard_symbol}" if @shards[shard_symbol].nil?
      end

      Thread.current['octopus.current_shard'] = shard_symbol
    end

    def current_group
      Thread.current['octopus.current_group']
    end

    def current_group=(group_symbol)
      # TODO: Error message should include all groups if given more than one bad name.
      [group_symbol].flatten.compact.each do |group|
        fail "Nonexistent Group Name: #{group}" unless has_group?(group)
      end

      Thread.current['octopus.current_group'] = group_symbol
    end

    def in_block?
      Thread.current['octopus.in_block'] || false
    end

    def in_block=(in_block)
      Thread.current['octopus.in_block'] = in_block
    end

    def last_current_shard
      Thread.current['octopus.last_current_shard']
    end

    def last_current_shard=(last_current_shard)
      Thread.current['octopus.last_current_shard'] = last_current_shard
    end

    # Public: Whether or not a group exists with the given name converted to a
    # string.
    #
    # Returns a boolean.
    def has_group?(group)
      @groups.key?(group.to_s)
    end

    # Public: Retrieves names of all loaded shards.
    #
    # Returns an array of shard names as symbols
    def shard_names
      @shards.keys
    end

    # Public: Retrieves the defined shards for a given group.
    #
    # Returns an array of shard names as symbols or nil if the group is not
    # defined.
    def shards_for_group(group)
      @groups.fetch(group.to_s, nil)
    end

    # Rails 3.1 sets automatic_reconnect to false when it removes
    # connection pool.  Octopus can potentially retain a reference to a closed
    # connection pool.  Previously, that would work since the pool would just
    # reconnect, but in Rails 3.1 the flag prevents this.
    def safe_connection(connection_pool)
      connection_pool.automatic_reconnect ||= true
      connection_pool.connection
    end

    def select_connection
      safe_connection(@shards[shard_name])
    end

    def shard_name
      current_shard.is_a?(Array) ? current_shard.first : current_shard
    end

    def run_queries_on_shard(shard, args={}, &_block)
      keeping_connection_proxy(args) do
        using_shard(shard) do
          yield
        end
      end
    end

    def send_queries_to_multiple_shards(shards, &block)
      shards.each do |shard|
        run_queries_on_shard(shard, &block)
      end
    end

    def clean_connection_proxy
      self.current_shard = :master
      self.current_group = nil
      self.in_block = false
    end

    def select_db(name)
      slave_connection.select_db(name)
      master_connection.select_db(name) if current_shard != master_shard_name
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
      if should_clean_connection_proxy?(method)
        conn = select_connection
        self.last_current_shard = current_shard
        clean_connection_proxy
        conn.send(method, *args, &block)
      elsif in_block?
        if should_use_slave?(method)
          slave_connection.send(method, *args, &block)
        else
          master_connection.send(method, *args, &block)
        end
      else
        select_connection.send(method, *args, &block)
      end
    end

    def respond_to?(method, include_private = false)
      super || select_connection.respond_to?(method, include_private)
    end

    def connection_pool
      @shards[current_shard]
    end

    def enable_query_cache!
      clear_query_cache
      connected_shards.each { |_k, v| safe_connection(v).enable_query_cache! }
    end

    def disable_query_cache!
      connected_shards.each { |_k, v| safe_connection(v).disable_query_cache! }
    end

    def clear_query_cache
      connected_shards.each { |_k, v| safe_connection(v).clear_query_cache }
    end

    def clear_active_connections!
      connected_shards.each { |_k, v| v.release_connection }
    end

    def clear_all_connections!
      connected_shards.each { |_k, v| v.disconnect! }
    end

    def connected?
      @shards.any? { |_k, v| v.connected? }
    end

    protected

    def connection_pool_for(adapter, config)
      arg = ActiveRecord::ConnectionAdapters::ConnectionSpecification.new(adapter.dup, config)
      ActiveRecord::ConnectionAdapters::ConnectionPool.new(arg)
    end

    def initialize_adapter(adapter)
      @adapters << adapter
      begin
        require "active_record/connection_adapters/#{adapter}_adapter"
      rescue LoadError
        raise "Please install the #{adapter} adapter: `gem install activerecord-#{adapter}-adapter` (#{$ERROR_INFO})"
      end
    end

    def resolve_string_connection(spec)
      resolver = ActiveRecord::ConnectionAdapters::ConnectionSpecification::Resolver.new({})
      HashWithIndifferentAccess.new(resolver.spec(spec).config)
    end

    def should_clean_connection_proxy?(method)
      method.to_s =~ /insert|select|execute/ && !in_block?
    end

    # Temporarily block cleaning connection proxy and run the block
    #
    # @see Octopus::Proxy#should_clean_connection?
    # @see Octopus::Proxy#clean_connection_proxy
    def keeping_connection_proxy(args, &_block)
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

      begin
        self.current_shard = shard
        yield
      ensure
        self.current_shard = older_shard
      end
    end

    private
      def master_connection
        master_shard_name ? safe_connection(@shards[master_shard_name]) : select_connection
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

      def connected_shards
        @shards.select {|_k, v| v.connected?}
      end
  end
end
