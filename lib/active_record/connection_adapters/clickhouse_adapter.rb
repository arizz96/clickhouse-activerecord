require 'clickhouse-activerecord/arel/visitors/to_sql'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'
require 'active_record/connection_adapters/clickhouse/schema_definitions'
require 'active_record/connection_adapters/clickhouse/schema_creation'
require 'active_record/connection_adapters/clickhouse/schema_statements'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    VALID_CONN_PARAMS = [:host, :port, :database, :user, :password]

    # Establishes a connection to the database that's used by all Active Record objects
    def clickhouse_connection(config)
      conn_params = config.symbolize_keys

      conn_params.delete_if { |_, v| v.nil? }

      # Forward only valid config params to conn.connect.
      conn_params.keep_if { |k, _| VALID_CONN_PARAMS.include?(k) }

      # The clickhouse drivers don't allow the creation of an unconnected conn object,
      # so just pass a nil connection object for the time being.
      ConnectionAdapters::ClickhouseAdapter.new(nil, logger, conn_params, config)
    end
  end

  module ModelSchema
     module ClassMethods
      def is_view
        @is_view || false
      end
       # @param [Boolean] value
      def is_view=(value)
        @is_view = value
      end
    end
   end

  module ConnectionAdapters
    class ClickhouseAdapter < AbstractAdapter
      ADAPTER_NAME = 'Clickhouse'.freeze

      NATIVE_DATABASE_TYPES = {
        string:      { name: 'String' },
        integer:     { name: 'UInt32' },
        big_integer: { name: 'UInt64' },
        float:       { name: 'Float32' },
        decimal:     { name: 'Decimal' },
        datetime:    { name: 'DateTime' },
        date:        { name: 'Date' },
        boolean:     { name: 'UInt8' }
      }.freeze

      include Clickhouse::SchemaStatements

      # Initializes and connects a Clickhouse adapter.
      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)

        @visitor = ClickhouseActiverecord::Arel::Visitors::Clickhouse.new self
        @prepared_statements = false
        @connection_parameters, @config = connection_parameters, config

        connect

        @type_map = Type::HashLookupTypeMap.new
        initialize_type_map(type_map)
      end

      # Is this connection alive and ready for queries?
      def active?
        @connection.query 'SELECT 1'
        true
      rescue ArgumentError
        false
      end

      # Close then reopen the connection.
      def reconnect!
        super
        @connection.reset
        configure_connection
      end

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        super
        @connection.close rescue nil
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      private

      def initialize_type_map(m) # :nodoc:
        super
        register_class_with_limit m, 'String', Type::String
        register_class_with_limit m, 'Nullable(String)', Type::String
        register_class_with_limit m, 'Uint8', Type::UnsignedInteger
        register_class_with_limit m, 'Date',  Clickhouse::OID::Date
        register_class_with_limit m, 'DateTime',  Clickhouse::OID::DateTime
        m.alias_type 'UInt16', 'uint4'
        m.alias_type 'UInt32', 'uint8'
        m.register_type 'UInt64', Clickhouse::OID::BigInteger.new
        m.alias_type 'Int8', 'int4'
        m.alias_type 'Int16', 'int4'
        m.alias_type 'Int32', 'int8'
        m.alias_type 'Int64', 'UInt64'
        m.register_type 'uuid', Clickhouse::OID::Uuid.new
      end

      def extract_limit(sql_type) # :nodoc:
        case sql_type
          when 'Nullable(String)'
            255
          when /Nullable\(U?Int(8|16)\)/
            4
          when /Nullable\(U?Int(32|64)\)/
            8
          else
            super
        end
      end

      # Extracts the value from a PostgreSQL column default definition.
      def extract_value_from_default(default)
        case default
          # Quoted types
        when /\A[\(B]?'(.*)'.*::"?([\w. ]+)"?(?:\[\])?\z/m
          # The default 'now'::date is CURRENT_DATE
          if $1 == "now".freeze && $2 == "date".freeze
            nil
          else
            $1.gsub("''".freeze, "'".freeze)
          end
          # Boolean types
        when "true".freeze, "false".freeze
          default
          # Numeric types
        when /\A\(?(-?\d+(\.\d*)?)\)?(::bigint)?\z/
          $1
          # Object identifier types
        when /\A-?\d+\z/
          $1
        else
          # Anything else is blank, some user type, or some function
          # and we can't know the value of that, so return nil.
          nil
        end
      end

      def extract_default_function(default_value, default) # :nodoc:
        default if has_default_function?(default_value, default)
      end

      def has_default_function?(default_value, default) # :nodoc:
        !default_value && (%r{\w+\(.*\)} === default)
      end

      def connect
        @connection = Net::HTTP.start(@connection_parameters[0], @connection_parameters[1])
      end

      # Configures the encoding, verbosity, schema search path, and time zone of the connection.
      # This is called by #connect and should not be called manually.
      def configure_connection
        # if @config[:encoding]
        #   @connection.set_client_encoding(@config[:encoding])
        # end
        # self.client_min_messages = @config[:min_messages] || 'warning'
        # self.schema_search_path = @config[:schema_search_path] || @config[:schema_order]
        #
        # # Use standard-conforming strings so we don't have to do the E'...' dance.
        # set_standard_conforming_strings
        #
        # # If using Active Record's time zone support configure the connection to return
        # # TIMESTAMP WITH ZONE types in UTC.
        # # (SET TIME ZONE does not use an equals sign like other SET variables)
        # if ActiveRecord::Base.default_timezone == :utc
        #   execute("SET time zone 'UTC'", 'SCHEMA')
        # elsif @local_tz
        #   execute("SET time zone '#{@local_tz}'", 'SCHEMA')
        # end
        #
        # # SET statements from :variables config hash
        # # http://www.postgresql.org/docs/8.3/static/sql-set.html
        # variables = @config[:variables] || {}
        # variables.map do |k, v|
        #   if v == ':default' || v == :default
        #     # Sets the value to the global or compile default
        #     execute("SET SESSION #{k} TO DEFAULT", 'SCHEMA')
        #   elsif !v.nil?
        #     execute("SET SESSION #{k} TO #{quote(v)}", 'SCHEMA')
        #   end
        # end
      end

      def last_inserted_id(result)
        result
      end
    end
  end
end
