# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaCreation < AbstractAdapter::SchemaCreation# :nodoc:

        def visit_AddColumnDefinition(o)
          sql = +"ADD COLUMN #{accept(o.column)}"
          sql << " AFTER " + quote_column_name(o.column.options[:after]) if o.column.options.key?(:after)
          sql
        end

        def add_column_options!(sql, options)
          if options[:null] || options[:null].nil?
            sql.gsub!(/\s+(.*)/, ' Nullable(\1)')
          end
          sql.gsub!(/(\sString)\(\d+\)/, '\1')
          sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if options_include_default?(options)
          sql
        end

        def add_table_options!(create_sql, options)
          is_view = create_sql.match(/^CREATE\s+((MATERIALIZED|LIVE)\s+)?VIEW/)
          if options[:options].present? && is_view && create_sql.match(/^CREATE\s+LIVE\s+VIEW\s+/)
            create_sql.replace(options[:options])
          elsif options[:options].present? && is_view && !create_sql.match(/^CREATE\s+MATERIALIZED\s+/)
            create_sql << options[:options]
          elsif options[:options].present? && is_view && options[:options].match(/(^|\s)TO\s+/)
            options[:options] = "TO #{options[:options].gsub(/^(?:.*?) TO (.*?)$/, '\\1')}"
            create_sql << options[:options]
          elsif options[:options].present?
            create_sql << " ENGINE = #{options[:options]}"
          else
            create_sql << " ENGINE = Log()"
          end

          create_sql
        end

        def visit_TableDefinition(o)
          create_sql = +"CREATE#{table_modifier_in_create(o)} #{o.view ? "VIEW" : "TABLE"} "
          create_sql << "IF NOT EXISTS " if o.if_not_exists
          create_sql << "#{quote_table_name(o.name)} "

          statements = o.columns.map { |c| accept c }
          statements << accept(o.primary_keys) if o.primary_keys

          create_sql << "(#{statements.join(', ')})" if statements.present?
          add_table_options!(create_sql, table_options(o))
          create_sql << " AS #{to_sql(o.as)}" if o.as
          create_sql
        end

        # Returns any SQL string to go between CREATE and TABLE. May be nil.
        def table_modifier_in_create(o)
          return " TEMPORARY" if o.temporary
          return " MATERIALIZED" if o.materialized
          return " LIVE" if o.live
        end

        def visit_ChangeColumnDefinition(o)
          column = o.column
          column.sql_type = type_to_sql(column.type, column.options)
          options = column_options(column)

          quoted_column_name = quote_column_name(o.name)
          type = column.sql_type
          type = "Nullable(#{type})" if options[:null]
          change_column_sql = +"MODIFY COLUMN #{quoted_column_name} #{type}"

          if options.key?(:default)
            quoted_default = quote_default_expression(options[:default], column)
            change_column_sql << " DEFAULT #{quoted_default}"
          end

          change_column_sql
        end

      end
    end
  end
end
