module MoSQL
  class SQLAdapter
    include MoSQL::Logging

    attr_reader :db

    def initialize(schema, uri, pgschema=nil)
      @schema = schema
      connect_db(uri, pgschema)
    end

    def connect_db(uri, pgschema)
      @db = Sequel.connect(uri, :after_connect => proc do |conn|
                             if pgschema
                               begin
                                 conn.execute("CREATE SCHEMA \"#{pgschema}\"")
                               rescue PG::Error
                               end
                               conn.execute("SET search_path TO \"#{pgschema}\"")
                             end
                           end)
    end

    def table_for_ns(ns)
      @db[@schema.table_for_ns(ns).intern]
    end

    def transform_one_ns(ns, obj)
      cols = @schema.all_columns(@schema.find_ns(ns))
      row  = @schema.transform(ns, obj)
      Hash[cols.zip(row)]
    end

    def transform_related_ns(ns, obj)
      cols = @schema.all_columns(@schema.find_ns(ns))
      @schema.transform(ns, obj).map do |row|
        Hash[cols.zip(row)]
      end
    end

    def upsert_ns(ns, obj)
      h = transform_one_ns(ns, obj)
      upsert(table_for_ns(ns), @schema.primary_sql_key_for_ns(ns), h)
    end

    def foreign_query_ns(ns, obj)
      query = {}
      @schema.foreign_keys(ns).each do |source,key|
        query[key.intern] = obj[source]
      end
      query
    end

    def _sync_related_ns(rns, obj)
      tbl = table_for_ns(rns)
      ids = tbl.where(foreign_query_ns(rns, obj)).order(:__id).map { |i| i[:__id] }
      transform_related_ns(rns, obj).each do |h|
        if id = ids.shift
          # There's something to update
          tbl.where(__id: id).update(h)
        else
          # It's an insert
          tbl.insert(h)
        end
      end
      tbl.where('__id IN ?', ids).delete if ids.any?
    end
    private :_sync_related_ns

    def sync_related_ns(ns, obj)
      rel = @schema.find_ns(ns)[:related]
      if rel.is_a? Hash
        rel.keys.each do |rns|
          _sync_related_ns "#{ns}.#{rns}", obj
        end
      end
    end

    # obj must contain an _id field. All other fields will be ignored.# {{{
    def delete_ns(ns, obj)
      primary_sql_key = @schema.primary_sql_key_for_ns(ns)
      h = transform_one_ns(ns, obj)
      raise "No #{primary_sql_key} found in transform of #{obj.inspect}" if h[primary_sql_key].nil?
      table_for_ns(ns).where(primary_sql_key.to_sym => h[primary_sql_key]).delete
    end# }}}

    def delete_related_ns(ns, obj)
      if rel = @schema.find_ns(ns)[:related]
        rel.keys.each do |rns|
          rns = "#{ns}.#{rns}"
          table_for_ns(rns).where(foreign_query_ns(rns, obj)).delete
        end
      end
    end

    def upsert(table, table_primary_key, item)# {{{
      begin
        upsert!(table, table_primary_key, item)
      rescue Sequel::DatabaseError => e
        wrapped = e.wrapped_exception
        if wrapped.result
          log.warn("Ignoring row (#{table_primary_key}=#{item[table_primary_key]}): #{e}")
        else
          raise e
        end
      end
    end# }}}

    def upsert!(table, table_primary_key, item)
      rows = table.where(table_primary_key.to_sym => item[table_primary_key]).update(item)
      if rows == 0
        begin
          table.insert(item)
        rescue Sequel::DatabaseError => e
          raise e unless e.message =~ /duplicate key value violates unique constraint/
          log.info("RACE during upsert: Upserting #{item} into #{table}: #{e}")
        end
      elsif rows > 1
        log.warn("Huh? Updated #{rows} > 1 rows: upsert(#{table}, #{item})")
      end
    end
  end
end

