module MoSQL
  class SchemaError < StandardError; end;

  class Schema
    include MoSQL::Logging

    def to_array(lst)
      array = []
      lst.each do |ent|
        if ent.is_a?(Hash) && ent[:source].is_a?(String) && ent[:type].is_a?(String)
          # new configuration format
          array << {
            :source => ent.delete(:source),
            :type   => ent.delete(:type),
            :name   => ent.first.first,
          }
        elsif ent.is_a?(Hash) && ent.keys.length == 1 && ent.values.first.is_a?(String)
          array << {
            :source => ent.first.first,
            :name   => ent.first.first,
            :type   => ent.first.last
          }
        else
          raise "Invalid ordered hash entry #{ent.inspect}"
        end

      end
      array
    end

    def check_columns!(ns, spec)
      seen = Set.new
      spec[:columns].each do |col|
        if seen.include?(col[:source])
          raise "Duplicate source #{col[:source]} in column definition #{col[:name]} for #{ns}."
        end
        seen.add(col[:source])
      end
    end

    def parse_spec(ns, spec)
      out = spec.dup
      out[:columns] = to_array(spec[:columns])
      check_columns!(ns, out)
      add_created(out) if out[:meta][:created_at]

      out[:related] ||= []
      out[:related].each do |reltable, details|
        out[:related][reltable] = to_array(details)
      end

      out
    end

    def add_created(spec)
      spec[:columns] << {
        :source => '_id',
        :type   => 'TIMESTAMP',
        :name   => 'createdAt',
        :key    => false
      }
    end

    def initialize(map)
      @map = {}
      map.each do |dbname, db|
        @map[dbname] ||= {}
        db.each do |cname, spec|
          @map[dbname][cname] = parse_spec("#{dbname}.#{cname}", spec)
        end
      end
    end

    def create_schema(db, clobber=false)
      @map.values.map(&:values).flatten.each do |collection|
        meta = collection[:meta]
        log.info("Creating table '#{meta[:table]}'...")
        db.send(clobber ? :create_table! : :create_table?, meta[:table]) do
          collection[:columns].each do |col|
            column col[:name], col[:type]

            if col[:source].to_sym == :_id && col[:key] != false
              primary_key [col[:name].to_sym]
            end
          end
          if meta[:extra_props]
            column '_extra_props', 'TEXT'
          end
        end

        # Add relational tables
        collection[:related].each do |reltable, details|
          db.send(clobber ? :create_table! : :create_table?, reltable) do
            primary_key :__id

            details.each do |col|
              column col[:name], col[:type]
            end
          end
        end
      end
    end

    def find_ns(ns)
      db, collection, relation = ns.split(".")
      schema = (@map[db] || {})[collection]
      if schema && relation
        schema = {
          :columns => schema[:related][relation],
          :meta => { :table => relation }
        }
      end

      if schema.nil?
        log.debug("No mapping for ns: #{ns}")
        return nil
      end
      schema
    end

    def find_ns!(ns)
      schema = find_ns(ns)
      raise SchemaError.new("No mapping for namespace: #{ns}") if schema.nil?
      schema
    end

    def fetch_and_delete_dotted(obj, dotted)
      key, rest = dotted.split(".", 2)

      if key.end_with?("[]")
        values = obj[key.slice(0...-2)] || []
        raise "Expected: Array for piece #{ key }, got #{ values.class }" unless values.is_a?(Array)

        return values.map do |v|
          if rest
            fetch_and_delete_dotted(v, rest)
          else
            v
          end
        end
      end

      # Base case
      return obj[key] unless rest

      fetch_and_delete_dotted(obj[key], rest)
    end

    def transform(ns, obj, schema=nil)
      schema ||= find_ns!(ns)

      obj = obj.dup
      row = []
      sources = {}
      schema[:columns].each do |col|

        source = col[:source]
        type = col[:type]

        unless sources.include?(source)
          sources[source] = fetch_and_delete_dotted(obj, source)
        end
        v = sources[source]

        case v
        when BSON::Binary, BSON::ObjectId
          if [:DATE, :TIMESTAMP, :TIME].include? type.to_sym
            v = Time.at v.to_s[0...8].to_i(16)
          else
            v = v.to_s
          end
        end
        row << v
      end

      if schema[:meta][:extra_props]
        # Kludgily delete binary blobs from _extra_props -- they may
        # contain invalid UTF-8, which to_json will not properly encode.
        sources = schema[:columns].map{|c| c[:source]}.uniq
        obj.each do |k,v|
          obj.delete(k) if v.is_a?(BSON::Binary) or sources.include?(k)
        end
        row << obj.to_json
      end

      log.debug { "Transformed: #{row.inspect}" }

      depth = row.select {|r| r.is_a? Array}.map {|r| [r].flatten.length }.max
      return row unless depth

      # Convert row [a, [b, c], d] into [[a, b, d], [a, c, d]]
      row.map! {|r| [r].flatten.cycle.take(depth)}
      row.first.zip(*row.drop(1))
    end

    def all_columns(schema)
      cols = []
      schema[:columns].each do |col|
        cols << col[:name]
      end
      if schema[:meta][:extra_props]
        cols << "_extra_props"
      end
      cols
    end

    def copy_data(db, ns, objs)
      schema = find_ns!(ns)
      db.synchronize do |pg|
        sql = "COPY \"#{schema[:meta][:table]}\" " +
          "(#{all_columns(schema).map {|c| "\"#{c}\""}.join(",")}) FROM STDIN"
        pg.execute(sql)
        objs.each do |o|
          pg.put_copy_data(transform_to_copy(ns, o, schema) + "\n")
        end
        pg.put_copy_end
        begin
          pg.get_result.check
        rescue PGError => e
          db.send(:raise_error, e)
        end
      end
    end

    def quote_copy(val)
      case val
      when nil
        "\\N"
      when true
        't'
      when false
        'f'
      else
        val.to_s.gsub(/([\\\t\n\r])/, '\\\\\\1')
      end
    end

    def transform_to_copy(ns, row, schema=nil)
      row.map { |c| quote_copy(c) }.join("\t")
    end

    def table_for_ns(ns)
      find_ns!(ns)[:meta][:table]
    end

    def all_mongo_dbs
      @map.keys
    end

    def collections_for_mongo_db(db)
      (@map[db]||{}).keys
    end

    def primary_sql_key_for_ns(ns)
      find_ns!(ns)[:columns].find {|c| c[:source] == '_id' && c[:key] != false}[:name]
    end
  end
end
