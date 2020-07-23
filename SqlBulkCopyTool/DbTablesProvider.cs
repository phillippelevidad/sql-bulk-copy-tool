using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using System.Linq;

namespace SqlBulkCopyTool
{
    public class DbTablesProvider
    {
        private readonly Logger logger;

        public DbTablesProvider(Logger logger)
        {
            this.logger = logger;
        }

        public ReadOnlyCollection<string> ListTableNames(SqlConnection connection)
        {
            return FilterHangfireTables(
                FetchTableNamesFromDatabase(connection));
        }

        private ReadOnlyCollection<string> FetchTableNamesFromDatabase(SqlConnection connection)
        {
            logger.Log("Fetching table names from the database...");

            var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                WITH cte (lvl, object_id, name, schema_Name) AS (
	                SELECT 1, object_id, sys.tables.name, sys.schemas.name as schema_Name
	                FROM sys.tables Inner Join sys.schemas on sys.tables.schema_id = sys.schemas.schema_id
	                WHERE type_desc = 'USER_TABLE' AND is_ms_shipped = 0
	                UNION ALL SELECT cte.lvl + 1, t.object_id, t.name, S.name as schema_Name
	                FROM cte
	                JOIN sys.tables AS t ON EXISTS (
		                SELECT NULL FROM sys.foreign_keys AS fk
		                WHERE fk.parent_object_id = t.object_id AND fk.referenced_object_id = cte.object_id)
	                JOIN sys.schemas as S on t.schema_id = S.schema_id AND t.object_id <> cte.object_id AND cte.lvl < 30
	                WHERE t.type_desc = 'USER_TABLE' AND t.is_ms_shipped = 0)
                SELECT schema_Name + '.[' + name + ']' table_name, MAX (lvl) AS dependency_level
                FROM cte
                GROUP BY schema_Name, name
                ORDER BY dependency_level DESC, schema_Name, name";

            var reader = cmd.ExecuteReader();
            var names = new List<string>(100);

            while (reader.Read()) names.Add(reader.GetString(0));
            return names.AsReadOnly();
        }

        private ReadOnlyCollection<string> FilterHangfireTables(IEnumerable<string> tables)
            => tables.Where(table => !table.Contains("hangfire", StringComparison.OrdinalIgnoreCase)).ToList().AsReadOnly();
    }
}
