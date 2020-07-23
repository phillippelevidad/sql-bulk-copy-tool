using CSharpFunctionalExtensions;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlBulkCopyTool
{
    public class BulkCopyHelper
    {
        private const int timeout = 180;
        private readonly Logger logger;

        public BulkCopyHelper(Logger logger)
        {
            this.logger = logger;
        }

        public Result CopyData(SqlConnection sourceConnection, SqlConnection targetConnection, IEnumerable<string> tableNames, SqlTransaction transaction)
        {
            foreach (var tableName in tableNames) CopyTableData(sourceConnection, targetConnection, tableName, transaction);
            return Result.Success();
        }

        private void CopyTableData(SqlConnection sourceConnection, SqlConnection targetConnection, string tableName, SqlTransaction transaction)
        {
            logger.Log($"Copying table {tableName}");

            var reader = ReadTableData(sourceConnection, tableName);
            WriteTableData(targetConnection, tableName, reader, transaction);
            reader.Close();
        }

        private SqlDataReader ReadTableData(SqlConnection connection, string tableName)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName}";
            cmd.CommandTimeout = timeout;
            return cmd.ExecuteReader();
        }

        private void WriteTableData(SqlConnection connection, string tableName, SqlDataReader reader, SqlTransaction transaction)
        {
            var options = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls;

            using (var bulkCopy = new SqlBulkCopy(connection, options, transaction))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BulkCopyTimeout = timeout;
                bulkCopy.WriteToServer(reader);
            }
        }
    }
}
