using CSharpFunctionalExtensions;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SqlBulkCopyTool
{
    public class DbCleaner
    {
        private readonly Logger logger;

        public DbCleaner(Logger logger)
        {
            this.logger = logger;
        }

        public Result CleanTables(SqlConnection connection, IEnumerable<string> tableNames, SqlTransaction transaction)
        {
            var deleteStatements = GetDeleteStatements(tableNames);
            var maxAttempts = tableNames.Count() + 5;
            string lastError = null;

            for (int attempts = 0; attempts < maxAttempts; attempts++)
            {
                logger.Log($"Attempting deletion #{attempts + 1}");

                var result = AttemptDelete(connection, deleteStatements, transaction);
                if (result.IsSuccess) return result;

                lastError = result.Error;
            }

            return Result.Failure("Error deleting data: " + lastError);
        }

        private string GetDeleteStatements(IEnumerable<string> tableNames)
        {
            var statements = tableNames.Select(name => $"DELETE FROM {name}");
            return string.Join(Environment.NewLine, statements);
        }

        private Result AttemptDelete(SqlConnection connection, string deleteStatements, SqlTransaction transaction)
        {
            try
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = deleteStatements;
                cmd.Transaction = transaction;
                cmd.ExecuteNonQuery();
                return Result.Ok();
            }
            catch (SqlException ex)
            {
                return Result.Failure(ex.Message);
            }
        }
    }
}
