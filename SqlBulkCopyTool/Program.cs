using CSharpFunctionalExtensions;
using System;
using System.Data.SqlClient;

namespace SqlBulkCopyTool
{
    class Program
    {
        static void Main()
        {
            // Set the source and target connection strings below
            // The copy only works if the database schemas match
            // Tables and table columns must be structurally equal

            // WARNING
            // All data in the target database will be deleted
            // Works best if the target database is brand new
            // Everything runs under a transaction to minimize risks

            Copy(
                sourceConnectionString: @"Server=.\sqlexpress;Database=sourcedatabase;Trusted_Connection=True;",
                targetConnectionString: @"Server=.\sqlexpress;Database=targetdatabase;Trusted_Connection=True;");
        }

        private static void Copy(string sourceConnectionString, string targetConnectionString)
        {
            var logger = new Logger();

            using (var sourceConnection = new SqlConnection(sourceConnectionString))
            using (var targetConnection = new SqlConnection(targetConnectionString))
            {
                logger.Log("Opening source connection...");
                sourceConnection.Open();

                logger.Log("Opening target connection...");
                targetConnection.Open();

                var transaction = targetConnection.BeginTransaction();

                try
                {
                    if (CopyInternal(logger, sourceConnection, targetConnection, transaction))
                    {
                        logger.Log("All good, committing transaction...");
                        transaction.Commit();
                        logger.LogSuccess("Success!");
                    }
                    else
                    {
                        logger.LogError("Something went wrong, rolling back...");
                        transaction.Rollback();
                        logger.Log("Rolled back");
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError("An error has ocurred: " + ex.Message);
                    logger.Log("Rolling back...");

                    transaction.Rollback();
                    logger.Log("Rolled back");
                }
            }

            logger.Log("Finished");
            Console.WriteLine("Press any key to finish...");
            Console.ReadLine();
        }

        private static bool CopyInternal(Logger logger, SqlConnection sourceConnection, SqlConnection targetConnection, SqlTransaction transaction)
        {
            var tableNames = new DbTablesProvider(logger).ListTableNames(sourceConnection);

            return new DbCleaner(logger).CleanTables(targetConnection, tableNames, transaction)
                .Tap(() => new BulkCopyHelper(logger).CopyData(sourceConnection, targetConnection, tableNames, transaction))
                .OnFailure(error => logger.LogError(error))
                .IsSuccess;
        }
    }
}
