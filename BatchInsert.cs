//#define ASYNC // Async makes no material difference (just slightly slower overall)
//#define PREPARE // Prepare makes no difference
//#define TRANSACTION // Explicit transaction makes no difference

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Running;
using M = Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Odbc;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Columns;
using System.Data;

namespace SqlBatchInsertPerformance
{
    public enum ProviderAsync
    {
#if ASYNC
        MicrosoftAsync = 0,
#endif
        MicrosoftSync = 1, SystemSync = 2 // Keep SqlClient first
        , OdbcSync
    }

    public partial class BatchInsert
    {
        // Edit these for your environment
        private const int _scaleNumberOfStatements = 200;
        private const string _odbcCS = "Driver={ODBC Driver 17 for SQL Server};database=MyDatabase;server=MyServer;Uid=MyUser;Pwd=MyPassword";
        private const string _sqlClientCS = "database=MyDatabase;server=MyServer;Uid=MyUser;Pwd=MyPassword";

        private readonly List<Argument> _arguments = new();

        private static string GetParameterName(int r, int c) => $"@R{r}_C{c}";

#if PREPARE
        [ParamsAllValues]
        public bool UsePrepare { get; set; }
#endif

#if TRANSACTION
        [ParamsAllValues]
        public bool UseTransaction { get; set; }
#endif

        public IEnumerable<object[]> GetArguments()
        {
            foreach (var pa in new[]
            {
                ProviderAsync.OdbcSync
                ,
                ProviderAsync.MicrosoftSync
                ,
                ProviderAsync.SystemSync
#if ASYNC
                , ProviderAsync.MicrosoftAsync
#endif
            })
                foreach (var rowsPerStatement in new[] { // Max 1000
                        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000
                    })
                    foreach (var columnsPerRow in new[] { // Max 1024
                        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
                    })
                    {
                        var parametersInBatch = rowsPerStatement * columnsPerRow;
                        // SqlClient: Max 1000 row values in INSERT, max 2100 parameters, max 1024 columns
                        if (parametersInBatch >= 2100)
                            continue;
                        // Calculate numberOfStatements (i.e. batches) to insert for 200-500ms run time
                        var estimatedParameterThroughput = pa <= ProviderAsync.SystemSync && parametersInBatch >= 100
                            ? -92 * parametersInBatch + 220_000
                            : 14_000 * Math.Pow(parametersInBatch, 0.55) - 10_000;
                        int numberOfStatements = (int)Math.Max(1, _scaleNumberOfStatements * estimatedParameterThroughput / parametersInBatch / 1_000);
                        var a = new Argument(pa, rowsPerStatement, columnsPerRow, numberOfStatements);
                        _arguments.Add(a);
                        yield return new object[] { pa, rowsPerStatement, columnsPerRow, numberOfStatements, a };
                    }
        }

        [ArgumentsSource(nameof(GetArguments))]
        [Benchmark]
        public
#if ASYNC
            async Task 
#else
            void
#endif
            InsertAsync(ProviderAsync providerAsync, int rowsPerStatement, int columnsPerRow, int numberOfStatements, Argument argument)
        {
            using var conn = argument.GetConnection();
            using var cmd = argument.GetCommand(argument.InsertQuery, conn);
            var parameters = new DbParameter[columnsPerRow * rowsPerStatement];
            for (int r = 0; r < rowsPerStatement; r++)
                for (int c = 0; c < columnsPerRow; c++)
                {
                    string parameterName = GetParameterName(r, c);
                    DbParameter parameter = argument.GetParameter(parameterName);
                    parameters[r * columnsPerRow + c] = parameter;
                    cmd.Parameters.Add(parameter);
                }
            conn.Open();
#if PREPARE
            if (UsePrepare)
                cmd.Prepare();
#endif

            for (int i = 0; i < numberOfStatements; i++)
            {
                for (int r = 0; r < rowsPerStatement; r++)
                    for (int c = 0; c < columnsPerRow; c++)
                    {
                        int value = i * columnsPerRow * rowsPerStatement + r * rowsPerStatement + c;
                        parameters[r * columnsPerRow + c].Value = value;
                    }

#if TRANSACTION
                IDbTransaction transaction = null;
                if (UseTransaction)
                {
                    transaction = conn.BeginTransaction();
                    cmd.Transaction = transaction;
                }
                try
                {
#endif
#if ASYNC
                if (providerAsync == ProviderAsync.MicrosoftAsync)
                    await ((M.SqlCommand)cmd).ExecuteNonQueryAsync().ConfigureAwait(false);
                else
#endif
                    cmd.ExecuteNonQuery();
#if TRANSACTION
                    if (UseTransaction)
                        transaction.Commit();
                }
                catch
                {
                    if (UseTransaction)
                        transaction.Rollback();
                    throw;
                }
#endif
            }
        }
    }
}
