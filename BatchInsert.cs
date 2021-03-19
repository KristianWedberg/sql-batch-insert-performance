//#define ASYNC // Async makes no material difference (just slightly slower overall)
//#define BOXEDINTVALUES // Define this to avoid boxing an int for every value
#define MINIMALPARAMETERNAME // Reduce length a few characters
//#define PREPARE // Prepare makes no difference
//#define TRANSACTION // Explicit transaction makes no difference

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Running;
using M = Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly int[] _rowsPerStatements = new[] { // Max 1000
            // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000 // All row sizes
            // 1, 2, 4, 8, 16, 32 // Some row sizes
            32 // One row size
        };
        private readonly int[] _columnsPerRows = new[] { // Max 1024
            // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000 // All column sizes
            1, 2, 4, 8, 16, 32, 64 // Some column sizes
            // 64 // One column size
        };
        private const int _scaleNumberOfStatements = 2000;
        private const string _odbcCS =
            "Driver={ODBC Driver 17 for SQL Server};database=actionetl_tester;server=actionetldb;Uid=actionetl_tester;Pwd=uHrQXUhAX7TRCejO";
        private const string _sqlClientCS = "database=actionetl_tester;server=actionetldb;Uid=actionetl_tester;Pwd=uHrQXUhAX7TRCejO";

        private readonly List<object[]> _arguments = new List<object[]>();

#if BOXEDINTVALUES
        private readonly object[] _boxedIntValues = Enumerable.Range(0, 1024).Select(i => (object)i).ToArray();
#endif

        private static string GetParameterName(int r, int c, int totalColumns) =>
#if MINIMALPARAMETERNAME
                $"@p{r * totalColumns + c}"; // Minimal parameter name
#else
                $"@R{r}_C{c}";
#endif

#if PREPARE
        [ParamsAllValues]
        public bool UsePrepare { get; set; }
#endif

#if TRANSACTION
        [ParamsAllValues]
        public bool UseTransaction { get; set; }
#endif

        public BatchInsert()
        {
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseODBCParameterFormat", true);
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.OmitNoMetadata", true);

            //Console.WriteLine("BatchInsert ctor");
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
            {
                foreach (var rowsPerStatement in _rowsPerStatements)
                {
                    foreach (var columnsPerRow in _columnsPerRows)
                    {
                        var parametersInBatch = rowsPerStatement * columnsPerRow;
                        // SqlClient: Max 1000 row values in INSERT, max 2100 parameters, max 1024 columns
                        if (parametersInBatch >= 2100)
                            continue;
                        // Calculate numberOfStatements (i.e. batches) to insert for 200-500ms run time
                        var estimatedParameterThroughput = pa == ProviderAsync.SystemSync && parametersInBatch >= 100
                            ? -92 * parametersInBatch + 220_000
                            : 14_000 * Math.Pow(parametersInBatch, 0.55) - 10_000;
                        int numberOfStatements = (int)Math.Max(1, _scaleNumberOfStatements * estimatedParameterThroughput / parametersInBatch / 1_000);
                        var a = new Argument(pa, rowsPerStatement, columnsPerRow, numberOfStatements);
                        _arguments.Add(new object[] { pa, rowsPerStatement, columnsPerRow, numberOfStatements, a });
                    }
                }
            }
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
        }

        public IEnumerable<object[]> GetArguments()
        {
            return _arguments;
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
            using (var conn = argument.GetConnection())
            {
                using (var cmd = argument.GetCommand(argument.InsertQuery, conn))
                {
                    var parameters = new DbParameter[columnsPerRow * rowsPerStatement];
                    for (int r = 0; r < rowsPerStatement; r++)
                        for (int c = 0; c < columnsPerRow; c++)
                        {
                            string parameterName = GetParameterName(r, c, columnsPerRow);
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
                                var value =
#if BOXEDINTVALUES
                        _boxedIntValues[c];
#else
                        i * columnsPerRow * rowsPerStatement + r * columnsPerRow + c;
#endif
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
                    conn.Close();
                }
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            foreach (var args in _arguments)
                ((Argument)args[^1]).Cleanup();
        }
    }
}
