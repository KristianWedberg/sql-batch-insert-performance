using M = Microsoft.Data.SqlClient;
using O = System.Data.Odbc;
using S = System.Data.SqlClient;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Text;

namespace SqlBatchInsertPerformance
{
    public partial class BatchInsert
    {
        public class Argument
        {
            public ProviderAsync ProviderAsync { get; }
            public int NumberOfStatements { get; }
            public int RowsPerStatement { get; }
            public int ColumnsPerRow { get; }

            public string InsertQuery { get; }
            public string TableName { get; }

            public Argument(ProviderAsync providerAsync, int rowsPerStatement, int columnsPerRow, int numberOfStatements)
            {
                ProviderAsync = providerAsync;
                NumberOfStatements = numberOfStatements;
                RowsPerStatement = rowsPerStatement;
                ColumnsPerRow = columnsPerRow;
                TableName = $"insertbenchmark_{rowsPerStatement}_{columnsPerRow}_{providerAsync}";

                SetupTable();

                var insertRows = new StringBuilder($"INSERT INTO {TableName} (")
                    .Append(string.Join(",", Enumerable.Range(0, ColumnsPerRow).Select(c => $"d{c}")))
                    .Append(") VALUES ")
                    .Append(string.Join(",", Enumerable.Range(0, RowsPerStatement).Select(
                        r => "(" + string.Join(",", Enumerable.Range(0, ColumnsPerRow)
                            .Select(c => providerAsync == ProviderAsync.OdbcSync ? "?" : GetParameterName(r, c))) + ")"
                        )));
                InsertQuery = insertRows.ToString();
                //Console.WriteLine("Insert rows: " + Query);
            }

            public IDbConnection GetConnection() =>
                ProviderAsync == ProviderAsync.OdbcSync
                    ? new O.OdbcConnection(_odbcCS)
                    : ProviderAsync == ProviderAsync.SystemSync
                        ? new S.SqlConnection(_sqlClientCS) : new M.SqlConnection(_sqlClientCS);

            public IDbCommand GetCommand(string query, IDbConnection connection) =>
                ProviderAsync == ProviderAsync.OdbcSync
                    ? new O.OdbcCommand(query, (O.OdbcConnection)connection)
                    : ProviderAsync == ProviderAsync.SystemSync
                        ? new S.SqlCommand(query, (S.SqlConnection)connection)
                        : new M.SqlCommand(query, (M.SqlConnection)connection);

            public DbParameter GetParameter(string parameterName) =>
                ProviderAsync == ProviderAsync.OdbcSync
                    ? new O.OdbcParameter(parameterName, O.OdbcType.Int)
                    : ProviderAsync == ProviderAsync.SystemSync
                        ? new S.SqlParameter(parameterName, SqlDbType.Int)
                        : new M.SqlParameter(parameterName, SqlDbType.Int);

            public void SetupTable()
            {
                ExecuteNonQuery("DROP TABLE IF EXISTS " + TableName);

                var createTable = $"CREATE TABLE {TableName} ("
                    + string.Join(", ", Enumerable.Range(0, ColumnsPerRow).Select(c => $"d{c} int"))
                    + ")";
                //Console.WriteLine("Create table: " + createTable);
                ExecuteNonQuery(createTable);
            }

            public void Cleanup()
            {
                ExecuteNonQuery("DROP TABLE IF EXISTS " + TableName);
            }

            public void ExecuteNonQuery(string query)
            {
                using var conn = GetConnection();
                using var cmd = GetCommand(query, conn);
                conn.Open();
                cmd.ExecuteNonQuery();
            }

            public override string ToString() => "";
        }
    }
}
