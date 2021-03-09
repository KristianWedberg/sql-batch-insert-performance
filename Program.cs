using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Horology;
using System.Linq;

namespace SqlBatchInsertPerformance
{
    class Program
    {
        static void Main()
        {
            BenchmarkRunner.Run<BatchInsert>(
                DefaultConfig.Instance
                    .AddJob(Job.ShortRun)

                    .AddColumn(new CalculateColumn("Throughput [params/s]"
                        , (summary, benchmarkCase) =>
                        {
                            var rps = (int)benchmarkCase.Parameters["rowsPerStatement"];
                            var cpr = (int)benchmarkCase.Parameters["columnsPerRow"];
                            var br = summary.Reports.First(br => br.BenchmarkCase == benchmarkCase);
                            var time = br.ResultStatistics.Mean;
                            var nos = (int)(benchmarkCase.Parameters["numberOfStatements"]);
                            var tp = rps * cpr * nos / time * 1_000_000_000;
                            return tp.ToString("0");
                        }))
                    .AddColumn(new CalculateColumn("Throughput [rows/s]"
                        , (summary, benchmarkCase) =>
                        {
                            var rps = (int)benchmarkCase.Parameters["rowsPerStatement"];
                            var br = summary.Reports.First(br => br.BenchmarkCase == benchmarkCase);
                            var time = br.ResultStatistics.Mean;
                            var nos = (int)(benchmarkCase.Parameters["numberOfStatements"]);
                            var tp = rps * nos / time * 1_000_000_000;
                            return tp.ToString("0");
                        }))

                    .AddExporter(new CsvExporter(
                        CsvSeparator.CurrentCulture,
                        new SummaryStyle(null, true, null, TimeUnit.Millisecond,
                            printUnitsInContent: false)))
                );
        }
    }
}
