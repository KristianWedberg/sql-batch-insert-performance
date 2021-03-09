using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Parameters;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBatchInsertPerformance
{
    public class CalculateColumn : IColumn
    {
        readonly Func<Summary, BenchmarkCase, string> _calculateFunc;
        public string Id { get; }
        public string ColumnName { get; }

        public CalculateColumn(string columnName, Func<Summary, BenchmarkCase, string> calculateFunc)
        {
            ColumnName = columnName;
            Id = nameof(CalculateColumn) + "." + ColumnName;
            _calculateFunc = calculateFunc;
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            return _calculateFunc(summary, benchmarkCase);
        }

        public bool IsAvailable(Summary summary) => true;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Size;
        public string Legend => $"Custom '{ColumnName}' column";
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) => GetValue(summary, benchmarkCase);
        public override string ToString() => ColumnName;
    }
}
