using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BatchTableImport
{
    public class ManageBatchProcessing
    {
        public string LocalConnStr { get; set; }
        public string RemoteConnStr { get; set; }

        public string CommandSql { get; set; }
        public int BatchSize { get; set; }
        public string TableName { get; set; }

        public DataTable RetriveToDatabase(int item)
        {
            var start = (item - 1) * this.BatchSize + 1;
            var end = item * this.BatchSize;
            var dataTable = new DataTable();

            using (var connection = new SqlConnection(this.RemoteConnStr))
            {
                var watch = new Stopwatch();
                watch.Start();

                using (var adapter = new SqlDataAdapter(string.Format(this.CommandSql, start, end), connection))
                {
                    adapter.SelectCommand.CommandTimeout = 3600;
                    adapter.Fill(dataTable);
                }

                watch.Stop();

                var totalSeconds = (double)watch.ElapsedMilliseconds / 1000;
                Console.WriteLine("\t\t\t -------------------------------------------------");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\t\t\t convert datareader to table done {0} s", totalSeconds.ToString("#.##"));
                Console.ResetColor();
                Console.WriteLine("\t\t\t -------------------------------------------------");
                return dataTable;
            }
        }

        public void WriteToDatabase(DataTable dataTable)
        {
            using (var connection = new SqlConnection(this.LocalConnStr))
            {
                var watch = new Stopwatch();
                watch.Start();

                connection.Open();

                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, null))
                {
                    bulkCopy.DestinationTableName = this.TableName;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.WriteToServer(dataTable);
                }

                connection.Close();

                watch.Stop();

                var totalSeconds = (double)watch.ElapsedMilliseconds / 1000;
                Console.WriteLine("\t\t\t -------------------------------------------------");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("\t\t\t insert target table done {0} s", totalSeconds.ToString("#.##"));
                Console.ResetColor();
                Console.WriteLine("\t\t\t -------------------------------------------------");
            }
        }

    }
}
