using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
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
        public int TimeOut { get; set; }
        public string TableName { get; set; }

        public string ColumnNames { get; set; }


        private object _lockObj = new object();

        public void ProcessDatabase(string taskFlag, int item)
        {
            var watch = new Stopwatch();
            watch.Start();

            Console.WriteLine("consumer-{0} processing batch-{1}", taskFlag, item);

            var start = (item - 1) * this.BatchSize + 1;
            var end = item * this.BatchSize;
            var strCommandSql = string.Format(this.CommandSql, start, end, this.ColumnNames);
            using (var remoteConn = new SqlConnection(this.RemoteConnStr))
            {
                remoteConn.Open();

                using (var command = new SqlCommand(strCommandSql, remoteConn))
                {
                    command.CommandTimeout = this.TimeOut;

                    using (var dataReader = command.ExecuteReader())
                    {
                        using (var bulkCopy = new SqlBulkCopy(this.LocalConnStr))
                        {
                            bulkCopy.DestinationTableName = this.TableName;
                            bulkCopy.BulkCopyTimeout = this.TimeOut;
                            bulkCopy.WriteToServer(dataReader);
                            bulkCopy.Close();
                        }
                    }
                }

                remoteConn.Close();
            }

            watch.Stop();

            var totalSeconds = (double)watch.ElapsedMilliseconds / 1000;
            Console.WriteLine("\t\t\t -------------------------------------------------");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("\t\t\t insert target table done {0} s at {1}", totalSeconds.ToString("#.##"), DateTime.Now.ToString("HH:mm:ss"));
            Console.ResetColor();
            Console.WriteLine("\t\t\t -------------------------------------------------");

        }

        public void ProcessDatabaseV2(string taskFlag, int item)
        {
            var watch = new Stopwatch();
            watch.Start();

            Console.WriteLine("consumer-{0} processing item", taskFlag);

            var start = (item - 1) * this.BatchSize + 1;
            var end = item * this.BatchSize;
            var strCommandSql = string.Format(this.CommandSql, start, end);

            var fileName = string.Format("{0}-{1}.txt", this.TableName, item);
            using (var remoteConn = new SqlConnection(this.RemoteConnStr))
            {
                remoteConn.Open();

                using (var command = new SqlCommand(strCommandSql, remoteConn))
                {
                    command.CommandTimeout = this.TimeOut;

                    using (var dataReader = command.ExecuteReader())
                    {

                        var rowRanges = Enumerable.Range(0, dataReader.FieldCount);
                        var columnNames = rowRanges.Select(dataReader.GetName).ToList();

                        var header = string.Join("\t", rowRanges.Select(dataReader.GetName).ToList());
                        using (var writer = File.AppendText(fileName))
                        {
                            writer.WriteLine(header);
                            while (dataReader.Read())
                            {
                                var rowContent = string.Join("\t", rowRanges.Select(dataReader.GetValue).ToList());
                                writer.WriteLine(rowContent);
                            }
                        }
                    }
                }

                remoteConn.Close();
            }

            watch.Stop();

            var totalSeconds = (double)watch.ElapsedMilliseconds / 1000;
            Console.WriteLine("\t\t\t -------------------------------------------------");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("\t\t\t insert target table done {0} s", totalSeconds.ToString("#.##"));
            Console.ResetColor();
            Console.WriteLine("\t\t\t -------------------------------------------------");
        }

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

        public void WriteToDatabase(IDataReader reader)
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
                    bulkCopy.WriteToServer(reader);
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
