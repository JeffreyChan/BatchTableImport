using Microsoft.FSharp.Collections;
using System;
using System.Collections.Concurrent;
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
    class Program
    {
        static BlockingCollection<int> pcCollection = new BlockingCollection<int>(5000);
        static string strRemoteConn = ConfigurationManager.AppSettings["RemoteConnStr"];
        static string strLocalConn = ConfigurationManager.AppSettings["LocalConnStr"];
        static string strCommandSql = ConfigurationManager.AppSettings["CommandSQL"];
        static string strTableName = ConfigurationManager.AppSettings["TableName"];
        static int batchSize = Int32.Parse(ConfigurationManager.AppSettings["CommitBatchSize"]);
        static int taskCount = Int32.Parse(ConfigurationManager.AppSettings["TaskCount"]);
        static int timeOut = Int32.Parse(ConfigurationManager.AppSettings["TimeOut"]);


        static string strColumns = string.Empty;
        static object s_consumer = new object();

        static void Main(string[] args)
        {
            try
            {
                var watch = Stopwatch.StartNew();

                var tableCount = GetTableCount();

                strColumns = GetTableColumns();

                var totalPages = (int)Math.Ceiling(tableCount / batchSize);

                var listPageRn = Enumerable.Range(1, totalPages);

                var listPartPage = listPageRn.Split(taskCount).ToList();

                var listProducerTask = new List<Task>();
                var listConsumerTask = new List<Task>();

                var consumerTask = taskCount;

                for (int i = 1; i <= consumerTask; i++)
                {
                    var taskFlag = i;
                    var consumer = Task.Factory.StartNew(() =>
                    {
                        ConsumerAction(taskFlag.ToString("D2"));

                    }, TaskCreationOptions.LongRunning);

                    listConsumerTask.Add(consumer);
                }

                var producerTaskIndex = 1;
                foreach (var item in listPartPage)
                {
                    var tmpIndex = producerTaskIndex.ToString("D2");
                    var producer = Task.Factory.StartNew(() =>
                    {
                        ProducerAction(item, tmpIndex);
                    });

                    listProducerTask.Add(producer);
                    producerTaskIndex++;
                }

                Task.WaitAll(listProducerTask.ToArray());
                pcCollection.CompleteAdding();
                Task.WaitAll(listConsumerTask.ToArray());

                watch.Stop();
                var mins = watch.ElapsedMilliseconds / 1000 / 60;
                Console.WriteLine("All Batch Insert Time Elapsed:\t {0} mins", mins);
                Console.WriteLine("Total rows are inserted:\t {0}", tableCount);
            }
            catch (AggregateException ex)
            {
                using (StreamWriter writer = File.AppendText("BatchError.txt"))
                {
                    writer.WriteLine("Error Time: {0}", DateTime.Now);
                    foreach (var exception in ex.InnerExceptions)
                    {
                        writer.WriteLine("Error: {0}", exception.Message);
                        writer.WriteLine("Source: {0}", exception.Source);
                        writer.WriteLine("Track: {0}", exception.StackTrace);
                    }
                }
                throw;

            }
            catch (Exception ex)
            {
                using (StreamWriter writer = File.AppendText("BatchError.txt"))
                {
                    writer.WriteLine("Error Time: {0}", DateTime.Now);
                    writer.WriteLine("Error: {0}", ex.Message);
                    writer.WriteLine("Source: {0}", ex.Source);
                    writer.WriteLine("Track: {0}", ex.StackTrace);
                }
                throw;
            }

            Console.ReadLine();
        }


        static double GetTableCount()
        {
            var totalCount = 0D;
            using (var connection = new SqlConnection(strRemoteConn))
            using (SqlCommand cmd = connection.CreateCommand())
            {
                connection.Open();
                cmd.CommandText = string.Format(@"SELECT
                                                        Total_Rows= SUM(st.row_count)
                                                    FROM
                                                        sys.dm_db_partition_stats st
                                                    WHERE
                                                        object_name(object_id) = '{0}' AND (index_id < 2)", strTableName);
                cmd.CommandTimeout = 300;

                if (!Double.TryParse(cmd.ExecuteScalar().ToString(), out totalCount))
                {
                    throw new Exception("please check database!");
                }

                return totalCount;
            }
        }

        static string GetTableColumns()
        {
            var listColumns = new List<string>();
            using (var connection = new SqlConnection(strRemoteConn))
            using (SqlCommand cmd = connection.CreateCommand())
            {
                connection.Open();
                cmd.CommandText = string.Format(@"SELECT Column_Name
                                                FROM Information_Schema.Columns
                                                WHERE Table_Name = '{0}'
                                                ORDER BY Ordinal_Position
                                                ", strTableName);
                cmd.CommandTimeout = 300;

                using (var dataReader = cmd.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        listColumns.Add(dataReader.GetValue(0).ToString());
                    }
                }

            }

            return string.Join(",", listColumns);
        }

        static void ProducerAction(IEnumerable<int> source, string taskFlag = "1")
        {
            foreach (var item in source)
            {
                Console.WriteLine("Producer-{0} processing item batch {1}", taskFlag, item);

                pcCollection.Add(item);
            }
        }

        static void ConsumerAction(string taskFlag = "")
        {


            foreach (var item in pcCollection.GetConsumingEnumerable())
            {
                var processing = new ManageBatchProcessing
                {
                    LocalConnStr = strLocalConn,
                    RemoteConnStr = strRemoteConn,
                    BatchSize = batchSize,
                    TableName = strTableName,
                    CommandSql = strCommandSql,
                    TimeOut = timeOut,
                    ColumnNames = strColumns
                };
                processing.ProcessDatabase(taskFlag, item);
            }
        }

    }
}
