using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataMigrationUtility
{

    class Program
    {
        static int counter ;
        static int number1;
        static int number2;
        static int sum;
        static int fk;
        static int range;
        static int start;
        static int end;
        static CancellationTokenSource cts;
        static CancellationToken ct;
        static bool status;
        static void Main(string[] args)
        {

            string connectionString = "Data Source=DESKTOP-I2EC168\\SQLEXPRESS;Initial Catalog=assignment;Integrated Security=True";
            SqlConnection connection = new SqlConnection(connectionString);
            status= false;
            while (true)
            {
                counter = 0;
                if(!status)
                {
                    range = GetRange(out range, out start, out end);
                    AsyncCallToMigration(connection);
                    Console.WriteLine("Data Migration is in process.................");
                }
                
                TakeInputFromKeyboard();
            }
        }
        public static async void AsyncCallToMigration(SqlConnection connection)
        {
            cts=new CancellationTokenSource();
            ct = cts.Token;
            await Task.Run(() => MigrateData(connection, end, range),ct);
        }
        public static async  void MigrateData(SqlConnection connection,int end,int range)
        {
            Dictionary<int, int> data = RetriveDataFromSourceTable(connection);
            int i = 0;
            while (range > 0 &&  !status)
            {
                connection.Open();
                int count = 1;
                while (count <= 100 && range > 0 && !status)
                {
                    await GetSumOfNumbers(connection,range, end, data,i);
                    count++;
                    range--;
                    i++;
                }
                connection.Close();
                Console.WriteLine($"Migration of {count-1} data is done.");
            }
            Console.WriteLine("-------DONE-------");
            Console.WriteLine("Enter 'YES' for continue and 'NO' for exit.\n");
            
        }

        private static async Task  GetSumOfNumbers(SqlConnection connection, int range, int end, Dictionary<int, int> data,int i)
        {
            KeyValuePair<int, int> pair = data.ElementAt(i);
            fk = pair.Key;
            sum=pair.Value; 
            Thread.Sleep(50);
            string addNumbersQuery = "insert into DestinationTable values(@fk,@sum)";
            SqlCommand addNumbersCommand = new SqlCommand(addNumbersQuery, connection);
            addNumbersCommand.Parameters.AddWithValue("@fk", fk);
            addNumbersCommand.Parameters.AddWithValue("@sum", sum);
            addNumbersCommand.ExecuteNonQuery();
            counter++;
        }


        private static Dictionary<int, int> RetriveDataFromSourceTable(SqlConnection connection)
        {
            Dictionary<int, int> data = new Dictionary<int, int>();
            int offset = start - 1;
            int limit = range;

            connection.Open();
            string retrievalQuery = "SELECT * FROM SourceTable ORDER BY ID OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY";
            SqlCommand retrievalCommand = new SqlCommand(retrievalQuery, connection);
            retrievalCommand.Parameters.AddWithValue("@offset", offset);
            retrievalCommand.Parameters.AddWithValue("@limit", limit);
            SqlDataAdapter dataAdapter = new SqlDataAdapter(retrievalCommand);

            DataSet dataSet = new DataSet();
            dataAdapter.Fill(dataSet, "T1");
            int dataSize = dataSet.Tables["T1"].Rows.Count;

            for (int i = 0; i < dataSize; i++)
            {
                fk = Convert.ToInt32(dataSet.Tables["T1"].Rows[i]["ID"]);
                number1 = Convert.ToInt32(dataSet.Tables["T1"].Rows[i]["FirstNumber"]);
                number2 = Convert.ToInt32(dataSet.Tables["T1"].Rows[i]["SecondNumber"]);
                sum = number1 + number2;
                data.Add(fk, sum);
            }
            connection.Close();
            return data;
        }

        private static int GetRange(out int range, out int start, out int end)
        {
            bool result;
            do
            {
                Console.WriteLine("Enter range:");
                Console.Write("Enter start number:");
                string a = Console.ReadLine();
                Console.Write("Enter end number:");
                string b= Console.ReadLine();
                bool x = int.TryParse(a, out start);
                bool y = int.TryParse(b, out end);
                result = (start > end || start < 1 || end > 1000000 || !x || !y);
                if (result)
                {
                    Console.WriteLine("Invalid input");
                }
            } while (result);
            
            range = end - start + 1;
            return range;
        }
        private static void TakeInputFromKeyboard()
        {
            string input = Console.ReadLine();
            while (true)
            {
                if (input.ToUpper().Equals("YES"))
                {
                    status = false;
                    break;
                }
                else if (input.ToUpper().Equals("NO"))
                {
                    Environment.Exit(0);
                }
                else if(input.ToUpper().Equals("CANCEL"))
                {
                    Console.WriteLine("-------CANCEL-------");
                    cts.Cancel();
                    status = true;
                    break;
                }
                else if (input.ToUpper().Equals("STATUS"))
                {
                    Console.WriteLine("-------STATUS-------");
                    Console.WriteLine($"Successfully migrated data:{counter+1}");
                    Console.WriteLine($"Remaining  data:{end - start - counter }\n");
                    input = Console.ReadLine();
                }
                else
                {
                    input = Console.ReadLine();
                }
            }

        }

    }
}
