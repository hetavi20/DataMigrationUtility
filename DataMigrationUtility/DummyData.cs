using System.Data.SqlClient;
using System;

namespace DataMigrationUtility
{
    public class DummyData
    {
        public static void AddDummyData(SqlConnection connection)
        {
            int first;
            int second;
            Random random = new Random();
            string queryForSourceTableInsertion = "insert into SourceTable values (@first,@second)";
            SqlCommand cmd;
            connection.Open();
            for (Int64 i = 0; i < 1000000; i++)
            {
                cmd = new SqlCommand(queryForSourceTableInsertion, connection);
                first = random.Next(200);
                second = random.Next(200);
                cmd.Parameters.AddWithValue("@first", first);
                cmd.Parameters.AddWithValue("@second", second);
                cmd.ExecuteNonQuery();
            }
            connection.Close();
        }
    }
}
