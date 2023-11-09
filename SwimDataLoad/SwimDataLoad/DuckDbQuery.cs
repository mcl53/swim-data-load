using DuckDB.NET.Data;

namespace SwimDataLoad
{
    internal class DuckDbQuery
    {
        public static void ExecDuckDbNonQuery(DuckDBConnection conn, string Query)
        {
            DuckDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = Query;
            int returnVal = cmd.ExecuteNonQuery();
            Console.WriteLine($"Number of rows affected by query: {returnVal}");
        }

        public static void ExecDuckDbReaderQuery(DuckDBConnection conn, string Query)
        {
            DuckDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = Query;
            DuckDBDataReader reader = cmd.ExecuteReader();

            System.Data.DataTable schema = reader.GetSchemaTable();

            while (reader.Read())
            {
                foreach (System.Data.DataRow column in schema.Rows)
                {
                    int columnPosition = (int)column[0];
                    string columnDataType = column[2].ToString();
                    string columnValue = GetQueryValue(reader, columnPosition, columnDataType);
                    Console.WriteLine($"{column[1]}: {columnValue}");
                }
                Console.WriteLine();
            }
        }

        public static string GetQueryValue(DuckDBDataReader reader, int columnPosition, string dataType)
        {
            switch (dataType)
            {
                case "System.String":
                    return reader.GetString(columnPosition);
                case "System.Int32":
                    return reader.GetInt32(columnPosition).ToString();
                case "System.Decimal":
                    return reader.GetDecimal(columnPosition).ToString();
                case "System.DateTime":
                    return reader.GetDateTime(columnPosition).ToString();
                case "System.Int64":
                    return reader.GetInt64(columnPosition).ToString();
                default:
                    Exception error = new Exception($"Data type not recognised: {dataType}");
                    throw error;
            }
        }
    }
}
