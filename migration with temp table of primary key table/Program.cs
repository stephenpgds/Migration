using System;
using Npgsql;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.ComponentModel;
using NpgsqlTypes;
using System.Linq;
using System.Text;
using System.IO;
using System.Globalization;

namespace Migrate
{
    class Program
    {
        static void Main(string[] args)
        {
            // SQL Server Connection String
            // string sqlConnectionString = "Data Source=<SQLServerName>;Initial Catalog=<DatabaseName>;Integrated Security=True";

            // PostgreSQL Connection String
            //string pgConnectionString = "Server=<PostgreSQLServerName>;Port=<PostgreSQLPortNumber>;User Id=<PostgreSQLUsername>;Password=<PostgreSQLPassword>;Database=<PostgreSQLDatabaseName>";

            string sqlConnectionString = "Data Source=profile-builder-demo-database.cqudqfsnwzu5.eu-west-2.rds.amazonaws.com;Initial Catalog=PEPProfilerDB;User ID=dab_readonly;Password=dab1234#;Encrypt=false;Timeout=5600;";
            string pgConnectionString = "Server=profilebuilder-v1-stage-db.cvwpjekkgsqh.ap-southeast-1.rds.amazonaws.com;Port=5432;Database=profilebuilder;User Id=pb_prod_admin;Password=siy8kar5HoxOJYI9z3ov;CommandTimeout=5600;";



            //// Connect to SQL Server
            //var sqlConnection = new SqlConnection(sqlConnectionString);
            //sqlConnection.Open();

            //// Retrieve the schema of the table
            //var sqlQuery = "SELECT * FROM information_schema.columns WHERE table_name = 'pepallocation'";
            //var sqlCommand = new SqlCommand(sqlQuery, sqlConnection);
            //var reader = sqlCommand.ExecuteReader();

            //// Build the CREATE TABLE query for PostgreSQL
            //var columns = new List<string>();
            //while (reader.Read())
            //{
            //    var columnName = reader.GetString(reader.GetOrdinal("column_name"));
            //    var dataType = reader.GetString(reader.GetOrdinal("data_type"));
            //    columns.Add($"{columnName} {MapDataType(dataType)}");
            //}
            //// var postgresQuery = $"CREATE IF NOT EXISTS TABLE application_user_temp ({string.Join(", ", columns)})";
            //var postgresQuery = $"CREATE  TABLE IF NOT EXISTS pepallocation_temp ({string.Join(", ", columns)})";

            //// Connect to PostgreSQL
            //var postgresConnection = new NpgsqlConnection(pgConnectionString);
            //postgresConnection.Open();

            //// Execute the CREATE TABLE query
            //var postgresCommand = new NpgsqlCommand(postgresQuery, postgresConnection);
            //postgresCommand.ExecuteNonQuery();




            //string MapDataType(string sqlDataType)
            //{
            //    switch (sqlDataType)
            //    {
            //        case "bigint":
            //            return "bigint";
            //        case "int":
            //        case "integer":
            //            return "integer";
            //        case "smallint":
            //            return "smallint";
            //        case "tinyint":
            //            return "smallint";
            //        case "bit":
            //            return "boolean";
            //        case "decimal":
            //            return "decimal";
            //        case "numeric":
            //            return "numeric";
            //        case "money":
            //        case "smallmoney":
            //            return "numeric";
            //        case "real":
            //            return "double precision";
            //        case "datetime":
            //        case "datetime2":
            //            return "timestamp without time zone";
            //        case "smalldatetime":
            //        case "date":
            //            return "date";
            //        case "time":
            //            return "time";
            //        case "char":
            //            return "char";
            //        case "varchar":
            //            return "text";
            //        case "text":
            //        case "ntext":
            //            return "text";
            //        case "nchar":
            //            return "nchar";
            //        case "nvarchar":
            //            return "text";
            //        case "binary":
            //        case "varbinary":
            //        case "image":
            //            return "bytea";
            //        case "uniqueidentifier":
            //            return "uuid";
            //        case "float":
            //            return "double precision";
            //        default:
            //            throw new NotSupportedException($"Unsupported data type: {sqlDataType}");
            //    }
            //}



            //// Define the SQL Server query to retrieve data from the source table
            //string sql = "SELECT top(5)* FROM dbo.pepallocation";

            //// Define the name of the temporary table you want to create in PostgreSQL
            //string tempTableName = "pepallocation_temp";

            ////start of new code

            //// Create a SQL Server connection and a PostgreSQL connection
            //using (SqlConnection sqlServerConnection = new SqlConnection(sqlConnectionString))
            //using (NpgsqlConnection postgreSQLConnection = new NpgsqlConnection(pgConnectionString))
            //{
            //    // Open the SQL Server connection
            //    sqlServerConnection.Open();

            //    // Create a SQL Server command to select data from the table
            //    SqlCommand sqlServerCommand = new SqlCommand(sql, sqlServerConnection);

            //    // Create a SQL Server reader to read the data
            //    SqlDataReader sqlServerReader = sqlServerCommand.ExecuteReader();

            //    // Open the PostgreSQL connection
            //    postgreSQLConnection.Open();

            //    // Create a PostgreSQL writer to insert the data
            //    NpgsqlBinaryImporter postgreSQLWriter = postgreSQLConnection.BeginBinaryImport($"COPY {tempTableName} FROM STDIN BINARY");

            //    // Loop through the rows in the SQL Server reader
            //    while (sqlServerReader.Read())
            //    {
            //        // Get the column values from the SQL Server reader
            //        object[] columnValues = new object[sqlServerReader.FieldCount];
            //        sqlServerReader.GetValues(columnValues);

            //        // Write the column values to the PostgreSQL writer
            //        postgreSQLWriter.WriteRow(columnValues);
            //    }

            //    // End the PostgreSQL writer
            //    postgreSQLWriter.Complete();

            //    // Close the SQL Server reader and connection
            //    sqlServerReader.Close();
            //    sqlServerConnection.Close();
            //}

            // end of new code

            // start of temp to or

            string sourceTableName = "pepallocation_temp";
            string destinationTableName = "pepallocation";


            // Define the excluded column name
            string excludedColumnName = "id";


            // Create a source connection and a destination connection
            using (NpgsqlConnection sourceConnection = new NpgsqlConnection(pgConnectionString))
            using (NpgsqlConnection destinationConnection = new NpgsqlConnection(pgConnectionString))
            {
                // Open the source connection
                sourceConnection.Open();

                // Create a source command to select data from the table, excluding the specified column
                string[] columnNames = GetColumnNames(sourceConnection, sourceTableName).Except(new[] { excludedColumnName }).ToArray();
                string selectCommandText = $"SELECT {string.Join(",", columnNames)} FROM {sourceTableName}";
                using (NpgsqlCommand sourceCommand = new NpgsqlCommand(selectCommandText, sourceConnection))
                {
                    // Create a source reader to read the data
                    using (NpgsqlDataReader sourceReader = sourceCommand.ExecuteReader())
                    {
                        // Open the destination connection
                        destinationConnection.Open();

                        // Create a destination writer to insert the data
                        using (NpgsqlBinaryImporter destinationWriter = destinationConnection.BeginBinaryImport($"COPY {destinationTableName} ({string.Join(",", columnNames)}) FROM STDIN BINARY"))
                        {
                            // Loop through the rows in the source reader
                            while (sourceReader.Read())
                            {
                                // Get the column values from the source reader
                                object[] columnValues = new object[sourceReader.FieldCount];
                                sourceReader.GetValues(columnValues);

                                // Write the column values to the destination writer
                                destinationWriter.WriteRow(columnValues);
                            }
                        }
                    }
                }
            }

            // Get the column names of the specified table in the specified database
            IEnumerable<string> GetColumnNames(NpgsqlConnection connection, string tableName)
            {
                using (NpgsqlCommand command = new NpgsqlCommand($"SELECT column_name FROM information_schema.columns WHERE table_name='{tableName}'", connection))
                using (NpgsqlDataReader reader2 = command.ExecuteReader())
                {
                    while (reader2.Read())
                    {
                        yield return reader2.GetString(0);
                    }
                }
            }
            // end of copy

            // Drop the temporary table

            Console.WriteLine("Data migration completed successfully");
        }
    }
}