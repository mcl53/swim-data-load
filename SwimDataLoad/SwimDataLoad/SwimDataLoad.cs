using UglyToad.PdfPig.Content;
using DuckDB.NET.Data;

namespace SwimDataLoad
{
    class SwimDataLoad
    {
        static void Main(string[] args)
        {

            string sourceDataDirectory = Environment.GetEnvironmentVariable("SOURCE_DATA_DIRECTORY");
            if (string.IsNullOrEmpty(sourceDataDirectory))
            {
                throw new Exception("The environment variable 'SOURCE_DATA_DIRECTORY' is not set.");
            }

            string DatabaseDir = Environment.GetEnvironmentVariable("DUCKDB_DIRECTORY");
            if (string.IsNullOrEmpty(DatabaseDir))
            {
                throw new Exception("The environment variable 'DUCKDB_DIRECTORY' is not set.");
            }

            StreamReader PdfPageWordCreateTable = new StreamReader(".\\TableDefinitions\\pdf_page_word.sql");

            string[] PdfFiles = Directory.GetFiles(sourceDataDirectory);

            using (DuckDBConnection duckDbConn = new DuckDBConnection($"Data Source={DatabaseDir}\\swim_data.duckdb"))
            {
                duckDbConn.Open();

                DuckDbQuery.ExecDuckDbNonQuery(duckDbConn, "DROP SCHEMA IF EXISTS isl_raw CASCADE; CREATE SCHEMA isl_raw;");

                DuckDbQuery.ExecDuckDbNonQuery(duckDbConn, PdfPageWordCreateTable.ReadToEnd());

                DuckDbQuery.ExecDuckDbNonQuery(duckDbConn, "TRUNCATE TABLE isl_raw.pdf_page_word;");

                DateTime loadDatetime = DateTime.Now;

                using (DuckDBAppender appender = duckDbConn.CreateAppender("isl_raw", "pdf_page_word"))
                {
                    foreach (string PdfFilePath in PdfFiles)
                    {
                        List<Dictionary<string, string>> pdfData = ConvertPdfFileToDict(new PdfFile(PdfFilePath, "Results"));
                        foreach (Dictionary<string, string> row in pdfData)
                        {
                            DuckDBAppenderRow appenderRow = appender.CreateRow();

                            foreach (KeyValuePair<string, string> value in row)
                            {
                                appenderRow.AppendValue(value.Value);
                            }

                            appenderRow.AppendValue(loadDatetime);

                            appenderRow.EndRow();
                        }
                    }
                }

                DuckDbQuery.ExecDuckDbReaderQuery(duckDbConn, "SELECT COUNT(*) AS numRows FROM isl_raw.pdf_page_word");
            }
        }

        public static List<Dictionary<string, string>> ConvertPdfFileToDict(PdfFile pdfFile)
        {
            List<Dictionary<string, string>> InsertValuesDict = new List<Dictionary<string, string>>();

            foreach (PDFPage page in pdfFile.Pages)
            {
                foreach (Word word in page.Words)
                {
                    Dictionary<string, string> letterDict = new Dictionary<string, string>
                    {
                        {"file_name",   pdfFile.FileName},
                        {"file_type",   pdfFile.FileType},
                        {"page_number", page.PageNum.ToString()},
                        {"letter",      word.Text},
                        {"location_x",  word.BoundingBox.BottomLeft.X.ToString()},
                        {"location_y",  word.BoundingBox.BottomLeft.Y.ToString()},
                        {"width",       word.BoundingBox.Width.ToString()},
                        {"height",      word.BoundingBox.Height.ToString()}
                    };
                    InsertValuesDict.Add(letterDict);
                }
            }

            return InsertValuesDict;
        }
    }
}