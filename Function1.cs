using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace JsonCSV
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([BlobTrigger("input-json-files/{name}", Connection = "connection")]string myBlob, string name, [Blob("output-json-files/{name}", FileAccess.ReadWrite, Connection = "connection")] string csvcontent,
                      ILogger log)
        {
            //output binding
           // [Blob("output-json-files/{name}", FileAccess.ReadWrite, Connection = "connection")] string csvcontent,
           log.LogInformation($"C# Blob trigger function Processed blob");

            csvcontent=GetDataTableFromJsonString(myBlob).ToString();
            //jsonStringToCSV(myBlob,name);
            //File.WriteAllLines(@"D:/Export.csv", csvcontent);
        }

        public static DataTable GetDataTableFromJsonString(string json)
        {
            var jsonLinq = JObject.Parse(json);
            // Find the first array using Linq  
            var srcArray = jsonLinq.Descendants().Where(d => d is JArray).First();
            var trgArray = new JArray();
            foreach (JObject row in srcArray.Children<JObject>())
            {
                var cleanRow = new JObject();
                foreach (JProperty column in row.Properties())
                {
                    // Only include JValue types  
                    if (column.Value is JValue)
                    {
                        cleanRow.Add(column.Name, column.Value);
                    }
                }
                trgArray.Add(cleanRow);
            }
            return JsonConvert.DeserializeObject<DataTable>(trgArray.ToString());
        }

        public static void jsonStringToCSV(string jsonContent,string name)
        {
            var dataTable = (DataTable)JsonConvert.DeserializeObject(jsonContent, (typeof(DataTable)));

            //Datatable to CSV
            var lines = new List<string>();
            string[] columnNames = dataTable.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName).
                                              ToArray();
            var header = string.Join(",", columnNames);
            lines.Add(header);
            var valueLines = dataTable.AsEnumerable()
                               .Select(row => string.Join(",", row.ItemArray));
            lines.AddRange(valueLines);
            File.WriteAllLines(@"C:\Customer Files\CsvFiles\"+name+".csv", lines);
        }

    }
}
