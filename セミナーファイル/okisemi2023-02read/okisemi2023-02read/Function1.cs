using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace okisemi2023_02read
{
    public static class Function1
    {
        // Get the secrets from Key Vault
        private static string clientId = GetSecretFromKeyVault("ClientId");
        private static string clientSecret = GetSecretFromKeyVault("ClientSecret");
        private static string tenantId = GetSecretFromKeyVault("TenantId");
        private static string sqlDbConnectionString = Environment.GetEnvironmentVariable("SqlDbConnectionString");

        private static async Task<string> GetAccessToken()
        {
            var app = ConfidentialClientApplicationBuilder.Create(clientId)
                .WithClientSecret(clientSecret)
                .WithAuthority($"https://login.microsoftonline.com/{tenantId}")
                .Build();

            var result = await app.AcquireTokenForClient(new[] { "https://database.windows.net/.default" }).ExecuteAsync();
            return result.AccessToken;
        }

        private static string GetSecretFromKeyVault(string secretName)
        {
            string keyVaultUrl = System.Environment.GetEnvironmentVariable("KeyVaultUrl");
            var client = new SecretClient(new Uri(keyVaultUrl), new DefaultAzureCredential());
            return client.GetSecret(secretName).Value.Value;
        }

        [FunctionName("GetTHData")]
        public static async Task<IActionResult> RunAsync(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            // Get token for Azure SQL DB
            var token = await GetAccessToken();

            // Insert into Azure SQL Database
            string queryString = "SELECT TOP 10 * FROM [dbo].[MyTable] ORDER BY [Timestamp] DESC";

            List<Object> results = new List<Object>();
            using (SqlConnection connection = new SqlConnection(sqlDbConnectionString))
            {
                connection.AccessToken = token; // Set the acquired token
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(queryString, connection))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            DateTime timestamp = (DateTime)reader["Timestamp"];
                            string formattedTimestamp = timestamp.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'z'");

                            // Add your logic to process the data
                            results.Add(new
                            {
                                ID = reader["Id"].ToString(),
                                Temp = reader["Temperature"].ToString(),
                                Humi = reader["Humidity"].ToString(),
                                Time = formattedTimestamp
                            });
                        }
                    }
                }
            }

            return new OkObjectResult(results);
        }
    }
}
