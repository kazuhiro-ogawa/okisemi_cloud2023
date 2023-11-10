using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Azure.Messaging.EventHubs;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Microsoft.Identity.Client;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using System;

namespace okisemi2023_02insert
{
    public class Function1
    {
        private static string SqlDbConnectionString = System.Environment.GetEnvironmentVariable("SqlDbConnectionString");
        private static string clientId = GetSecretFromKeyVault("ClientId");
        private static string clientSecret = GetSecretFromKeyVault("ClientSecret");
        private static string tenantId = GetSecretFromKeyVault("TenantId");

        [FunctionName("Function1")]
        public async Task Run([IoTHubTrigger("messages/events", Connection = "ConnectionString")] EventData message, ILogger log)
        {
            try
            {
                log.LogInformation($"C# IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.ToArray())}");

                // Parse the IoT Hub message as JSON
                var messageBody = Encoding.UTF8.GetString(message.Body.ToArray());
                JObject jsonMessage = JObject.Parse(messageBody);

                // Extract temperature and humidity from the message
                double temperature = jsonMessage.Value<double>("temperature");
                double humidity = jsonMessage.Value<double>("humidity");

                // Get token for Azure SQL DB
                var token = await GetAccessToken();

                // Insert into Azure SQL Database
                string queryString = $"INSERT INTO MyTable (Temperature, Humidity, Timestamp) VALUES ({temperature}, {humidity}, DATEADD(HOUR, 9, GETUTCDATE()))";

                using (SqlConnection connection = new SqlConnection(SqlDbConnectionString))
                {
                    connection.AccessToken = token; // Set the acquired token
                    SqlCommand command = new SqlCommand(queryString, connection);
                    await connection.OpenAsync();
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (System.Exception ex)
            {
                log.LogError($"Error processing message: {ex.Message}");
                log.LogError(ex.StackTrace);
            }
        }

        private async Task<string> GetAccessToken()
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
    }
}