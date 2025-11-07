using Google.Apis.Json;
using GraphQL;
using GraphQL.Client.Http;
using ImporterIntegrationFrameworkApp.Classes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Configuration;
using RestSharp;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using System.Threading;
using System.Threading.Tasks;
using GraphQL.Client.Serializer.Newtonsoft;

namespace ImporterIntegrationFrameworkApp
{
    public class JobsManager
    {
        public enum AuthenticationType
        {
            //APIKey = 0,
            //UserNamePassword = 1,
            //OAUTH = 2,
            //BasicAuth = 3,
            APIKey = 1,
            UserNamePassword,
            OAUTH,
            BasicAuth
        }
        public enum RESTPullAPIParameterType
        {
            HeaderParameter,
            URLParameter
        }
        public enum EnumRequestParameterType
        {
            URLParameter = 0,
            HeaderParameter = 1,
            BodyParameter = 2,
            GraphQL = 3
        }

        //protected static int intIntegrationFrameworkHeaderID { get; set; }
        //public static bool IsAnyJobRunning(IntegrationType iType)
        //{
        //    // query the database
        //    var sql = $"Select count(*) FROM Integration_Status WHERE IntegrationType = '{IntegrationTypeHelper.IntegrationTypeDataValueMapping[iType]}'";

        //    var count = 0; // get the value from the datastore

        //    // process
        //    return (count > 0);
        //}
        public static bool IsAnyJobRunning(IntegrationType iType, int maxParallelJobs, string connectionString)
        {

            //#region OnPremise
            //string strReadOnlyConnectionString = string.Empty;
            //string strReadWriteConnectionString = string.Empty;
            string strCustomerID = string.Empty;
            SqlConnection objSqlConnection = null;
            SqlCommand objSqlCommand = null;
            SqlDataAdapter objSqlDataAdapter = null;
            try
            {
                //strReadOnlyConnectionString = System.Configuration.ConfigurationManager.AppSettings["ReadOnlyConnnection"].ToString();
                //strReadWriteConnectionString = System.Configuration.ConfigurationManager.AppSettings["ReadWriteConnnection"].ToString();
                objSqlConnection = new SqlConnection(connectionString);
                objSqlCommand = new SqlCommand();
                objSqlConnection.Open();
                objSqlCommand.Connection = objSqlConnection;
                objSqlCommand.CommandTimeout = 0;
                var strSQL = $" select ID from IntegrationTaskStatus where ISNULL(IntegrationTaskStatus.Integration_Last_Run_Status,0) = 0 and IntegrationTaskStatus.IntegrationType = '{IntegrationTypeHelper.IntegrationTypeDataValueMapping[iType]}' ";

                objSqlCommand.CommandType = CommandType.Text;
                objSqlCommand.CommandText = strSQL;
                DataSet da = new DataSet();
                objSqlDataAdapter = new SqlDataAdapter(objSqlCommand);
                objSqlDataAdapter.Fill(da);
                return (da.Tables[0].Rows.Count > maxParallelJobs);
            }
            catch (Exception exp)
            {
                StringBuilder strerrorMsg = new StringBuilder();
                strerrorMsg.AppendLine(exp.Message);
                strerrorMsg.AppendLine(exp.StackTrace);
                if (exp.InnerException != null)
                {
                    strerrorMsg.AppendLine(exp.InnerException.Message);
                    strerrorMsg.AppendLine(exp.InnerException.StackTrace);
                }
                //IntegrationTask.InsertIntegrationLog(connectionString, 0, strerrorMsg.ToString(), string.Empty);
                return true;
            }
            finally
            {
                if (objSqlDataAdapter != null)
                {
                    objSqlDataAdapter.Dispose();
                    objSqlDataAdapter = null;
                }
                if (objSqlCommand != null)
                {
                    objSqlCommand.Dispose();
                    objSqlCommand = null;
                }
                if (objSqlConnection != null)
                {
                    objSqlConnection.Close();
                    objSqlConnection.Dispose();
                    objSqlConnection = null;
                }
            }

        }
        public static List<CommonIntegrationData> GetIntegrationsToExecute(IntegrationType iType, DateTime asOfTime, string[] args, string connectionString)
        {
            // query the database

            #region OnPremise

            List<CommonIntegrationData> lstCommonIntegrationData = new List<CommonIntegrationData>();
            string strReadWriteConnectionString = connectionString; // or use separate if needed
            string strCustomerID = "4";
            try
            {

                switch (iType)
                {
                    case IntegrationType.RestGet:
                        DataTable dtRestPullHeaderData = IntegrationTask.GetRESTPullIntegrationFrameworkHeaderData(connectionString, 0, true);
                        if (dtRestPullHeaderData != null && dtRestPullHeaderData.Rows.Count > 0)
                        {
                            Console.WriteLine("Getting REST Pull Integration Header Data......");
                            //for (int i = 0; i < dtRestPullHeaderData.Rows.Count; i++)
                            //{

                            //List<Appointment> objAppointments = IntegrationTask.GetFileIntegration(dtRestPullHeaderData1);
                            //Appointment objAppointment = IntegrationTask.GetSingleIntegrationAppointment(dtRestPullHeaderData.Rows[i]);
                            //var integrationAppointment = IntegrationTask.GetSingleIntegrationAppointment(dtRestPullHeaderData.Rows[i]);
                            //AppointmentCore objAppointment = new AppointmentCore
                            //{
                            //    StartDate = Convert.ToDateTime(dtRestPullHeaderData.Rows[i]["StartDate"]),
                            //    EndDate = Convert.ToDateTime(dtRestPullHeaderData.Rows[i]["EndDate"]), // or use EndDate column if exists
                            //    RecurrenceRule = Convert.ToString(dtRestPullHeaderData.Rows[i]["RecurrenceRuleForProcess"]),
                            //    IsMaster = !string.IsNullOrEmpty(Convert.ToString(dtRestPullHeaderData.Rows[i]["RecurrenceRuleForProcess"]))
                            //};
                            ////foreach (Appointment objAppointment in objAppointments)
                            //if (asOfTime.TimeOfDay >= objAppointment.StartDate.TimeOfDay)
                            //{
                            CommonIntegrationData objCommonIntegrationData = new CommonIntegrationData
                            {
                                strConnection = connectionString,
                                strCustomerID = strCustomerID,
                                HeaderID = 3
                                // IntegrationFrameworkHeaderID = Convert.ToInt32(dtHeaderData.Rows[i]["ID"])
                            };
                            lstCommonIntegrationData.Add(objCommonIntegrationData);
                            //}

                            //}
                            Console.WriteLine("REST Pull Integration has been Completed Successfully.");
                        }
                        else
                        {
                            Console.WriteLine("No REST Pull Integration Header Data......");
                        }

                        break;

                }
            }
            catch (Exception exp)
            {
                ExceptionWriter.WriteLogEntry($"Create Connection Exception {exp.Message}");
                StringBuilder strerrorMsg = new StringBuilder();
                strerrorMsg.AppendLine(exp.Message);
                strerrorMsg.AppendLine(exp.StackTrace);
                if (exp.InnerException != null)
                {
                    strerrorMsg.AppendLine(exp.InnerException.Message);
                    strerrorMsg.AppendLine(exp.InnerException.StackTrace);
                }
                //IntegrationTask.InsertIntegrationLog(strReadWriteConnectionString, 0, strerrorMsg.ToString(), string.Empty);
            }
            finally
            {

            }
            #endregion

            // return Integrations Ids
            //return new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
            return lstCommonIntegrationData;
        }



        public static void ProcessIntegrations(IntegrationType iType, List<CommonIntegrationData> integrations, int maxJobsInParallel = 3)
        {
            var commandToExecute = IntegrationActionMapping[iType];
            integrations = integrations ?? new List<CommonIntegrationData>();

            //var x2 = IntegrationType.File | IntegrationType.XmlFile;
            //switch (iType)
            //{
            //    case IntegrationType.File:
            //        if ((x2 & IntegrationType.File) == IntegrationType.File)
            //        {
            //            // do something
            //        }
            //        commandToExecute = ProcessFile;
            //        break;
            //    case IntegrationType.XmlFile:
            //        break;
            //    case IntegrationType.RestGet:
            //        break;
            //    case IntegrationType.RestPost:
            //        break;
            //    case IntegrationType.Soap:
            //        break;
            //}
            Parallel.ForEach(integrations, new ParallelOptions() { MaxDegreeOfParallelism = maxJobsInParallel },
                                x =>
                                {
                                    try
                                    {
                                        commandToExecute(x);
                                    }
                                    catch (Exception ex)
                                    {
                                        // log the message
                                        Console.WriteLine(ex.Message);
                                    }
                                }
                            );


            //var integrationsProcessed = 0;
            //do
            //{
            //    var integrationsToProcess = integrations
            //        .Skip(integrationsProcessed)
            //        .Take(maxJobsInParallel);
            //    // Parallel.ForEach waits for all the items in the batch to finish executing.
            //    // Use Task.Run with a collection instead
            //    // https://stackoverflow.com/questions/15136542/parallel-foreach-with-asynchronous-lambda
            //    Parallel.ForEach(integrationsToProcess, x => commandToExecute(x));


            //    integrationsProcessed += integrationsToProcess.Count();
            //} while (integrationsProcessed < integrations.Count);
        }
        //public static async Task ParallelForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> asyncAction, int maxDegreeOfParallelism)
        //{
        //    var throttler = new SemaphoreSlim(initialCount: maxDegreeOfParallelism);
        //    var tasks = source.Select(async item =>
        //    {
        //        await throttler.WaitAsync();
        //        try
        //        {
        //            await asyncAction(item).ConfigureAwait(false);
        //        }
        //        finally
        //        {
        //            throttler.Release();
        //        }
        //    });
        //    await Task.WhenAll(tasks);
        //}

        public static Dictionary<IntegrationType, Action<CommonIntegrationData>> IntegrationActionMapping = new Dictionary<IntegrationType, Action<CommonIntegrationData>>
        {

            { IntegrationType.RestGet, ProcessRESTPullIntegration },

        };

        private static int GetUserID(string strConnection)
        {
            Int32 intUserID = 0;
            using (var conn = new SqlConnection(strConnection))
            {
                using (var command = new SqlCommand("", conn))
                {
                    conn.Open();
                    string strQuery = string.Empty;
#if (UseeQuipOnDemand)
                    strQuery = " select top 1 UserID from [User] where UserID in (select User_ID from [User_Profile] )";
#else
                    strQuery = "select case when exists (select UserID from [User] where UserName = 'Admin') then (select UserID from [User] where UserName = 'Admin') else (select top 1 UserID from [User] where UserID in (select User_ID from [User_Profile] )) end ";
#endif

                    SqlCommand sqlcmd = new SqlCommand(strQuery, conn);

                    sqlcmd.CommandType = CommandType.Text;
                    intUserID = (Int32)sqlcmd.ExecuteScalar();
                }
                conn.Close();
            }
            return intUserID;
        }


        public static Int32 GetLanguageIDByUserID(string strConnection, Int32 UserID)
        {
            int id = 0;
            try
            {
                string strSql = "Select DefaultLanguageID from [User] Where UserID = @UserID";
                SqlConnection myConn = new SqlConnection(strConnection);
                SqlCommand objSqlCommand = new SqlCommand(); ;
                objSqlCommand.Connection = myConn;
                objSqlCommand.CommandText = strSql;
                myConn.Open();
                List<IDbDataParameter> objParams = new List<IDbDataParameter>();
                objSqlCommand.Parameters.Add(new SqlParameter("@UserID", UserID));
                object obj = objSqlCommand.ExecuteScalar();
                if (obj != null)
                {
                    id = Convert.ToInt32(obj);
                }
                myConn.Close();
                return id;
            }
            catch (Exception ex)
            {
                return id;
            }
        }

        #region Process Rest Get Integration

        private static void ProcessRESTPullIntegration(CommonIntegrationData objCommonIntegrationData)
        {
            var errors = new List<ErrorLog>();

            DataTable dtHeaderData = IntegrationTask.GetRESTPullIntegrationFrameworkHeaderData(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID);
            if (dtHeaderData != null && dtHeaderData.Rows.Count > 0)
            {
                var intUserID = 0;
                try
                {
                    //intUserID = GetUserID(objCommonIntegrationData.strConnection);
                    //var DefaultLanguageId = GetLanguageIDByUserID(objCommonIntegrationData.strConnection, intUserID);
                    DateTime minDate = new DateTime(1753, 1, 1);
                    var lastRunDate = Convert.ToString(dtHeaderData.Rows[0]["LastRun"]) == "" ? minDate.ToString() : Convert.ToString(dtHeaderData.Rows[0]["LastRun"]);

                    int intAuthenticationType = Convert.ToInt16(dtHeaderData.Rows[0]["AuthenticationType"]);
                    string strAuthenticationTypeName = string.Empty;
                    if (intAuthenticationType == Convert.ToInt16(AuthenticationType.APIKey))
                    {
                        strAuthenticationTypeName = "APIKey";
                    }
                    else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.UserNamePassword))
                    {
                        strAuthenticationTypeName = "UserNamePassword";
                    }
                    else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.OAUTH))
                    {
                        strAuthenticationTypeName = "OAUTH";
                    }
                    else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.BasicAuth))
                    {
                        strAuthenticationTypeName = "Basic Authentication";
                    }

                    if (!string.IsNullOrEmpty(objCommonIntegrationData.strCustomerID))
                    {
                        Console.WriteLine("Process REST Pull Integration (For CustomerID : " + objCommonIntegrationData.strCustomerID + ") : " + Convert.ToString(dtHeaderData.Rows[0]["Name"]).Trim() + " AuthenticationType: " + strAuthenticationTypeName);
                    }
                    else
                    {
                        Console.WriteLine("Process REST Pull Integration : " + Convert.ToString(dtHeaderData.Rows[0]["Name"]) + " AuthenticationType: " + strAuthenticationTypeName);
                    }

                    DataTable dtDetail = IntegrationTask.GetRESTPullIntegrationFrameworkDetailData(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID);
                    // Get Header mappings

                    //DataTable dtDetail = new DataTable();
                    DataTable Detail = new DataTable();
                    string TemplateTableID = "2"; // ideally from DB/config


                    #region NEW CODE FROM XML FILE DETAIL

                    //if (!string.IsNullOrEmpty(TemplateTableID))
                    //{
                    //    // Header mappings
                    //    var headerMappings = GetTemplateTblIdentificationColumns(
                    //        objCommonIntegrationData.strConnection,
                    //        "ChartType",
                    //        objCommonIntegrationData.HeaderID,
                    //        "Header"
                    //    );

                    //    // Multiple detail tables — you can later load this list dynamically
                    //    List<string> detailTblList = new List<string> { "ControlType" };
                    //    Dictionary<string, Dictionary<string, object>> dictionary = new Dictionary<string, Dictionary<string, object>>();
                    //    Dictionary<string, Dictionary<string, object>> detailMappings = dictionary;

                    //    foreach (string detailTable in detailTblList)
                    //    {
                    //        var mapping = GetTemplateTblIdentificationColumns(
                    //            objCommonIntegrationData.strConnection,
                    //            detailTable,
                    //            objCommonIntegrationData.HeaderID,
                    //            "Detail"
                    //        );

                    //        foreach (var kvp in mapping)
                    //        {
                    //            string uniqueKey = $"{detailTable}_{kvp.Key}";
                    //            detailMappings[uniqueKey] = kvp.Value;
                    //        }
                    //    }

                    //    // Merge header + details
                    //    var allMappings = new Dictionary<string, Dictionary<string, object>>(headerMappings);
                    //    foreach (var kvp in detailMappings)
                    //        allMappings[kvp.Key] = kvp.Value;

                    //    // Convert to DataTables
                    //    dtDetail = ConvertDictionaryToDataTable(allMappings); // full (header + detail)
                    //    Detail = ConvertDictionaryToDataTable(detailMappings); // only detail
                    //}

                    #endregion


                    //DataTable dtDetail = IntegrationTask.GetRESTPullIntegrationFrameworkDetailData(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID);
                    DataTable dtReqParams = IntegrationTask.GetRESTAPIsRequestParameters(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID);
                    DataTable dtGraphQLParams = new DataTable();
                    DataTable dtWithoutGraphQLParams = new DataTable();

                    // Filter GraphQL parameters (ParameterType = 'GraphQL')
                    var rowsGraphQLParams = dtReqParams.AsEnumerable().Where(x => 
                        x.Field<string>("ParameterType")?.Equals("GraphQL", StringComparison.OrdinalIgnoreCase) == true);
                    if (rowsGraphQLParams.Any())
                    {
                        dtGraphQLParams = rowsGraphQLParams.CopyToDataTable();
                    }
                    
                    // Filter non-GraphQL parameters
                    var rowsWithoutGraphQLParams = dtReqParams.AsEnumerable().Where(x => 
                        !string.Equals(x.Field<string>("ParameterType"), "GraphQL", StringComparison.OrdinalIgnoreCase));
                    if (rowsWithoutGraphQLParams.Any())
                    {
                        dtWithoutGraphQLParams = rowsWithoutGraphQLParams.CopyToDataTable();
                    }
                    DataTable objLogin;
                    var fieldCollectionlist = new List<FieldCollections>();
                    if (dtDetail != null && dtDetail.Rows.Count > 0)
                    {
                        var tableName = Convert.ToString(dtHeaderData.Rows[0]["Entity"]);

                        //List<Appointment> objAppointments = IntegrationTask.GetFileIntegration(dtHeaderData);
                        //foreach (Appointment objAppointment in objAppointments)
                        //{
                        //    if (objAppointment.RecurrenceState == RecurrenceState.Master)
                        //    {
                        //        RecurrenceRule parsedRule;
                        //        RecurrenceRule.TryParse(objAppointment.RecurrenceRule.ToString(), out parsedRule);
                        //        if (parsedRule.Occurrences.Any(p => p.Date == DateTime.Now.Date))
                        //        {
                        //            DateTime dtStartDate = Convert.ToDateTime(dtHeaderData.Rows[0]["StartDate"]);
                        //            if (DateTime.Now.TimeOfDay >= dtStartDate.TimeOfDay)
                        //            {

                        bool IsExist = IntegrationTask.SaveIntegrationTaskStatuswithFailure(objCommonIntegrationData.strConnection, Convert.ToString(dtHeaderData.Rows[0]["ID"]), DateTime.Now.Date, objCommonIntegrationData.strCustomerID, false, false, "REST Pull Integration");

                        IsExist = false;
                        if (!IsExist)
                        {
                            try
                            {

                                IEnumerable<string> streQuipTableNameList = dtDetail.AsEnumerable().Select(x => x.Field<string>("Entity")).AsEnumerable().Distinct();
                                objLogin = new DataTable();
                                //string RestPullResponse = Convert.ToString(dtHeaderData.Rows[0]["RestPullResponse"]).Trim();
                                foreach (string strTableName in streQuipTableNameList)
                                {
                                    DataTable dt = dtDetail.AsEnumerable().Where(x => x.Field<string>("Entity") == strTableName).AsEnumerable().CopyToDataTable();
                                    if (dt != null && dt.Rows.Count > 0)
                                    {
                                        string strAPIURL = Convert.ToString(dtHeaderData.Rows[0]["APIURL"]).Trim();
                                        string strAPIKeyName = Convert.ToString(dtHeaderData.Rows[0]["APIKeyName"]).Trim();
                                        string strAPIKeyValue = Convert.ToString(dtHeaderData.Rows[0]["APIKeyValue"]).Trim();

                                        if (dtGraphQLParams != null && dtGraphQLParams.Rows.Count > 0)
                                        {
                                            if (Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterType"]) == Convert.ToBoolean(RESTPullAPIParameterType.URLParameter))
                                            {
                                                if (!string.IsNullOrEmpty(strAPIKeyValue.Trim()))
                                                {
                                                    strAPIURL = strAPIURL + "/" + strAPIKeyValue;
                                                }
                                            }
                                            else
                                            {
                                                strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + strAPIKeyValue;
                                            }
                                            //if (!string.IsNullOrEmpty(strAPIKeyValue.Trim()))
                                            //{
                                            //    strAPIURL = strAPIURL + "/" + strAPIKeyValue;
                                            //}
                                            object inputJson = CallGraphQLAsync(strAPIURL, Convert.ToString(dtGraphQLParams.Rows[0]["GraphQLQuery"]).Trim()).Result;

                                            if (inputJson != DBNull.Value)
                                            {
                                                string strAPIResponse = Convert.ToString(inputJson);
                                                //string strAPIResponse = Convert.ToString(dtHeaderData.Rows[0]["RestPullResponse"]).Trim();
                                                dynamic apiResponse = Newtonsoft.Json.JsonConvert.DeserializeObject(strAPIResponse);

                                                ExceptionWriter.WriteLogEntry("Rest Pull GraphQL Response for customer ID " + objCommonIntegrationData.strCustomerID + " " + DateTime.Now.ToString());
                                                ExceptionWriter.WriteLogEntry("Rest Pull GraphQL Response " + Convert.ToString(apiResponse) + " " + DateTime.Now.ToString());

                                                if (apiResponse != null)
                                                {
                                                    if (apiResponse.GetType() == typeof(Newtonsoft.Json.Linq.JObject) || apiResponse.GetType() == typeof(Newtonsoft.Json.Linq.JValue) || apiResponse == null)
                                                    {
                                                        var properties = apiResponse.Properties();
                                                        foreach (var property in properties)
                                                        {
                                                            if (property.Value.GetType() == typeof(Newtonsoft.Json.Linq.JObject) || property.Value.GetType() == typeof(Newtonsoft.Json.Linq.JArray))
                                                            {
                                                                Newtonsoft.Json.Linq.JArray array = Newtonsoft.Json.Linq.JArray.Parse(property.Value.ToString());
                                                                foreach (Newtonsoft.Json.Linq.JToken jToken in array)
                                                                {
                                                                    Newtonsoft.Json.Linq.JObject ObjinputValues = Newtonsoft.Json.Linq.JObject.FromObject(jToken);
                                                                    List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(ObjinputValues, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);
                                                                }
                                                            }
                                                            else
                                                            {
                                                                List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(property, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(apiResponse, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);
                                                    }
                                                }
                                            }
                                        }

                                        #region APIKey & UserNamePassword
                                        else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.APIKey) ||
                                            Convert.ToInt16(dtHeaderData.Rows[0]["AuthenticationType"]) == Convert.ToInt16(AuthenticationType.UserNamePassword))
                                        {
                                            string KeyNameOfUserName = Convert.ToString(dtHeaderData.Rows[0]["KeyNameOfUserName"]).Trim();
                                            string KeyValueOfUserName = Convert.ToString(dtHeaderData.Rows[0]["KeyValueOfUserName"]).Trim();
                                            string KeyNameOfPassword = Convert.ToString(dtHeaderData.Rows[0]["KeyNameOfPassword"]).Trim();
                                            string KeyValueOfPassword = Convert.ToString(dtHeaderData.Rows[0]["KeyValueOfPassword"]).Trim();
                                            string strLoginAPIURL = Convert.ToString(dtHeaderData.Rows[0]["LoginAPIURL"]).Trim();

                                            if (intAuthenticationType == Convert.ToInt16(AuthenticationType.APIKey))
                                            {
                                                if (Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterType"]) == Convert.ToBoolean(RESTPullAPIParameterType.URLParameter))
                                                {
                                                    //if (!string.IsNullOrEmpty(strAPIKeyValue.Trim()))
                                                    //{
                                                    //    strAPIURL = strAPIURL + "/" + strAPIKeyValue;
                                                    //}
                                                    if (dtWithoutGraphQLParams != null && dtWithoutGraphQLParams.Rows.Count > 0)
                                                    {
                                                        StringBuilder strURLParameters = new StringBuilder();
                                                        foreach (DataRow drparms in dtWithoutGraphQLParams.Rows)
                                                        {
                                                            string strFName = Convert.ToString(drparms["ParameterName"]).Trim();
                                                            string strParameterValue = string.Empty;
                                                            string strDataType = string.Empty;

                                                            if (!string.IsNullOrEmpty(drparms["DataType"].ToString().Trim()))
                                                            {
                                                                int Offset = (!string.IsNullOrEmpty(drparms["Offset"].ToString().Trim())) ? Convert.ToInt32(drparms["Offset"].ToString()) : 0;

                                                                if (drparms["DataType"].ToString() == "Date")//Data Type =Date
                                                                {
                                                                    int Typeindex = Convert.ToInt32(drparms["DateType"].ToString());
                                                                    if (Typeindex == 1)//"Specified Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"]));
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"])).ToString("yyyy-MM-dd");
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 2)//"Current Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = DateTime.Now;
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 3)//"Beginning Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            strParameterValue = Convert.ToDateTime(startDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 4)//"End Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            var finalDate = endDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(finalDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }

                                                                    }
                                                                    else if (Typeindex == 5)//"Last Run (DateTime)"
                                                                    {
                                                                        DateTime lastRunDateTime = Convert.ToDateTime(lastRunDate);
                                                                        lastRunDateTime = DateTime.SpecifyKind(lastRunDateTime, DateTimeKind.Utc);
                                                                        if (Offset != 0)
                                                                        {
                                                                            var _lastRunDate = lastRunDateTime.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(_lastRunDate).ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = lastRunDateTime.ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                strParameterValue = Convert.ToString(drparms["ParameterValue"]);
                                                            }

                                                            strURLParameters.AppendLine("&" + strFName + "=" + strParameterValue);
                                                        }
                                                        if (!string.IsNullOrEmpty(Convert.ToString(strURLParameters)))
                                                        {
                                                            strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + strAPIKeyValue + Convert.ToString(strURLParameters).Trim().Replace("\r\n", "") + "&format=json";
                                                        }
                                                    }
                                                    else
                                                    {
                                                        strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + strAPIKeyValue + "&format=json";
                                                    }
                                                }
                                                else
                                                {
                                                    objLogin.Columns.Add(strAPIKeyName);
                                                    DataRow drHeaderParms;
                                                    drHeaderParms = objLogin.NewRow();

                                                    drHeaderParms[strAPIKeyName] = strAPIKeyValue;
                                                    objLogin.Rows.Add(drHeaderParms);

                                                    if (dtWithoutGraphQLParams != null && dtWithoutGraphQLParams.Rows.Count > 0)
                                                    {
                                                        StringBuilder strURLParameters = new StringBuilder();
                                                        foreach (DataRow drparms in dtWithoutGraphQLParams.Rows)
                                                        {
                                                            string strFName = Convert.ToString(drparms["ParameterName"]).Trim();
                                                            string strParameterValue = string.Empty;
                                                            string strDataType = string.Empty;

                                                            if (!string.IsNullOrEmpty(drparms["DataType"].ToString().Trim()))
                                                            {
                                                                int Offset = (!string.IsNullOrEmpty(drparms["Offset"].ToString().Trim())) ? Convert.ToInt32(drparms["Offset"].ToString()) : 0;

                                                                if (drparms["DataType"].ToString() == "Date")//Data Type =Date
                                                                {
                                                                    int Typeindex = Convert.ToInt32(drparms["DateType"].ToString());
                                                                    if (Typeindex == 1)//"Specified Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"]));
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"])).ToString("yyyy-MM-dd");
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 2)//"Current Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = DateTime.Now;
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 3)//"Beginning Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            strParameterValue = Convert.ToDateTime(startDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 4)//"End Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            var finalDate = endDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(finalDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }

                                                                    }
                                                                    else if (Typeindex == 5)//"Last Run (DateTime)"
                                                                    {
                                                                        DateTime lastRunDateTime = Convert.ToDateTime(lastRunDate);
                                                                        lastRunDateTime = DateTime.SpecifyKind(lastRunDateTime, DateTimeKind.Utc);
                                                                        if (Offset != 0)
                                                                        {
                                                                            var _lastRunDate = lastRunDateTime.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(_lastRunDate).ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = lastRunDateTime.ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                strParameterValue = Convert.ToString(drparms["ParameterValue"]);
                                                            }

                                                            if (Convert.ToInt32(drparms["RequestParameterType"]) == (int)EnumRequestParameterType.URLParameter)
                                                            {
                                                                strURLParameters.AppendLine("&" + strFName + "=" + strParameterValue.Trim().Replace("\r\n", ""));
                                                            }
                                                            else
                                                            {
                                                                objLogin.Columns.Add(strFName);
                                                                //drHeaderParms = objLogin.NewRow();
                                                                drHeaderParms[strFName] = strParameterValue.Trim();
                                                                //objLogin.Rows.Add(drHeaderParms);
                                                                objLogin.AcceptChanges();
                                                            }
                                                        }
                                                        if (!string.IsNullOrEmpty(Convert.ToString(strURLParameters)))
                                                        {
                                                            strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + strAPIKeyValue + Convert.ToString(strURLParameters).Trim().Replace("\r\n", "") + "&format=json";
                                                        }
                                                    }
                                                }
                                                var apiUrl = strAPIURL;
                                                if (!apiUrl.Contains("format=json"))
                                                {
                                                    apiUrl = apiUrl.Contains("?") ? $"{apiUrl}&format=json" : $"{apiUrl}?format=json";
                                                }

                                                object objAPI = GetLoginAPISingle<object>(apiUrl, Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterType"]), objLogin, AuthenticationType.APIKey).GetAwaiter().GetResult();

                                                if (objAPI != DBNull.Value)
                                                {
                                                    var jsonText = objAPI.ToString().Trim();

                                                    if (string.IsNullOrWhiteSpace(jsonText))
                                                    {
                                                        Console.WriteLine("Empty or null JSON response.");
                                                        return;
                                                    }

                                                    JToken inputJson;

                                                    try
                                                    {
                                                        inputJson = JToken.Parse(jsonText);
                                                    }
                                                    catch
                                                    {
                                                        Console.WriteLine("Invalid JSON received: " + jsonText);
                                                        return;
                                                    }

                                                    if (inputJson is JArray array)
                                                    {
                                                        foreach (var item in array.OfType<JObject>())
                                                        {
                                                            SaveRESTPullIntegrationData(item, dt, strTableName,
                                                                objCommonIntegrationData.strConnection, intUserID,
                                                                objCommonIntegrationData.HeaderID,
                                                                Convert.ToInt32(objCommonIntegrationData.strCustomerID),
                                                                fieldCollectionlist, null);
                                                        }
                                                    }
                                                    else if (inputJson is JObject obj)
                                                    {
                                                        foreach (var prop in obj.Properties())
                                                        {
                                                            if (prop.Value is JArray propArray)
                                                            {
                                                                foreach (var item in propArray.OfType<JObject>())
                                                                {
                                                                    SaveRESTPullIntegrationData(item, dt, strTableName,
                                                                        objCommonIntegrationData.strConnection, intUserID,
                                                                        objCommonIntegrationData.HeaderID,
                                                                        Convert.ToInt32(objCommonIntegrationData.strCustomerID),
                                                                        fieldCollectionlist, null);
                                                                }
                                                            }
                                                            else if (prop.Value is JObject singleObj)
                                                            {
                                                                SaveRESTPullIntegrationData(singleObj, dt, strTableName,
                                                                    objCommonIntegrationData.strConnection, intUserID,
                                                                    objCommonIntegrationData.HeaderID,
                                                                    Convert.ToInt32(objCommonIntegrationData.strCustomerID),
                                                                    fieldCollectionlist, null);
                                                            }
                                                            else
                                                            {
                                                                Console.WriteLine($"Skipping non-JSON data: {prop.Value}");
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        Console.WriteLine("Unsupported JSON structure received.");
                                                    }
                                                }

                                            }
                                            else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.UserNamePassword))
                                            {
                                                //string strDecryptedADPassword = string.Empty;
                                                //if (!string.IsNullOrEmpty(KeyValueOfPassword.Trim()))
                                                //{
                                                //    strDecryptedADPassword = Decrypt(objCommonIntegrationData.strConnection, KeyValueOfPassword, "1234");
                                                //    //strDecryptedADPassword = "Ravina@12345";
                                                //}

                                                //if (Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"]) == Convert.ToBoolean(RESTPullAPIParameterType.URLParameter))
                                                //{
                                                //    strLoginAPIURL = strLoginAPIURL + "/" + KeyNameOfUserName + "/" + KeyNameOfPassword;
                                                //}


                                                if (!Convert.IsDBNull(dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"]) &&
                                                  Convert.ToInt32(dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"]) == (int)RESTPullAPIParameterType.URLParameter)
                                                {
                                                    strLoginAPIURL = strLoginAPIURL + "/" + KeyNameOfUserName + "/" + KeyNameOfPassword;
                                                }




                                                //else
                                                //{
                                                //    // If KeyNameOfUserName or KeyNameOfPassword are null or empty, give fallback names
                                                //    string colUserName = !string.IsNullOrWhiteSpace(KeyNameOfUserName) ? KeyNameOfUserName : "Name";
                                                //    string colPassword = !string.IsNullOrWhiteSpace(KeyNameOfPassword) ? KeyNameOfPassword : "Password";

                                                //    // Add columns only if they don't exist
                                                //    if (!objLogin.Columns.Contains(colUserName))
                                                //        objLogin.Columns.Add(colUserName);

                                                //    if (!objLogin.Columns.Contains(colPassword))
                                                //        objLogin.Columns.Add(colPassword);

                                                //    // Create row
                                                //    DataRow drLogin = objLogin.NewRow();

                                                //    // Fill values (use empty string when missing)
                                                //    drLogin[colUserName] = KeyValueOfUserName ?? string.Empty;
                                                //    drLogin[colPassword] = Convert.ToBase64String(
                                                //        Encoding.UTF8.GetBytes(strDecryptedADPassword ?? string.Empty)
                                                //    );

                                                //    objLogin.Rows.Add(drLogin);
                                                //}
                                                else
                                                {
                                                    // Safely convert all possible DBNull values to string
                                                    string safeKeyNameOfUserName = Convert.ToString(KeyNameOfUserName);
                                                    string safeKeyNameOfPassword = Convert.ToString(KeyNameOfPassword);
                                                    string safeKeyValueOfUserName = Convert.ToString(KeyValueOfUserName);
                                                    //string safeDecryptedADPassword = Convert.ToString(strDecryptedADPassword);

                                                    // If KeyNameOfUserName or KeyNameOfPassword are null or empty, give fallback names
                                                    string colUserName = !string.IsNullOrWhiteSpace(safeKeyNameOfUserName) ? safeKeyNameOfUserName : "Name";
                                                    string colPassword = !string.IsNullOrWhiteSpace(safeKeyNameOfPassword) ? safeKeyNameOfPassword : "Password";

                                                    // Add columns only if they don't exist
                                                    if (!objLogin.Columns.Contains(colUserName))
                                                        objLogin.Columns.Add(colUserName);

                                                    if (!objLogin.Columns.Contains(colPassword))
                                                        objLogin.Columns.Add(colPassword);

                                                    // Create new row
                                                    DataRow drLogin = objLogin.NewRow();

                                                    // Fill values safely
                                                    drLogin[colUserName] = safeKeyValueOfUserName;
                                                    drLogin[colPassword] = 1234;
                                                    //drLogin[colPassword] = Convert.ToBase64String(
                                                    //    Encoding.UTF8.GetBytes(safeDecryptedADPassword)
                                                    //);

                                                    objLogin.Rows.Add(drLogin);
                                                }



                                                //dynamicObject.Add(KeyNameOfUserName, KeyValueOfUserName);
                                                //dynamicObject.Add(KeyNameOfPassword, "1234");
                                                strLoginAPIURL = strLoginAPIURL + "?format=json";

                                                bool paramTypeForLoginAPI = dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"] != DBNull.Value && Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"]);

                                                object objLoginAPI = GetLoginAPISingle<object>(
                                                    strLoginAPIURL,
                                                    paramTypeForLoginAPI,
                                                    objLogin,
                                                    AuthenticationType.UserNamePassword
                                                ).GetAwaiter().GetResult();

                                                objLogin = new DataTable();

                                                if (objLoginAPI != DBNull.Value)
                                                {
                                                    dynamic objLogindata = Newtonsoft.Json.Linq.JObject.Parse(objLoginAPI.ToString());
                                                    string authorization = objLogindata[Convert.ToString(dtHeaderData.Rows[0]["TokenKeyNameAfterLogin"])];
                                                    strAPIKeyName = Convert.ToString(dtHeaderData.Rows[0]["TokenKeyNameAfterLogin"]);


                                                    //if (Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterType"]) == Convert.ToBoolean(RESTPullAPIParameterType.URLParameter))
                                                    //{
                                                    //    strAPIURL = strAPIURL + "/" + authorization; 
                                                    //}
                                                    //else
                                                    //{
                                                    objLogin.Columns.Add(strAPIKeyName);
                                                    DataRow drHeaderParms;
                                                    drHeaderParms = objLogin.NewRow();

                                                    drHeaderParms[strAPIKeyName] = authorization;
                                                    objLogin.Rows.Add(drHeaderParms);

                                                    if (dtGraphQLParams != null && dtGraphQLParams.Rows.Count > 0)
                                                    {
                                                        foreach (DataRow drparms in dtGraphQLParams.Rows)
                                                        {
                                                            string strFName = Convert.ToString(drparms["ParameterName"]).Trim();
                                                            string strParameterValue = string.Empty;
                                                            if (!string.IsNullOrEmpty(Convert.ToString(drparms["ParameterValue"])))
                                                            {
                                                                strParameterValue = Convert.ToString(drparms["ParameterValue"]).Trim();
                                                            }
                                                            objLogin.Columns.Add(strFName);

                                                            drHeaderParms = objLogin.NewRow();

                                                            drHeaderParms[strFName] = strParameterValue;
                                                            objLogin.Rows.Add(drHeaderParms);
                                                        }
                                                    }
                                                    //}

                                                    if (dtWithoutGraphQLParams != null && dtWithoutGraphQLParams.Rows.Count > 0)
                                                    {
                                                        StringBuilder strURLParameters = new StringBuilder();
                                                        foreach (DataRow drparms in dtWithoutGraphQLParams.Rows)
                                                        {
                                                            string strFName = Convert.ToString(drparms["ParameterName"]).Trim();
                                                            string strParameterValue = string.Empty;
                                                            string strDataType = string.Empty;

                                                            if (!string.IsNullOrEmpty(drparms["DataType"].ToString().Trim()))
                                                            {
                                                                int Offset = (!string.IsNullOrEmpty(drparms["Offset"].ToString().Trim())) ? Convert.ToInt32(drparms["Offset"].ToString()) : 0;

                                                                if (drparms["DataType"].ToString() == "Date")//Data Type =Date
                                                                {
                                                                    int Typeindex = Convert.ToInt32(drparms["DateType"].ToString());
                                                                    if (Typeindex == 1)//"Specified Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"]));
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(Convert.ToString(drparms["ParameterValue"])).ToString("yyyy-MM-dd");
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 2)//"Current Date"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime value = DateTime.Now;
                                                                            var endDate = value.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 3)//"Beginning Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            strParameterValue = Convert.ToDateTime(startDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                    else if (Typeindex == 4)//"End Of Month"
                                                                    {
                                                                        if (Offset != 0)
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            var finalDate = endDate.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(finalDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            DateTime currunt = DateTime.Now;
                                                                            var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                                            var endDate = startDate.AddMonths(1).AddDays(-1);
                                                                            strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                                                        }

                                                                    }
                                                                    else if (Typeindex == 5)//"Last Run (DateTime)"
                                                                    {
                                                                        DateTime lastRunDateTime = Convert.ToDateTime(lastRunDate);
                                                                        lastRunDateTime = DateTime.SpecifyKind(lastRunDateTime, DateTimeKind.Utc);
                                                                        if (Offset != 0)
                                                                        {
                                                                            var _lastRunDate = lastRunDateTime.AddDays(Offset);
                                                                            strParameterValue = Convert.ToDateTime(_lastRunDate).ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                        else
                                                                        {
                                                                            strParameterValue = lastRunDateTime.ToString("yyyy-MM-dd hh:mm:ss", CultureInfo.InvariantCulture);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                strParameterValue = Convert.ToString(drparms["ParameterValue"]);
                                                            }

                                                            if (Convert.ToInt32(drparms["RequestParameterType"]) == (int)EnumRequestParameterType.URLParameter)
                                                            {
                                                                strURLParameters.AppendLine("&" + strFName + "=" + strParameterValue.Trim().Replace("\r\n", ""));
                                                            }
                                                            else
                                                            {
                                                                objLogin.Columns.Add(strFName);
                                                                //drHeaderParms = objLogin.NewRow();
                                                                drHeaderParms[strFName] = strParameterValue.Trim();
                                                                //objLogin.Rows.Add(drHeaderParms);
                                                                objLogin.AcceptChanges();
                                                            }
                                                        }

                                                        if (!string.IsNullOrEmpty(Convert.ToString(strURLParameters)))
                                                        {
                                                            strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + authorization + Convert.ToString(strURLParameters).Trim().Replace("\r\n", "") + "&format=json";
                                                        }
                                                    }
                                                    else
                                                    {
                                                        strAPIURL = strAPIURL.TrimEnd('/') + "?" + strAPIKeyName + "=" + authorization + "&format=json";
                                                    }

                                                    var apiUrl = strAPIURL;
                                                    if (!apiUrl.Contains("format=json"))
                                                    {
                                                        apiUrl = apiUrl.Contains("?") ? $"{apiUrl}&format=json" : $"{apiUrl}?format=json";
                                                    }

                                                    object objAPI = GetLoginAPISingle<object>(apiUrl, Convert.ToBoolean(dtHeaderData.Rows[0]["ParameterType"]), objLogin, AuthenticationType.UserNamePassword).GetAwaiter().GetResult();

                                                    if (objAPI != DBNull.Value)
                                                    {
                                                        dynamic inputJson = Newtonsoft.Json.JsonConvert.DeserializeObject(objAPI.ToString());

                                                        if (inputJson != null)
                                                        {
                                                            if (inputJson.GetType() == typeof(Newtonsoft.Json.Linq.JObject) || inputJson.GetType() == typeof(Newtonsoft.Json.Linq.JValue) || inputJson == null)
                                                            {
                                                                var properties = inputJson.Properties();
                                                                foreach (var property in properties)
                                                                {
                                                                    if (property.Value.GetType() == typeof(Newtonsoft.Json.Linq.JObject) || property.Value.GetType() == typeof(Newtonsoft.Json.Linq.JArray))
                                                                    {
                                                                        Newtonsoft.Json.Linq.JArray array = Newtonsoft.Json.Linq.JArray.Parse(property.Value.ToString());
                                                                        foreach (Newtonsoft.Json.Linq.JToken jToken in array)
                                                                        {
                                                                            Newtonsoft.Json.Linq.JObject ObjinputValues = Newtonsoft.Json.Linq.JObject.FromObject(jToken);
                                                                            List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(array, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);
                                                                        }
                                                                        //List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(property.Value, dtDetail, Convert.ToString(dtHeaderData.Rows[0]["TableName"]), strConnection, intUserID);
                                                                    }
                                                                    else
                                                                    {
                                                                        Newtonsoft.Json.Linq.JArray array = Newtonsoft.Json.Linq.JArray.Parse(property.Value.ToString());
                                                                        foreach (Newtonsoft.Json.Linq.JToken jToken in array)
                                                                        {
                                                                            Newtonsoft.Json.Linq.JObject ObjinputValues = Newtonsoft.Json.Linq.JObject.FromObject(jToken);
                                                                            List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(ObjinputValues, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);
                                                                        }
                                                                        //List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(property.Value, dtDetail, Convert.ToString(dtHeaderData.Rows[0]["TableName"]), strConnection, intUserID);
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                Newtonsoft.Json.Linq.JArray array = Newtonsoft.Json.Linq.JArray.Parse(inputJson.ToString());


                                                                List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(array, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors);

                                                                //List<Tuple<string, string>> fieldNames = SaveRESTPullIntegrationData(inputJson, dtDetail, Convert.ToString(dtHeaderData.Rows[0]["TableName"]), strConnection, intUserID);

                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        #endregion

                                        #region OAuth and Basic Authentication
                                        else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.OAUTH) || intAuthenticationType == Convert.ToInt16(AuthenticationType.BasicAuth))
                                        {
                                            var apiUrlVerb = dtHeaderData.Rows[0]["ApiUrlverb"] != DBNull.Value && dtHeaderData.Rows[0]["ApiUrlverb"] != null
                                               ? Convert.ToString(dtHeaderData.Rows[0]["ApiUrlverb"]).Trim()
                                               : "GET";

                                            Method methodForUrl = apiUrlVerb.ToLower() == "POST".ToLower() ? Method.Post : Method.Get;

                                            var tokenizedRequestTemplate = dtHeaderData.Rows[0]["TokenizedRequestTemplate"] != DBNull.Value && dtHeaderData.Rows[0]["TokenizedRequestTemplate"] != null
                                                ? Convert.ToString(dtHeaderData.Rows[0]["TokenizedRequestTemplate"]).Trim()
                                                : "";

                                            dtReqParams = dtReqParams ?? new DataTable();

                                            var body = tokenizedRequestTemplate;
                                            var defaultContentType = "";

                                            if (methodForUrl != Method.Get)
                                            {
                                                defaultContentType = "application/json";
                                            }

                                            var contentType = "";
                                            var pageNumberParameterName = "";
                                            var startPageNumber = 1;

                                            var urlParameters = new Dictionary<string, Object>();
                                            var bodyParameters = new Dictionary<string, Object>();
                                            var headerParameters = new Dictionary<string, Object>();
                                            var stringTokenParameters = new Dictionary<string, Object>();

                                            foreach (DataRow drParam in dtReqParams.Rows)
                                            {
                                                var strParameterName = Convert.ToString(drParam["ParameterName"]).Trim();
                                                var strParameterValue = string.Empty;

                                                //if (!string.IsNullOrEmpty(drParam["DataType"].ToString().Trim()))
                                                //{
                                                //    var Offset = (!string.IsNullOrEmpty(drParam["Offset"].ToString().Trim())) ? Convert.ToInt32(drParam["Offset"].ToString()) : 0;

                                                //    if (drParam["DataType"].ToString() == "Date")//Data Type =Date
                                                //    {
                                                //        var Typeindex = Convert.ToInt32(drParam["DateType"].ToString());
                                                //        if (Typeindex == 1)//"Specified Date"
                                                //        {
                                                //            if (Offset != 0)
                                                //            {
                                                //                DateTime value = Convert.ToDateTime(Convert.ToString(drParam["ParameterValue"]));
                                                //                var endDate = value.AddDays(Offset);
                                                //                strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //            else
                                                //            {
                                                //                strParameterValue = Convert.ToDateTime(Convert.ToString(drParam["ParameterValue"])).ToString("yyyy-MM-ddT00:00:00.000000000");
                                                //            }
                                                //        }
                                                //        else if (Typeindex == 2)//"Current Date"
                                                //        {
                                                //            if (Offset != 0)
                                                //            {
                                                //                DateTime value = DateTime.Now;
                                                //                var endDate = value.AddDays(Offset);
                                                //                strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //            else
                                                //            {
                                                //                strParameterValue = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //        }
                                                //        else if (Typeindex == 3)//"Beginning Of Month"
                                                //        {
                                                //            if (Offset != 0)
                                                //            {
                                                //                DateTime currunt = DateTime.Now;
                                                //                var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                //                var endDate = startDate.AddDays(Offset);
                                                //                strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //            else
                                                //            {
                                                //                DateTime currunt = DateTime.Now;
                                                //                var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                //                strParameterValue = Convert.ToDateTime(startDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //        }
                                                //        else if (Typeindex == 4)//"End Of Month"
                                                //        {
                                                //            if (Offset != 0)
                                                //            {
                                                //                DateTime currunt = DateTime.Now;
                                                //                var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                //                var endDate = startDate.AddMonths(1).AddDays(-1);
                                                //                var finalDate = endDate.AddDays(Offset);
                                                //                strParameterValue = Convert.ToDateTime(finalDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //            else
                                                //            {
                                                //                DateTime currunt = DateTime.Now;
                                                //                var startDate = new DateTime(currunt.Year, currunt.Month, 1);
                                                //                var endDate = startDate.AddMonths(1).AddDays(-1);
                                                //                strParameterValue = Convert.ToDateTime(endDate).ToString("yyyy-MM-ddT00:00:00.000000000", CultureInfo.InvariantCulture);
                                                //            }

                                                //        }
                                                //        else if (Typeindex == 5)//"Last Run (DateTime)"
                                                //        {
                                                //            DateTime lastRunDateTime = Convert.ToDateTime(lastRunDate);
                                                //            lastRunDateTime = DateTime.SpecifyKind(lastRunDateTime, DateTimeKind.Utc);
                                                //            if (Offset != 0)
                                                //            {
                                                //                var _lastRunDate = lastRunDateTime.AddDays(Offset);
                                                //                strParameterValue = Convert.ToDateTime(_lastRunDate).ToString("yyyy-MM-ddThh:mm:ss.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //            else
                                                //            {
                                                //                strParameterValue = lastRunDateTime.ToString("yyyy-MM-ddThh:mm:ss.000000000", CultureInfo.InvariantCulture);
                                                //            }
                                                //        }
                                                //    }
                                                //    else
                                                //    {
                                                //        strParameterValue = Convert.ToString(drParam["ParameterValue"]);
                                                //    }

                                                //    if (drParam["DataType"].ToString().ToLower() == "PageNumber".ToLower())
                                                //    {
                                                //        pageNumberParameterName = strParameterName;
                                                //        Int32.TryParse(Convert.ToString(drParam["ParameterValue"]), out startPageNumber);
                                                //    }
                                                //}
                                                //else
                                                //{
                                                //    strParameterValue = Convert.ToString(drParam["ParameterValue"]);
                                                //}

                                                if (Convert.ToString(drParam["ParamType"]).Trim() == "1")
                                                {
                                                    bodyParameters.Add(strParameterName, strParameterValue.Trim().Replace("\r\n", ""));
                                                }
                                                else if (Convert.ToString(drParam["ParamType"]).Trim() == "2")
                                                {
                                                    urlParameters.Add(strParameterName, strParameterValue.Trim().Replace("\r\n", ""));
                                                }
                                                else if (Convert.ToString(drParam["ParamType"]).Trim() == "3")
                                                {
                                                    stringTokenParameters.Add(strParameterName, strParameterValue.Trim().Replace("\r\n", ""));
                                                }
                                                else if (Convert.ToString(drParam["ParamType"]).Trim() == "4" && strParameterName.ToLower() != "Content-Type".ToLower())
                                                {
                                                    headerParameters.Add(strParameterName, strParameterValue.Trim().Replace("\r\n", ""));
                                                }

                                                if (strParameterName.ToLower() == "Content-Type".ToLower())
                                                {
                                                    contentType = strParameterValue;
                                                }

                                            }

                                            contentType = !string.IsNullOrEmpty(contentType) ? contentType : defaultContentType;

                                            if (!string.IsNullOrEmpty(contentType.Trim()) && !contentType.ToUpper().Equals(defaultContentType.ToUpper()))
                                            {
                                                System.Net.Mime.ContentType ct = new System.Net.Mime.ContentType(contentType);
                                                contentType = ct.MediaType != null ? ct.MediaType : defaultContentType;
                                            }

                                            var actualBody = body;

                                            while (startPageNumber > 0)
                                            {
                                                isProcessed = false;
                                                body = actualBody;

                                                var urlParams = "";

                                                if (urlParameters.Count > 0)
                                                {
                                                    foreach (var item in urlParameters)
                                                    {
                                                        var value = item.Value;
                                                        if (pageNumberParameterName.ToLower() == item.Key.ToLower())
                                                        {
                                                            value = startPageNumber;
                                                        }
                                                        urlParams += (string.IsNullOrEmpty(urlParams) && !strAPIURL.Contains("?") ? "?" : "&") + item.Key + "=" + value;
                                                    }
                                                }

                                                strAPIURL = strAPIURL + urlParams;
                                                var client = new RestClient();
                                                var request = new RestRequest(strAPIURL, methodForUrl);

                                                if (!string.IsNullOrEmpty(contentType))
                                                {
                                                    request.AddHeader("Content-Type", contentType);
                                                }

                                                if (headerParameters.Count > 0)
                                                {
                                                    foreach (var item in headerParameters)
                                                    {
                                                        request.AddHeader(item.Key, Convert.ToString(item.Value));
                                                    }
                                                }

                                                request.AddHeader("Authorization", Authorization(objCommonIntegrationData.strConnection, intAuthenticationType, dtHeaderData, objCommonIntegrationData.HeaderID, errors));

                                                if (bodyParameters.Count > 0)
                                                {
                                                    request.AlwaysMultipartFormData = true;
                                                    var modifiedBodyParameters = new Dictionary<string, Object>();
                                                    foreach (var item in bodyParameters)
                                                    {
                                                        var value = item.Value;
                                                        if (pageNumberParameterName.ToLower() == item.Key.ToLower())
                                                        {
                                                            value = startPageNumber;
                                                        }
                                                        modifiedBodyParameters.Add(item.Key, value);
                                                    }
                                                    request.AddBody(modifiedBodyParameters);
                                                }

                                                if (stringTokenParameters.Count > 0 && !string.IsNullOrEmpty(body))
                                                {
                                                    foreach (var item in stringTokenParameters)
                                                    {
                                                        var value = item.Value;
                                                        if (pageNumberParameterName.ToLower() == item.Key.ToLower())
                                                        {
                                                            value = startPageNumber;
                                                        }
                                                        body = Regex.Replace(body, $@"@\b{item.Key}\b", Convert.ToString(value));
                                                    }
                                                }

                                                if (!string.IsNullOrEmpty(body))
                                                {
                                                    request.AddStringBody(body, DataFormat.Json);
                                                }

                                                RestResponse response = client.Execute(request);
                                                if (response != null && !string.IsNullOrEmpty(response.Content) && response.ResponseStatus == ResponseStatus.Completed)
                                                {
                                                    SaveRestGetData(response.Content, dt, strTableName, objCommonIntegrationData.strConnection, intUserID, objCommonIntegrationData.HeaderID, Convert.ToInt32(objCommonIntegrationData.strCustomerID), fieldCollectionlist, errors, Convert.ToString(dtHeaderData.Rows[0]["Name"]));

                                                    if (string.IsNullOrEmpty(pageNumberParameterName) || !isProcessed)
                                                    {
                                                        startPageNumber = -1;
                                                        break;
                                                    }

                                                    startPageNumber++;
                                                }
                                                else
                                                {
                                                    var errorMessage = new StringBuilder();
                                                    errorMessage.AppendLine($"Response Status: {response.ResponseStatus.ToString()}");

                                                    if (!string.IsNullOrEmpty(response.Content))
                                                    {
                                                        errorMessage.AppendLine($"Response Content: {response.Content}");
                                                    }

                                                    if (!string.IsNullOrEmpty(response.ErrorMessage))
                                                    {
                                                        errorMessage.AppendLine($"Error Message: {response.ErrorMessage}");
                                                    }

                                                    if (response.ErrorException != null)
                                                    {
                                                        errorMessage.AppendLine($"Error Exception Message: {response.ErrorException.Message}");
                                                    }

                                                    ErrorLog.ErrorCollation(errors, string.Empty, string.Empty, errorMessage.ToString());

                                                    IntegrationTask.InsertRESTPullIntegrationLog(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID, errorMessage.ToString(), "Error occured while getting response.");

                                                    startPageNumber = -1;
                                                    break;
                                                }
                                            }
                                        }
                                        #endregion
                                    }
                                }
                                //if (tableName.ToUpper() == "PEOPLE")
                                //{
                                //IntegrationTask.SaveLastRunDateTime(objCommonIntegrationData.strConnection, IntegrationType.RestGet, Convert.ToString(dtHeaderData.Rows[0]["HeaderID"]));
                                //}
                            }
                            catch (Exception ex)
                            {
                                StringBuilder strerrorMsg = new StringBuilder();
                                strerrorMsg.AppendLine(ex.Message);
                                strerrorMsg.AppendLine(ex.StackTrace);
                                if (ex.InnerException != null)
                                {
                                    strerrorMsg.AppendLine(ex.InnerException.Message);
                                    strerrorMsg.AppendLine(ex.InnerException.StackTrace);
                                }
                                //ExceptionWriter.WriteLogEntry("Rest Pull Response for customer ID " + objCommonIntegrationData.strCustomerID + " " + DateTime.Now.ToString());
                                //ExceptionWriter.WriteLogEntry("Rest Pull Response Error --- " + objCommonIntegrationData.HeaderID + "---" + Convert.ToString(strerrorMsg) + "---- " + DateTime.Now.ToString());

                                IntegrationTask.InsertRESTPullIntegrationLog(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID, strerrorMsg.ToString(), "Error occured while saving data into table");
                                ErrorLog.ErrorCollation(errors, string.Empty, string.Empty, strerrorMsg.ToString());
                            }
                            IntegrationTask.SaveIntegrationTaskStatuswithFailure(objCommonIntegrationData.strConnection,
    Convert.ToString(dtHeaderData.Rows[0]["ID"]),
    DateTime.Now.Date, objCommonIntegrationData.strCustomerID, true, false, "REST Pull Integration");
                            ErrorLog.CreateCsvLogFile(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID, Convert.ToString(dtHeaderData.Rows[0]["Name"]), IntegrationType.RestGet, Convert.ToInt32(objCommonIntegrationData.strCustomerID), intUserID, errors);
                        }
                        //            }
                        //        }
                        //    }
                        //}
                    }

                    if (!string.IsNullOrEmpty(objCommonIntegrationData.strCustomerID))
                    {
                        Console.WriteLine("Process End REST Pull Integration (For CustomerID : " + objCommonIntegrationData.strCustomerID + ") : " + Convert.ToString(dtHeaderData.Rows[0]["Name"]) + " AuthenticationType: " + strAuthenticationTypeName);
                    }
                    else
                    {
                        Console.WriteLine("Process End REST Pull Integration : " + Convert.ToString(dtHeaderData.Rows[0]["Name"]) + " AuthenticationType: " + strAuthenticationTypeName);
                    }
                }
                catch (Exception ex)
                {
                    StringBuilder strerrorMsg = new StringBuilder();
                    strerrorMsg.AppendLine(ex.Message);
                    strerrorMsg.AppendLine(ex.StackTrace);
                    if (ex.InnerException != null)
                    {
                        strerrorMsg.AppendLine(ex.InnerException.Message);
                        strerrorMsg.AppendLine(ex.InnerException.StackTrace);
                    }
                    //ExceptionWriter.WriteLogEntry("Rest Pull Response for customer ID " + objCommonIntegrationData.strCustomerID + " " + DateTime.Now.ToString());
                    //ExceptionWriter.WriteLogEntry("Rest Pull Response Error --- " + objCommonIntegrationData.HeaderID + "---" + Convert.ToString(strerrorMsg) + "---- " + DateTime.Now.ToString());
                    IntegrationTask.SaveIntegrationTaskStatuswithFailure(objCommonIntegrationData.strConnection, Convert.ToString(dtHeaderData.Rows[0]["ID"]), DateTime.Now.Date, objCommonIntegrationData.strCustomerID, true, true, "REST Pull Integration");
                    IntegrationTask.InsertRESTPullIntegrationLog(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID, strerrorMsg.ToString(), "Error occured while saving data into table");

                    ErrorLog.ErrorCollation(errors, string.Empty, string.Empty, strerrorMsg.ToString());
                    ErrorLog.CreateCsvLogFile(objCommonIntegrationData.strConnection, objCommonIntegrationData.HeaderID, Convert.ToString(dtHeaderData.Rows[0]["Name"]), IntegrationType.RestGet, Convert.ToInt32(objCommonIntegrationData.strCustomerID), intUserID, errors);
                }
            }
        }


        private static Dictionary<string, Dictionary<string, object>> GetTemplateTblIdentificationColumns(
string strConnection,
string tableName,
int headerId,
string mappingType)
        {
            var result = new Dictionary<string, Dictionary<string, object>>();
            DataTable dtMapping = new DataTable();
            DataTable dtSchema = new DataTable();

            try
            {
                // STEP 1: Get mapping info from RestPullIntegrationFrameworkDetail
                // This maps API response fields to DB table fields
                string strMappingQuery = @"
            SELECT 
                d.RestPullResponseField AS FileFieldName,
                d.TableFieldName AS HeaderTableFieldName,
                d.DetailTableFieldName,
                d.DetailTableName,
                d.HeaderTableFieldDefaultValue,
                d.DetailTableFieldDefaultValue
            FROM RestPullIntegrationDetail d
            INNER JOIN RestPullIntegrationHeader h 
                ON d.HeaderID = h.ID
            WHERE d.HeaderID = @HeaderId;";

                using (SqlConnection connection = new SqlConnection(strConnection))
                using (SqlCommand sqlcmd = new SqlCommand(strMappingQuery, connection))
                {
                    sqlcmd.Parameters.AddWithValue("@HeaderId", headerId);
                    using (SqlDataAdapter a = new SqlDataAdapter(sqlcmd))
                    {
                        a.Fill(dtMapping);
                    }
                }

                // STEP 2: Get table schema for validation
                string strSchemaQuery = @"
            SELECT 
                c.COLUMN_NAME AS FieldName,
                c.DATA_TYPE AS FieldType,
                c.CHARACTER_MAXIMUM_LENGTH AS [Length],
                c.TABLE_NAME AS Entity,
                CASE c.IS_NULLABLE 
                    WHEN 'NO' THEN CAST(1 AS bit)
                    ELSE CAST(0 AS bit)
                END AS Mandatory
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = 'dbo'
              AND c.TABLE_NAME = @TableName;";

                using (SqlConnection connection = new SqlConnection(strConnection))
                using (SqlCommand sqlcmd = new SqlCommand(strSchemaQuery, connection))
                {
                    sqlcmd.Parameters.AddWithValue("@TableName", tableName);
                    using (SqlDataAdapter a = new SqlDataAdapter(sqlcmd))
                    {
                        a.Fill(dtSchema);
                    }
                }

                // STEP 3: Build schema dictionary for quick lookup
                var schemaDict = dtSchema.AsEnumerable().ToDictionary(
                    r => r["FieldName"].ToString(),
                    r => new
                    {
                        FieldType = r["FieldType"].ToString(),
                        Entity = r["Entity"].ToString(),
                        Length = r["Length"] == DBNull.Value ? null : (int?)Convert.ToInt32(r["Length"]),
                        Mandatory = Convert.ToBoolean(r["Mandatory"])
                    }
                );

                // STEP 4: Combine Mapping + Schema info
                foreach (DataRow row in dtMapping.Rows)
                {
                    string fileField = row["FileFieldName"].ToString();

                    string dbField = mappingType.Equals("Header", StringComparison.OrdinalIgnoreCase)
                        ? row["HeaderTableFieldName"]?.ToString()
                        : row["DetailTableFieldName"]?.ToString();

                    if (string.IsNullOrWhiteSpace(dbField))
                        continue;

                    if (schemaDict.TryGetValue(dbField, out var schema))
                    {
                        result[fileField] = new Dictionary<string, object>
                {
                    { "FieldName", dbField },
                    { "FieldType", schema.FieldType },
                    { "Entity", schema.Entity },
                    { "Length", schema.Length ?? 0 },
                    { "Mandatory", schema.Mandatory },
                    { "MappingType", mappingType },
                    { "DefaultValue", mappingType.Equals("Header", StringComparison.OrdinalIgnoreCase)
                        ? row["HeaderTableFieldDefaultValue"]?.ToString() ?? ""
                        : row["DetailTableFieldDefaultValue"]?.ToString() ?? "" },
                    { "DetailTableName", row["DetailTableName"]?.ToString() ?? "" }
                };
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while building REST Pull field mappings: " + ex.Message);
                throw;
            }

            return result;
        }


        private static DataTable ConvertDictionaryToDataTable(Dictionary<string, Dictionary<string, object>> dict)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("FileFieldName");
            dt.Columns.Add("FieldName");
            dt.Columns.Add("FieldType");
            dt.Columns.Add("Entity");
            dt.Columns.Add("Length", typeof(int));
            dt.Columns.Add("Mandatory", typeof(bool));
            dt.Columns.Add("MappingType");
            dt.Columns.Add("DefaultValue");
            dt.Columns.Add("DetailTableName");

            foreach (var kvp in dict)
            {
                var row = dt.NewRow();
                row["FileFieldName"] = kvp.Key;
                row["FieldName"] = kvp.Value["FieldName"];
                row["FieldType"] = kvp.Value["FieldType"];
                row["Entity"] = kvp.Value["Entity"];
                row["Length"] = kvp.Value["Length"];
                row["Mandatory"] = kvp.Value["Mandatory"];
                row["MappingType"] = kvp.Value["MappingType"];
                row["DefaultValue"] = kvp.Value["DefaultValue"];
                row["DetailTableName"] = kvp.Value["DetailTableName"];
                dt.Rows.Add(row);
            }

            return dt;

        }
        public static List<Tuple<string, string>> SaveRESTPullIntegrationData(
      dynamic input,
      DataTable dtColumnCollection,
      string strTableName,
      string strConnection,
      int UserID,
      int HeaderID,
      int customerID,
      List<FieldCollections> fieldCollectionlist,
      List<ErrorLog> errors)
        {
            var result = new List<Tuple<string, string>>();

            try
            {
                JArray jsonArray = input is string ? JArray.Parse(input) :
                                   input is JArray ? input : new JArray(input);

                using (SqlConnection conn = new SqlConnection(strConnection))
                {
                    conn.Open();

                    foreach (var record in jsonArray)
                    {
                        JObject jObj = (JObject)record;

                       
                        var headerRows = dtColumnCollection.AsEnumerable()
                            .Where(r => Convert.ToString(r["MappingType"]) == "Header")
                            .ToList();

                        List<string> headerCols = new List<string>();
                        List<string> headerVals = new List<string>();
                        List<SqlParameter> headerParams = new List<SqlParameter>();

                        foreach (var row in headerRows)
                        {
                            string respField = Convert.ToString(row["ResponseField"]);
                            string tableField = Convert.ToString(row["EntityFieldName"]);

                            if (string.IsNullOrEmpty(tableField))
                                continue;

                            JToken token = jObj.SelectToken(respField);
                            object value = token != null ? (object)token.ToString() : DBNull.Value;

                            headerCols.Add($"[{tableField}]");
                            headerVals.Add($"@{tableField}");
                            headerParams.Add(new SqlParameter($"@{tableField}", value));
                        }

                        headerCols.Add("[CreatedBy]");
                        headerCols.Add("[CreatedDate]");
                        headerVals.Add("@CreatedBy");
                        headerVals.Add("@CreatedDate");
                        headerParams.Add(new SqlParameter("@CreatedBy", UserID));
                        headerParams.Add(new SqlParameter("@CreatedDate", DateTime.Now));

                        string headerInsert = $@"
                INSERT INTO [{strTableName}] ({string.Join(",", headerCols)})
                VALUES ({string.Join(",", headerVals)});
                SELECT SCOPE_IDENTITY();";

                        int parentId = 0;
                        using (SqlCommand cmd = new SqlCommand(headerInsert, conn))
                        {
                            cmd.Parameters.AddRange(headerParams.ToArray());
                            object newId = cmd.ExecuteScalar();
                            parentId = newId != null ? Convert.ToInt32(newId) : 0;
                        }

                        result.Add(Tuple.Create("Success", $"Inserted into {strTableName}"));

                        // -------------------------------------------
                        // ✅ DETAIL TABLE INSERT
                        // -------------------------------------------
                        var detailRows = dtColumnCollection.AsEnumerable()
                            .Where(r => Convert.ToString(r["MappingType"]) == "Detail")
                            .ToList();

                        if (detailRows.Any())
                        {
                            foreach (var detailRow in detailRows)
                            {
                                string respField = Convert.ToString(detailRow["ResponseField"]);
                                string detailFieldName = Convert.ToString(detailRow["DetailEntityFieldName"]);
                                
                                if (string.IsNullOrEmpty(detailFieldName))
                                    continue;

                                JToken token = jObj.SelectToken(respField);
                                if (token == null)
                                    continue;

                                // For detail records, we need to get the detail table name from the entity
                                // This would typically be determined by the DetailTableId field
                                int detailTableId = Convert.ToInt32(detailRow["DetailTableId"]);
                                
                                // You would need to implement logic to get the actual table name from DetailTableId
                                // For now, using a placeholder - this needs to be implemented based on your entity structure
                                string detailTableName = GetDetailTableName(strConnection, detailTableId);
                                
                                if (string.IsNullOrEmpty(detailTableName))
                                    continue;

                                List<string> detCols = new List<string> { $"[{detailFieldName}]", "[HeaderID]", "[CreatedBy]", "[CreatedDate]" };
                                List<string> detVals = new List<string> { $"@{detailFieldName}", "@HeaderID", "@CreatedBy", "@CreatedDate" };
                                List<SqlParameter> detParams = new List<SqlParameter>
                                {
                                    new SqlParameter($"@{detailFieldName}", token.ToString()),
                                    new SqlParameter("@HeaderID", parentId),
                                    new SqlParameter("@CreatedBy", UserID),
                                    new SqlParameter("@CreatedDate", DateTime.Now)
                                };

                                string detailInsert = $@"
                        INSERT INTO [{detailTableName}] ({string.Join(",", detCols)})
                        VALUES ({string.Join(",", detVals)})";

                                using (SqlCommand cmd = new SqlCommand(detailInsert, conn))
                                {
                                    cmd.Parameters.AddRange(detParams.ToArray());
                                    cmd.ExecuteNonQuery();
                                }

                                result.Add(Tuple.Create("Success", $"Inserted into Detail Table: {detailTableName}"));
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result.Add(new Tuple<string, string>("Error", ex.ToString()));
            }

            return result;
        }


        //  public static List<Tuple<string, string>> SaveRESTPullIntegrationData(
        //dynamic input,
        //DataTable dtColumnCollection,
        //string strTableName,
        //string strConnection,
        //int UserID,
        //int HeaderID,
        //int customerID,
        //List<FieldCollections> fieldCollectionlist,
        //List<ErrorLog> errors)
        //  {
        //      var result = new List<Tuple<string, string>>();

        //      try
        //      {
        //          // 1️⃣ Convert input to JArray
        //          JArray jsonArray;
        //          if (input is string)
        //              jsonArray = JArray.Parse(input);
        //          else if (input is JArray)
        //              jsonArray = input;
        //          else if (input is DataTable dt)
        //              jsonArray = JArray.FromObject(dt); // Handle DataTable input
        //          else
        //              jsonArray = new JArray(input);

        //          using (SqlConnection conn = new SqlConnection(strConnection))
        //          {
        //              conn.Open();

        //              foreach (var record in jsonArray)
        //              {
        //                  JObject jObj = record as JObject;
        //                  if (jObj == null) continue;

        //                  // ========= HEADER TABLE INSERT =========
        //                  var headerRows = dtColumnCollection.AsEnumerable()
        //                      .Where(r => Convert.ToString(r["MappingType"]).Equals("Header", StringComparison.OrdinalIgnoreCase))
        //                      .ToList();

        //                  List<string> headerCols = new List<string>();
        //                  List<string> headerVals = new List<string>();
        //                  List<SqlParameter> headerParams = new List<SqlParameter>();

        //                  foreach (var mapRow in headerRows)
        //                  {
        //                      string responseField = Convert.ToString(mapRow["FileFieldName"]); // JSON key
        //                      string tableField = Convert.ToString(mapRow["FieldName"]);        // DB column

        //                      JToken valueToken = jObj.SelectToken(responseField);
        //                      object value = valueToken != null ? (object)valueToken.ToString() : DBNull.Value;

        //                      headerCols.Add($"[{tableField}]");
        //                      headerVals.Add($"@{tableField}");
        //                      headerParams.Add(new SqlParameter($"@{tableField}", value ?? DBNull.Value));
        //                  }

        //                  // Add audit fields
        //                  headerCols.Add("[CreatedBy]");
        //                  headerCols.Add("[CreatedDate]");
        //                  headerVals.Add("@CreatedBy");
        //                  headerVals.Add("@CreatedDate");
        //                  headerParams.Add(new SqlParameter("@CreatedBy", UserID));
        //                  headerParams.Add(new SqlParameter("@CreatedDate", DateTime.Now));

        //                  string headerInsert = $@"
        //                  INSERT INTO {strTableName} ({string.Join(",", headerCols)})
        //                  VALUES ({string.Join(",", headerVals)});
        //                  SELECT SCOPE_IDENTITY();";

        //                  int newHeaderId = 0;
        //                  using (SqlCommand cmd = new SqlCommand(headerInsert, conn))
        //                  {
        //                      cmd.Parameters.AddRange(headerParams.ToArray());
        //                      object id = cmd.ExecuteScalar();
        //                      if (id != null && id != DBNull.Value)
        //                          newHeaderId = Convert.ToInt32(id);
        //                  }

        //                  result.Add(new Tuple<string, string>("Success", $"Inserted into {strTableName}"));

        //                  // ========= DETAIL TABLE INSERT =========
        //                  var detailTables = dtColumnCollection.AsEnumerable()
        //                      .Where(r => Convert.ToString(r["MappingType"]).Equals("Detail", StringComparison.OrdinalIgnoreCase))
        //                      .Select(r => Convert.ToString(r["DetailTableName"]))
        //                      .Where(t => !string.IsNullOrEmpty(t))
        //                      .Distinct()
        //                      .ToList();

        //                  foreach (var detailTable in detailTables)
        //                  {
        //                      var detailRows = dtColumnCollection.AsEnumerable()
        //                          .Where(r => Convert.ToString(r["DetailTableName"]) == detailTable)
        //                          .ToList();

        //                      // Check if this JSON contains an array for this detail table
        //                      JToken detailToken = jObj.SelectToken(detailTable);
        //                      JArray detailArray;
        //                      if (detailToken == null)
        //                      {
        //                          // Single object fallback
        //                          detailArray = new JArray(jObj);
        //                      }
        //                      else if (detailToken.Type == JTokenType.Array)
        //                          detailArray = (JArray)detailToken;
        //                      else
        //                          detailArray = new JArray(detailToken);

        //                      foreach (var detailRecord in detailArray)
        //                      {
        //                          JObject detailObj = detailRecord as JObject;
        //                          if (detailObj == null) continue;

        //                          List<string> detailCols = new List<string>();
        //                          List<string> detailVals = new List<string>();
        //                          List<SqlParameter> detailParams = new List<SqlParameter>();

        //                          foreach (var mapRow in detailRows)
        //                          {
        //                              string responseField = Convert.ToString(mapRow["RestPullResponseField"]);
        //                              string detailField = Convert.ToString(mapRow["FieldName"]);

        //                              JToken valueToken = detailObj.SelectToken(responseField);
        //                              object value = valueToken != null ? (object)valueToken.ToString() : DBNull.Value;

        //                              detailCols.Add($"[{detailField}]");
        //                              detailVals.Add($"@{detailField}");
        //                              detailParams.Add(new SqlParameter($"@{detailField}", value ?? DBNull.Value));
        //                          }

        //                          // Add FK to header
        //                          detailCols.Add("HeaderID");
        //                          detailVals.Add("@HeaderID");
        //                          detailParams.Add(new SqlParameter("@HeaderID", newHeaderId));

        //                          // Add audit fields
        //                          detailCols.Add("CreatedBy");
        //                          detailCols.Add("CreatedDate");
        //                          detailVals.Add("@CreatedBy");
        //                          detailVals.Add("@CreatedDate");
        //                          detailParams.Add(new SqlParameter("@CreatedBy", UserID));
        //                          detailParams.Add(new SqlParameter("@CreatedDate", DateTime.Now));

        //                          string detailInsert = $@"
        //                          INSERT INTO {detailTable} ({string.Join(",", detailCols)})
        //                          VALUES ({string.Join(",", detailVals)})";

        //                          using (SqlCommand cmd = new SqlCommand(detailInsert, conn))
        //                          {
        //                              cmd.Parameters.AddRange(detailParams.ToArray());
        //                              cmd.ExecuteNonQuery();
        //                          }

        //                          result.Add(new Tuple<string, string>("Success", $"Inserted into {detailTable}"));
        //                      }
        //                  }
        //              }
        //          }
        //      }
        //      catch (Exception ex)
        //      {
        //          result.Add(new Tuple<string, string>("Error", ex.Message));
        //          // Optional: add ex.StackTrace or logging
        //      }

        //      return result;
        //  }

        // Overload to accept DataTable directly
        public static List<Tuple<string, string>> SaveRESTPullIntegrationData(
            DataTable dtInput,
            DataTable dtColumnCollection,
            string strTableName,
            string strConnection,
            int UserID,
            int HeaderID,
            int customerID,
            List<FieldCollections> fieldCollectionlist,
            List<ErrorLog> errors)
        {
            // Convert DataTable to JArray and call main method
            JArray jsonArray = JArray.FromObject(dtInput);
            return SaveRESTPullIntegrationData(jsonArray, dtColumnCollection, strTableName, strConnection,
                                               UserID, HeaderID, customerID, fieldCollectionlist, errors);
        }



        public static async Task<string> CallGraphQLAsync(string strURL, string strGraphQLQuery)
        {
            try
            {
                //string responseObj = string.Empty;
                //var graphQLClient = new GraphQLHttpClient(strURL, new NewtonsoftJsonSerializer()); 

                //var personAndFilmsRequest = new GraphQLRequest
                //{
                //    Query = strGraphQLQuery,
                //    Variables = new
                //    {
                //        id = "6fe6eeae32f548c1b3473135ca2eed5d"
                //    }
                //};
                //ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

                //var graphQLResponse = await graphQLClient.SendQueryAsync<object>(personAndFilmsRequest);

                var graphQLOptions = new GraphQLHttpClientOptions
                {
                    EndPoint = new Uri(strURL, UriKind.Absolute),
                };
                var graphQLClient = new GraphQLHttpClient(graphQLOptions, new GraphQL.Client.Serializer.Newtonsoft.NewtonsoftJsonSerializer());

                var msg = new GraphQLRequest
                {
                    Query = strGraphQLQuery,
                };
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

                //graphQLClient.HttpClient.Timeout = new TimeSpan(0, 0, 1000);

                var graphQLResponse = await graphQLClient.SendQueryAsync<dynamic>(msg).ConfigureAwait(false);
                return Convert.ToString(graphQLResponse.Data);
            }
            catch (Exception ex) { throw ex; }
        }

        #region OAUTH 
        public static async Task<string> GetAuthorizeToken(string strGetURL, string strAccessTokenURL, string strClientId, string strScope, string strGrantType, string strClientSecret, string strClientAuthentication, string strTokenName)
        {
            // Initialization.  
            string responseObj = string.Empty;

            // Posting.  
            using (var client = new HttpClient())
            {
                // Setting Base address.  
                //client.BaseAddress = new Uri("https://apis-sandbox.intel.com/v1/auth/token");
                client.BaseAddress = new Uri(strAccessTokenURL);

                // Setting content type. 
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                //var byteArray = Encoding.ASCII.GetBytes("grant_type:client_credentials");
                //client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

                // Initialization.  
                HttpResponseMessage response = new HttpResponseMessage();

                List<KeyValuePair<string, string>> allIputParams = new List<KeyValuePair<string, string>>()
                {
                     new KeyValuePair<string, string>("client_id", strClientId),
                                new KeyValuePair<string, string>("scope", strScope),
                                new KeyValuePair<string, string>("grant_type", strGrantType),
                                new KeyValuePair<string, string>("client_secret", strClientSecret),
                                new KeyValuePair<string, string>("client_authentication", strClientAuthentication),
                                new KeyValuePair<string, string>("token_name", strTokenName)


                     //new KeyValuePair<string, string>("client_id", "ee32466b-f33a-46c0-aef2-1d2c46ea1286"),
                     //           new KeyValuePair<string, string>("scope", "api://71c9ae16-9d10-45b7-9c1d-30925311dabf/.default"),
                     //           new KeyValuePair<string, string>("grant_type", "client_credentials"),
                     //           new KeyValuePair<string, string>("client_secret", "VJS7Q~Li8OTB7HiL6Im7sPhMJhf8RsQ_EvEaY"),
                     //           new KeyValuePair<string, string>("client_authentication", "Send client credentials in body"),
                     //           new KeyValuePair<string, string>("token_name", "ITSM Federal Asset Token")
                };

                // Convert Request Params to Key Value Pair.  

                // URL Request parameters.  
                HttpContent requestParams = new FormUrlEncodedContent(allIputParams);
                //requestParams.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

                // HTTP POST  
                //response = await client.PostAsync("https://apis.intel.com/v1/auth/token", requestParams).ConfigureAwait(false);
                response = await client.PostAsync(strAccessTokenURL, requestParams).ConfigureAwait(false);

                // Verification  
                if (response.IsSuccessStatusCode)
                {
                    // Reading Response.                      
                    string returnedData = response.Content.ReadAsStringAsync().Result;
                    responseObj = GetInfo(returnedData, strGetURL).Result;
                }
                else
                {
                    //var loginError = JsonConvert.DeserializeObject<LoginResponse>(response.Content.ReadAsStringAsync().Result);
                    //throw (new LoginException(loginError.ResponseStatus.Message + " Please check the username or password."));
                }
            }

            return responseObj;
        }

        public static async Task<string> GetInfo(string authorizeToken, string strGetURL)
        {
            // Initialization.  
            string responseObj = string.Empty;

            // HTTP GET.  
            using (var client = new HttpClient())
            {
                // Initialization
                dynamic obj = Newtonsoft.Json.Linq.JObject.Parse(authorizeToken);
                string authorization = obj.access_token;

                // Setting Authorization.  
                client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", authorization);

                // Setting Base address.  
                //client.BaseAddress = new Uri("https://apis.intel.com/itsm/api/now/table/u_federal_asset_external?sysparm_limit=1");
                client.BaseAddress = new Uri(strGetURL);

                // Setting content type.  
                client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                client.Timeout = TimeSpan.FromMinutes(30);
                // Initialization.  
                HttpResponseMessage response = new HttpResponseMessage();

                // HTTP GET  
                response = await client.GetAsync(strGetURL).ConfigureAwait(false);

                // Verification  
                if (response.IsSuccessStatusCode)
                {
                    // Reading Response.  
                    responseObj = response.Content.ReadAsStringAsync().Result;
                }
            }

            return responseObj;
        }
        #endregion
        public static string Decrypt(string strConnection, string cipherText, string Password)
        {

            string strValue = string.Empty;


            using (SqlConnection connection = new SqlConnection(strConnection))
            {
                SqlCommand sqlcmd = new SqlCommand("SELECT TOP 1 Value FROM AppSetting WHERE Name = 'FIPS COMPLIANCE'", connection);
                // set the destination table name

                connection.Open();

                sqlcmd.CommandType = CommandType.Text;

                object objId = sqlcmd.ExecuteScalar();

                if (objId != null)
                {
                    strValue = Convert.ToString(objId);
                }
                connection.Close();
            }

            if (strValue.ToUpper() == "ON")
            {
                byte[] cipherbytes = Convert.FromBase64String(cipherText);

                PasswordDeriveBytes mdf = new PasswordDeriveBytes(Password,
                new byte[] {0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65,
            0x64, 0x76, 0x65, 0x64, 0x65, 0x76});

                string plainText = DecryptStringFromBytes_Aes(cipherbytes, mdf.GetBytes(32), mdf.GetBytes(16));

                return plainText;
            }
            else
            {
                byte[] cipherBytes = Convert.FromBase64String(cipherText);

                PasswordDeriveBytes pdb = new PasswordDeriveBytes(Password,
                    new byte[] {0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65,
            0x64, 0x76, 0x65, 0x64, 0x65, 0x76});

                byte[] decryptedData = Decrypt(cipherBytes,
                    pdb.GetBytes(32), pdb.GetBytes(16));

                return System.Text.Encoding.Unicode.GetString(decryptedData);
            }

        }
        public static string DecryptStringFromBytes_Aes(byte[] cipherText, byte[] Key, byte[] IV)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length <= 0)
                throw new ArgumentNullException("cipherText");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("IV");

            // Declare the string used to hold
            // the decrypted text.
            string plaintext = null;


            // Create an AesCryptoServiceProvider object
            // with the specified key and IV.
            using (AesCryptoServiceProvider aesAlg = new AesCryptoServiceProvider())
            {
                aesAlg.Key = Key;
                aesAlg.IV = IV;
                //aesAlg.Padding = PaddingMode.PKCS7;
                // Create a decrytor to perform the stream transform.
                ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(cipherText))
                {
                    using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader srDecrypt = new StreamReader(csDecrypt))
                        {

                            // Read the decrypted bytes from the decrypting stream
                            // and place them in a string.

                            plaintext = srDecrypt.ReadToEnd();
                        }
                    }
                }

            }

            return plaintext;

        }
        private static byte[] Decrypt(byte[] cipherData, byte[] Key, byte[] IV)
        {
            MemoryStream ms = new MemoryStream();

            Rijndael alg = Rijndael.Create();

            alg.Key = Key;
            alg.IV = IV;

            CryptoStream cs = new CryptoStream(ms,
                alg.CreateDecryptor(), CryptoStreamMode.Write);

            cs.Write(cipherData, 0, cipherData.Length);

            cs.Close();

            byte[] decryptedData = ms.ToArray();

            return decryptedData;
        }
        public static async Task<T> GetLoginAPISingle<T>(
       string requestUrl,
       bool isURLParams,
       DataTable objLogin,
       AuthenticationType objAuthenticationType)
        {
            using (var handler = new HttpClientHandler())
            {
                // Accept all SSL certificates
                handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;

                using (var client = new HttpClient(handler))
                {
                    client.Timeout = TimeSpan.FromMinutes(5);
                    client.DefaultRequestHeaders.CacheControl = new System.Net.Http.Headers.CacheControlHeaderValue
                    {
                        NoCache = true
                    };

                    // Convert DataTable to single JSON object (first row)
                    string jsonData = "{}";
                    if (objLogin != null && objLogin.Rows.Count > 0)
                    {
                        var firstRow = objLogin.Rows[0];
                        var dict = objLogin.Columns
                            .Cast<DataColumn>()
                            .ToDictionary(col => col.ColumnName, col => firstRow[col]?.ToString() ?? "");
                        jsonData = JsonConvert.SerializeObject(dict);
                    }

                    // Debug: Log the request data
                    System.Diagnostics.Debug.WriteLine($"=== API Request ===");
                    System.Diagnostics.Debug.WriteLine($"URL: {requestUrl}");
                    System.Diagnostics.Debug.WriteLine($"JSON Data: {jsonData}");
                    System.Diagnostics.Debug.WriteLine($"Auth Type: {objAuthenticationType}");
                    System.Diagnostics.Debug.WriteLine($"Is URL Params: {isURLParams}");

                    T apiResponse = default(T);

                    try
                    {
                        if (objAuthenticationType == AuthenticationType.UserNamePassword && isURLParams)
                        {
                            System.Diagnostics.Debug.WriteLine("Making GET request...");

                            HttpResponseMessage response = await client.GetAsync(requestUrl);

                            // Debug: Log response status
                            System.Diagnostics.Debug.WriteLine($"Response Status: {response.StatusCode}");
                            System.Diagnostics.Debug.WriteLine($"Content-Type: {response.Content.Headers.ContentType?.MediaType}");

                            // Get raw response first
                            string responseContent = await response.Content.ReadAsStringAsync();

                            // Debug: Log raw response
                            System.Diagnostics.Debug.WriteLine($"Raw Response (first 500 chars): {responseContent.Substring(0, Math.Min(500, responseContent.Length))}");

                            // Check if response is successful
                            if (!response.IsSuccessStatusCode)
                            {
                                throw new Exception($"API returned error status {response.StatusCode}: {responseContent}");
                            }

                            // Check content type
                            var contentType = response.Content.Headers.ContentType?.MediaType;
                            if (contentType != null && !contentType.Contains("json"))
                            {
                                throw new Exception($"Expected JSON response but got '{contentType}'. Response: {responseContent}");
                            }

                            // Validate JSON before deserializing
                            if (string.IsNullOrWhiteSpace(responseContent))
                            {
                                throw new Exception("API returned empty response");
                            }

                            if (!responseContent.TrimStart().StartsWith("{") && !responseContent.TrimStart().StartsWith("["))
                            {
                                throw new Exception($"Response doesn't appear to be JSON. First character: '{responseContent.TrimStart()[0]}'. Response: {responseContent}");
                            }

                            apiResponse = JsonConvert.DeserializeObject<T>(responseContent);
                            System.Diagnostics.Debug.WriteLine("Successfully deserialized response");

                            return apiResponse;
                        }
                        else
                        {
                            /* System.Diagnostics.Debug.WriteLine("Making POST request...");
                             System.Diagnostics.Debug.WriteLine("Creating request content...");

                             // POST request with proper JSON object
                             var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                             System.Diagnostics.Debug.WriteLine($"Sending POST to: {requestUrl}");
                             System.Diagnostics.Debug.WriteLine($"Request body: {jsonData}");
                             System.Diagnostics.Debug.WriteLine("Waiting for response...");

                             HttpResponseMessage postResponse = null;
                             try
                             {
                                 postResponse = await client.PostAsync(requestUrl, content);
                                 System.Diagnostics.Debug.WriteLine($"Response received! Status: {postResponse.StatusCode}");
                             }
                             catch (TaskCanceledException ex)
                             {
                                 System.Diagnostics.Debug.WriteLine("Request TIMEOUT!");
                                 throw new Exception($"Request timeout after 5 minutes. URL: {requestUrl}", ex);
                             }
                             catch (HttpRequestException ex)
                             {
                                 System.Diagnostics.Debug.WriteLine($"HTTP Request failed: {ex.Message}");
                                 throw new Exception($"HTTP Request failed: {ex.Message}. URL: {requestUrl}", ex);
                             }

                             // Debug: Log response status
                             System.Diagnostics.Debug.WriteLine($"Response Status: {postResponse.StatusCode}");
                             System.Diagnostics.Debug.WriteLine($"Content-Type: {postResponse.Content.Headers.ContentType?.MediaType}");
                             System.Diagnostics.Debug.WriteLine("Reading response content...");

                             // Get raw response first
                             string postResponseContent = await postResponse.Content.ReadAsStringAsync();
                             System.Diagnostics.Debug.WriteLine($"Content length: {postResponseContent.Length} characters");

                             // Debug: Log raw response
                             System.Diagnostics.Debug.WriteLine($"Raw Response (first 500 chars): {postResponseContent.Substring(0, Math.Min(500, postResponseContent.Length))}");

                             // Check if response is successful
                             if (!postResponse.IsSuccessStatusCode)
                             {
                                 throw new Exception($"API returned error status {postResponse.StatusCode}: {postResponseContent}");
                             }

                             // Check content type
                             var contentType = postResponse.Content.Headers.ContentType?.MediaType;
                             if (contentType != null && !contentType.Contains("json"))
                             {
                                 throw new Exception($"Expected JSON response but got '{contentType}'. Response: {postResponseContent}");
                             }

                             // Validate JSON before deserializing
                             if (string.IsNullOrWhiteSpace(postResponseContent))
                             {
                                 throw new Exception("API returned empty response");
                             }

                             if (!postResponseContent.TrimStart().StartsWith("{") && !postResponseContent.TrimStart().StartsWith("["))
                             {
                                 throw new Exception($"Response doesn't appear to be JSON. First character: '{postResponseContent.TrimStart()[0]}'. Response: {postResponseContent}");
                             }

                             apiResponse = JsonConvert.DeserializeObject<T>(postResponseContent);
                             System.Diagnostics.Debug.WriteLine("Successfully deserialized response");

                             return apiResponse*/
                            ;
                            System.Diagnostics.Debug.WriteLine("Making GET request...");

                            HttpResponseMessage response = await client.GetAsync(requestUrl);

                            // Debug: Log response status
                            System.Diagnostics.Debug.WriteLine($"Response Status: {response.StatusCode}");
                            System.Diagnostics.Debug.WriteLine($"Content-Type: {response.Content.Headers.ContentType?.MediaType}");

                            // Get raw response first
                            string responseContent = await response.Content.ReadAsStringAsync();

                            // Debug: Log raw response
                            System.Diagnostics.Debug.WriteLine($"Raw Response (first 500 chars): {responseContent.Substring(0, Math.Min(500, responseContent.Length))}");

                            // Check if response is successful
                            if (!response.IsSuccessStatusCode)
                            {
                                throw new Exception($"API returned error status {response.StatusCode}: {responseContent}");
                            }

                            // Check content type
                            var contentType = response.Content.Headers.ContentType?.MediaType;
                            if (contentType != null && !contentType.Contains("json"))
                            {
                                throw new Exception($"Expected JSON response but got '{contentType}'. Response: {responseContent}");
                            }

                            // Validate JSON before deserializing
                            if (string.IsNullOrWhiteSpace(responseContent))
                            {
                                throw new Exception("API returned empty response");
                            }

                            if (!responseContent.TrimStart().StartsWith("{") && !responseContent.TrimStart().StartsWith("["))
                            {
                                throw new Exception($"Response doesn't appear to be JSON. First character: '{responseContent.TrimStart()[0]}'. Response: {responseContent}");
                            }

                            apiResponse = JsonConvert.DeserializeObject<T>(responseContent);
                            System.Diagnostics.Debug.WriteLine("Successfully deserialized response");

                            return apiResponse;
                        }
                    }
                    catch (HttpRequestException ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"HTTP Error: {ex.Message}");
                        throw new Exception($"HTTP Error: {ex.Message}. URL: {requestUrl}", ex);
                    }
                    catch (JsonException ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"JSON Parsing Error: {ex.Message}");
                        throw new Exception($"JSON Parsing Error: {ex.Message}. Check debug output for raw response.", ex);
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Unexpected Error: {ex.Message}");
                        throw new Exception($"Unexpected Error: {ex.Message}", ex);
                    }
                }
            }
        }



        private static string GetDetailTableName(string strConnection, int detailTableId)
        {
            if (detailTableId <= 0)
                return string.Empty;

            try
            {
                using (SqlConnection conn = new SqlConnection(strConnection))
                {
                    string query = "SELECT Name FROM TableDefination WHERE ID = @DetailTableId";
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@DetailTableId", detailTableId);
                        conn.Open();
                        object result = cmd.ExecuteScalar();
                        return result?.ToString() ?? string.Empty;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting detail table name: {ex.Message}");
                return string.Empty;
            }
        }

        #endregion

        #region Save Rest Get Integration

        static string firstFieldName = "";

        static bool isProcessed = false;

        private static void SaveRestGetData(string SingleJsonObject, DataTable dtColumnCollection, string strTableName, string strConnection, int UserID, int HeaderID, int customerID, List<FieldCollections> fieldCollectionlist, List<ErrorLog> errors, string integrationName)
        {
            try
            {

                //var jsonToken = JToken.Parse(SingleJsonObject);

              

                var jsonToken = JToken.Parse(SingleJsonObject);
              

                string strQuerywhere = string.Empty;
                List<Tuple<string, object>> fieldNames = new List<Tuple<string, object>>();

                if (jsonToken.Type == JTokenType.Object)
                {
                    var jsonObject = JObject.Parse(SingleJsonObject);

                    foreach (var property in jsonObject.Properties())
                    {
                        if (property.Value.Type == JTokenType.Object)
                        {
                            fieldNames = GetValuesFromJsonRecursive(property.Value as JObject, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                        }
                        else if (property.Value.Type == JTokenType.Array)
                        {
                            fieldNames = GetValuesFromJsonRecursive(property.Value as JArray, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                        }
                        else
                        {
                            //Console.WriteLine($"Key: {property.Name}, Value: {property.Value}");
                            if (dtColumnCollection != null && dtColumnCollection.Rows.Count > 0)
                            {
                                DataRow[] dr = dtColumnCollection.Select("RestPullResponseField='" + property.Name + "'");
                                if (dr != null && dr.Length > 0)
                                {
                                    if (string.IsNullOrEmpty(firstFieldName))
                                    {
                                        firstFieldName = property.Name;
                                    }
                                    else
                                    {
                                        if (property.Name == firstFieldName)
                                        {
                                            SaveRestGetDataInner(fieldNames, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName);
                                            fieldNames.Clear();
                                        }
                                    }

                                    fieldNames.Add(new Tuple<string, object>(property.Name, property.Value));
                                }
                            }
                        }
                    }
                }
                else if (jsonToken.Type == JTokenType.Array)
                {
                    var jsonArray = JArray.Parse(SingleJsonObject);
                    foreach (var item in jsonArray)
                    {
                        Newtonsoft.Json.Linq.JObject jsonObject = Newtonsoft.Json.Linq.JObject.FromObject(item);

                        foreach (var property in jsonObject.Properties())
                        {
                            if (property.Value.Type == JTokenType.Object)
                            {
                                fieldNames = GetValuesFromJsonRecursive(property.Value as JObject, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                            }
                            else if (property.Value.Type == JTokenType.Array)
                            {
                                fieldNames = GetValuesFromJsonRecursive(property.Value as JArray, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                            }
                            else
                            {
                                //Console.WriteLine($"Key: {property.Name}, Value: {property.Value}");
                                if (dtColumnCollection != null && dtColumnCollection.Rows.Count > 0)
                                {
                                    DataRow[] dr = dtColumnCollection.Select("RestPullResponseField='" + property.Name + "'");
                                    if (dr != null && dr.Length > 0)
                                    {
                                        if (string.IsNullOrEmpty(firstFieldName))
                                        {
                                            firstFieldName = property.Name;
                                        }
                                        else
                                        {
                                            if (property.Name == firstFieldName)
                                            {
                                                SaveRestGetDataInner(fieldNames, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName);
                                                fieldNames.Clear();
                                            }
                                        }

                                    }
                                    fieldNames.Add(new Tuple<string, object>(property.Name, property.Value));

                                }
                            }
                        }

                        if (fieldNames.Count > 0)
                        {
                            SaveRestGetDataInner(fieldNames, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName);
                            fieldNames.Clear();
                        }
                    }
                }


                if (fieldNames.Count > 0)
                {
                    SaveRestGetDataInner(fieldNames, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName);
                    fieldNames.Clear();
                }
            }
            catch (Exception ex)
            {
                StringBuilder strerrorMsg = new StringBuilder();
                strerrorMsg.AppendLine(ex.Message);
                strerrorMsg.AppendLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    strerrorMsg.AppendLine(ex.InnerException.Message);
                    strerrorMsg.AppendLine(ex.InnerException.StackTrace);
                }
                ExceptionWriter.WriteLogEntry("Rest Pull Response for customer ID " + customerID + " " + DateTime.Now.ToString());
                ExceptionWriter.WriteLogEntry("Rest Pull Response Error --- " + HeaderID + "---" + Convert.ToString(strerrorMsg) + "---- " + DateTime.Now.ToString());
                IntegrationTask.SaveIntegrationTaskStatuswithFailure(strConnection, HeaderID.ToString(), DateTime.Now.Date, customerID.ToString(), true, true, "REST Pull Integration");
                IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, strerrorMsg.ToString(), "Error occured while saving data into table");

                ErrorLog.ErrorCollation(errors, SingleJsonObject, string.Empty, strerrorMsg.ToString());
                ErrorLog.CreateCsvLogFile(strConnection, HeaderID, integrationName, IntegrationType.RestGet, customerID, UserID, errors);
            }
        }

        private static List<Tuple<string, object>> GetValuesFromJsonRecursive(JObject obj, DataTable dtColumnCollection, string strTableName, string strConnection, int UserID, int HeaderID, int customerID, List<FieldCollections> fieldCollectionlist, List<ErrorLog> errors, string integrationName, List<Tuple<string, object>> fieldNames)
        {
            try
            {

                string strQueryFileds = string.Empty;
                string strQueryValueFields = string.Empty;
                string strQueryUpdate = string.Empty;
                string strQuerywhere = string.Empty;

                foreach (var property in obj.Properties())
                {
                    if (property.Value.Type == JTokenType.Object)
                    {
                        fieldNames = GetValuesFromJsonRecursive(property.Value as JObject,



                            dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                    }
                    else if (property.Value.Type == JTokenType.Array)
                    {
                        fieldNames = GetValuesFromJsonRecursive(property.Value as JArray, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                    }
                    else
                    {
                        if (dtColumnCollection != null && dtColumnCollection.Rows.Count > 0)
                        {
                            DataRow[] dr = dtColumnCollection.Select("RestPullResponseField='" + property.Name + "'");
                            if (dr != null && dr.Length > 0)
                            {
                                if (string.IsNullOrEmpty(firstFieldName))
                                {
                                    firstFieldName = property.Name;
                                }
                                else
                                {
                                    if (property.Name == firstFieldName)
                                    {
                                        SaveRestGetDataInner(fieldNames, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName);
                                        fieldNames.Clear();
                                    }
                                }

                                fieldNames.Add(new Tuple<string, object>(property.Name, property.Value));
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                StringBuilder strerrorMsg = new StringBuilder();
                strerrorMsg.AppendLine(ex.Message);
                strerrorMsg.AppendLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    strerrorMsg.AppendLine(ex.InnerException.Message);
                    strerrorMsg.AppendLine(ex.InnerException.StackTrace);
                }
                ExceptionWriter.WriteLogEntry("Rest Pull Response for customer ID " + customerID + " " + DateTime.Now.ToString());
                ExceptionWriter.WriteLogEntry("Rest Pull Response Error --- " + HeaderID + "---" + Convert.ToString(strerrorMsg) + "---- " + DateTime.Now.ToString());
                IntegrationTask.SaveIntegrationTaskStatuswithFailure(strConnection, HeaderID.ToString(), DateTime.Now.Date, customerID.ToString(), true, true, "REST Pull Integration");
                IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, strerrorMsg.ToString(), "Error occured while saving data into table");

                ErrorLog.ErrorCollation(errors, obj.ToString(), string.Empty, strerrorMsg.ToString());
                ErrorLog.CreateCsvLogFile(strConnection, HeaderID, integrationName, IntegrationType.RestGet, customerID, UserID, errors);
            }

            return fieldNames;
        }

        private static List<Tuple<string, object>> GetValuesFromJsonRecursive(JArray array, DataTable dtColumnCollection, string strTableName, string strConnection, int UserID, int HeaderID, int customerID, List<FieldCollections> fieldCollectionlist, List<ErrorLog> errors, string integrationName, List<Tuple<string, object>> fieldNames)
        {
            foreach (var item in array)
            {
                if (item.Type == JTokenType.Object)
                {
                    GetValuesFromJsonRecursive(item as JObject, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                }
                else if (item.Type == JTokenType.Array)
                {
                    GetValuesFromJsonRecursive(item as JArray, dtColumnCollection, strTableName, strConnection, UserID, HeaderID, customerID, fieldCollectionlist, errors, integrationName, fieldNames);
                }
                else
                {
                    Console.WriteLine($"Value: {item}");
                }
            }
            return fieldNames;
        }
        private static List<Tuple<string, string>> SaveRestGetDataInner(
            List<Tuple<string, object>> fieldNames,
            DataTable dtColumnCollection,
            string strTableName,
            string strConnection,
            int UserID,
            int HeaderID,
            int customerID,
            List<FieldCollections> fieldCollectionlist,
            List<ErrorLog> errors,
            string integrationName)
        {
            var result = new List<Tuple<string, string>>();

            try
            {
                if (fieldNames == null || fieldNames.Count == 0)
                {
                    result.Add(new Tuple<string, string>("Error", "No field data provided."));
                    return result;
                }

                using (SqlConnection conn = new SqlConnection(strConnection))
                {
                    conn.Open();

                    // 1️⃣ Build a lookup dictionary from fieldNames (FieldName → Value)
                    var fieldDict = fieldNames
                        .GroupBy(f => f.Item1)
                        .ToDictionary(g => g.Key, g => g.Last().Item2);

                    // 2️⃣ Build field/value lists based on column mapping
                    List<string> columnNames = new List<string>();
                    List<string> valueParams = new List<string>();
                    List<SqlParameter> sqlParams = new List<SqlParameter>();

                    foreach (DataRow mapRow in dtColumnCollection.Rows)
                    {
                        string responseField = Convert.ToString(mapRow["RestPullResponseField"]);
                        string tableField = Convert.ToString(mapRow["TblFieldName"]);

                        if (!string.IsNullOrEmpty(responseField) && !string.IsNullOrEmpty(tableField))
                        {
                            object value = fieldDict.ContainsKey(responseField)
                                ? fieldDict[responseField]
                                : DBNull.Value;

                            // Null-safe check
                            if (value == null || string.IsNullOrEmpty(value.ToString()))
                                value = DBNull.Value;

                            columnNames.Add($"[{tableField}]");
                            valueParams.Add($"@{tableField}");
                            // Convert JValue/JToken to plain .NET type if needed
                            if (value is JValue jv)
                                value = jv.Value; // extract inner primitive (string, int, etc.)

                            sqlParams.Add(new SqlParameter($"@{tableField}", value ?? DBNull.Value));
                        }
                    }

                    // 3️⃣ Add audit fields
                    columnNames.Add("[CreatedBy]");
                    columnNames.Add("[CreatedDate]");
                    valueParams.Add("@CreatedBy");
                    valueParams.Add("@CreatedDate");
                    sqlParams.Add(new SqlParameter("@CreatedBy", UserID));
                    sqlParams.Add(new SqlParameter("@CreatedDate", DateTime.Now));

                    // 4️⃣ Construct the INSERT query
                    string insertQuery = $@"
                INSERT INTO {strTableName} ({string.Join(",", columnNames)})
                VALUES ({string.Join(",", valueParams)})";

                    using (SqlCommand cmd = new SqlCommand(insertQuery, conn))
                    {
                        cmd.Parameters.AddRange(sqlParams.ToArray());
                        cmd.ExecuteNonQuery();
                    }

                    result.Add(new Tuple<string, string>("Success", $"Inserted into {strTableName}"));
                }
            }
            catch (Exception ex)
            {
                result.Add(new Tuple<string, string>("Error", ex.Message));
                ErrorLog.ErrorCollation(errors, ex.ToString(), $"{integrationName}: Failed while saving REST pull data");
            }

            return result;
        }

        //private static void SaveRestGetDataInner(List<Tuple<string, object>> fieldNames, DataTable dtColumnCollection, string strTableName, string strConnection, int UserID, int HeaderID, int customerID, List<FieldCollections> fieldCollectionlist, List<ErrorLog> errors, string integrationName)
        //{
        //    try
        //    {
        //        fieldNames = fieldNames ?? new List<Tuple<string, object>>();
        //        if (fieldNames.Count == 0)
        //        {
        //            return;
        //        }

        //        isProcessed = true;

        //        DataTable dt = new DataTable();

        //        // Get distinct column names
        //        var columnNames = fieldNames.Select(t => t.Item1).Distinct();

        //        // Add columns to DataTable
        //        foreach (var columnName in columnNames)
        //        {
        //            dt.Columns.Add(columnName);
        //        }

        //        DataRow row = dt.NewRow();

        //        var duplicateItems = fieldNames.GroupBy(x => x.Item1).Where(g => g.Count() > 1).Select(x => x.Key);

        //        for (int i = 0; i < fieldNames.Count; i++)
        //        {
        //            if (!duplicateItems.Contains(fieldNames[i].Item1))
        //            {
        //                var index = fieldNames.FindIndex(x => x.Item1 == fieldNames[i].Item1);
        //                row[fieldNames[i].Item1] = fieldNames[index].Item2;
        //            }
        //        }

        //        dt.Rows.Add(row);

        //        if (duplicateItems.Count() > 0)
        //        {
        //            foreach (var item in duplicateItems)
        //            {
        //                var childrenRecords = fieldNames.FindAll(f => f.Item1 == (fieldNames.GroupBy(x => x.Item1).Where(g => g.Count() > 1).Where(x => x.Key == item).Select(x => x.Key).FirstOrDefault()));
        //                for (int i = 0; i < childrenRecords.Count; i++)
        //                {
        //                    if (i <= dt.Rows.Count - 1)
        //                    {
        //                        DataRow dr = dt.Rows[i];
        //                        dr[item] = childrenRecords[i].Item2;
        //                    }
        //                    else
        //                    {
        //                        DataRow newRow = dt.NewRow();
        //                        newRow.ItemArray = row.ItemArray.Clone() as object[];
        //                        newRow[item] = childrenRecords[i].Item2;
        //                        dt.Rows.Add(newRow);
        //                    }
        //                }
        //            }
        //        }

        //        dt.AcceptChanges();

        //        if (dtColumnCollection != null && dtColumnCollection.Rows.Count > 0)
        //        {
        //            for (int r = 0; r < dt.Rows.Count; r++)
        //            {

        //                string strQueryFileds = string.Empty;
        //                string strQueryValueFields = string.Empty;
        //                string strQueryUpdate = string.Empty;
        //                string strQuerywhere = string.Empty;

        //                strTableName = strTableName ?? string.Empty;
        //                var isvalid = true;

        //                for (int col = 0; col < dt.Columns.Count; col++)
        //                {
        //                    DataRow[] dr = dtColumnCollection.Select("RestPullResponseField='" + dt.Columns[col] + "'");
        //                    if (dr != null && dr.Length > 0)
        //                    {
        //                        for (int i = 0; i < dr.Length; i++)
        //                        {
        //                            var defaultValue = (Convert.ToBoolean(dr[i]["IsHeaderTable"]) ? Convert.ToString(dr[i]["HeaderTableFieldDefaultValue"]) : Convert.ToString(dr[i]["DetailTableFieldDefaultValue"]));

        //                            dt.Rows[r][col] = dt.Rows[r][col] != null && dt.Rows[r][col] != DBNull.Value ? Convert.ToString(dt.Rows[r][col]).Replace("'", "''").Trim() : "";

        //                            defaultValue = !string.IsNullOrEmpty(defaultValue) ? defaultValue.Replace("'", "''").Trim() : "";

        //                            strQueryFileds += dr[i]["TblFieldName"] + ",";

        //                            //if (Convert.ToBoolean(dr[i]["IsTemplateTblValuelist"]))
        //                            //{
        //                            //    strQueryValueFields += " (select top 1 " + Convert.ToString(dr[i]["ValueListTemplateValueFieldName"]) + " from " + Convert.ToString(dr[i]["ValueListTemplateTableName"]) + " where CAST(" + Convert.ToString(dr[i]["ValueListTemplateDisplayFieldName"]) + " as VARCHAR(MAX)) = CAST('" + dt.Rows[r][col] + "' as VARCHAR(MAX)) ),";
        //                            //    strQueryUpdate += dr[i]["TblFieldName"] + "= (select top 1 " + Convert.ToString(dr[i]["ValueListTemplateValueFieldName"]) + " from " + Convert.ToString(dr[i]["ValueListTemplateTableName"]) + " where CAST(" + Convert.ToString(dr[i]["ValueListTemplateDisplayFieldName"]) + " as VARCHAR(MAX)) = CAST('" + dt.Rows[r][col] + "' as VARCHAR(MAX)) ) ,";

        //                            //    if (Convert.ToBoolean(dr[i]["IsIdentificationField"]))
        //                            //    {
        //                            //        if (string.IsNullOrEmpty(strQuerywhere.Trim()))
        //                            //        {
        //                            //            strQuerywhere = " WHERE ISNULL(" + Convert.ToString(dr[i]["TblFieldName"]) + ",'') = (select top 1 " + Convert.ToString(dr[i]["ValueListTemplateValueFieldName"]) + " from " + Convert.ToString(dr[i]["ValueListTemplateTableName"]) + " where CAST(" + Convert.ToString(dr[i]["ValueListTemplateDisplayFieldName"]) + " as VARCHAR(MAX)) = CAST('" + dt.Rows[r][col] + "' as VARCHAR(MAX)) ) ";
        //                            //        }
        //                            //        else
        //                            //        {
        //                            //            strQuerywhere = strQuerywhere + " AND ISNULL(" + Convert.ToString(dr[i]["TblFieldName"]) + ",'') = (select top 1 " + Convert.ToString(dr[i]["ValueListTemplateValueFieldName"]) + " from " + Convert.ToString(dr[i]["ValueListTemplateTableName"]) + " where CAST(" + Convert.ToString(dr[i]["ValueListTemplateDisplayFieldName"]) + " as VARCHAR(MAX)) = CAST('" + dt.Rows[r][col] + "' as VARCHAR(MAX)) )  ";
        //                            //        }
        //                            //    }
        //                            //}
        //                            //else
        //                            //{
        //                            //    if (strTableName.ToUpper() == "PEOPLE")
        //                            //    {
        //                            //        if (string.IsNullOrEmpty(dt.Rows[r][col].ToString()) && defaultValue != "")
        //                            //        {
        //                            //            dt.Rows[r][col] = defaultValue;
        //                            //            if (Convert.ToBoolean(dr[i]["IsFieldDefinationValueList"]))
        //                            //            {
        //                            //                var dropdownField = fieldCollectionlist.Where(x => x.FieldName.ToUpper() == dr[i]["TblFieldName"].ToString().ToUpper()).FirstOrDefault();
        //                            //                if (dropdownField != null)
        //                            //                {
        //                            //                    var valueList = dropdownField.DataSource.Where(t => t.DisplayFieldValue == dt.Rows[r][col].ToString()).FirstOrDefault();
        //                            //                    if (valueList == null)
        //                            //                    {
        //                            //                        ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt), $"{dr[i]["TblFieldName"].ToString()} value {dt.Rows[r][col]} does not exist.");
        //                            //                        isvalid = false;
        //                            //                        dt.Rows[r][col] = "";
        //                            //                    }
        //                            //                    else
        //                            //                    {
        //                            //                        dt.Rows[r][col] = valueList.ValueField;
        //                            //                    }
        //                            //                }
        //                            //                else
        //                            //                {
        //                            //                    ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt), $"{dr[i]["TblFieldName"].ToString()} does not exist.");
        //                            //                    isvalid = false;
        //                            //                    dt.Rows[r][col] = "";
        //                            //                }
        //                            //            }
        //                            //            else
        //                            //            {
        //                            //                dt.Rows[r][col] = dr[i]["HeaderTableFieldDefaultValue"].ToString();
        //                            //            }

        //                            //            //if(!string.IsNullOrEmpty(Convert.ToString(dt.Rows[r][col])))
        //                            //            //{
        //                            //            //    strQueryUpdate += dr[i]["TblFieldName"] + "='" + dt.Rows[r][col] + "',";
        //                            //            //    strQueryValueFields += "'" + dt.Rows[r][col] + "',";
        //                            //            //}
        //                            //            //else
        //                            //            //{
        //                            //            //    strQueryUpdate += dr[i]["TblFieldName"] + " = null,";
        //                            //            //    strQueryValueFields += "null,";
        //                            //            //}

        //                            //        }
        //                            //        else
        //                            //        {
        //                            //            if (dt.Rows[r][col].ToString().Trim() == "")
        //                            //            {
        //                            //                if (Convert.ToBoolean(dr[i]["IsRequired"]))
        //                            //                {
        //                            //                    ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt), $"{dr[i]["TblFieldName"].ToString()} is required.");
        //                            //                    isvalid = false;
        //                            //                }
        //                            //                dt.Rows[r][col] = "";
        //                            //            }
        //                            //            else
        //                            //            {
        //                            //                if (Convert.ToBoolean(dr[i]["IsFieldDefinationValueList"]))
        //                            //                {
        //                            //                    var dropdownField = fieldCollectionlist.Where(x => x.FieldName.ToUpper() == dr[i]["TblFieldName"].ToString().ToUpper()).FirstOrDefault();
        //                            //                    if (dropdownField != null)
        //                            //                    {
        //                            //                        var valueList = dropdownField.DataSource.Where(t => t.DisplayFieldValue == dt.Rows[r][col].ToString()).FirstOrDefault();

        //                            //                        if (valueList != null)
        //                            //                        {
        //                            //                            dt.Rows[r][col] = valueList.ValueField;
        //                            //                        }
        //                            //                        else
        //                            //                        {
        //                            //                            ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt), $"{dr[i]["TblFieldName"].ToString()} values {dt.Rows[r][col].ToString()} does not exist.");
        //                            //                            isvalid = false;
        //                            //                            dt.Rows[r][col] = "";
        //                            //                        }

        //                            //                    }
        //                            //                    else
        //                            //                    {
        //                            //                        ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt), $"{dr[i]["TblFieldName"].ToString()} does not exist.");
        //                            //                        isvalid = false;
        //                            //                        dt.Rows[r][col] = "";
        //                            //                    }


        //                            //                }
        //                            //                //else
        //                            //                //{
        //                            //                //    strQueryUpdate += dr[i]["TblFieldName"] + "='" + dt.Rows[r][col] + "',";
        //                            //                //    strQueryValueFields += "'" + dt.Rows[r][col] + "',";
        //                            //                //}
        //                            //            }
        //                            //        }
        //                            //    }
        //                            //    else
        //                            //    {
        //                            //        dt.Rows[r][col] = (string.IsNullOrEmpty(dt.Rows[r][col].ToString()) && defaultValue != "") ? defaultValue : dt.Rows[r][col];
        //                            //    }

        //                            //    if (!string.IsNullOrEmpty(Convert.ToString(dt.Rows[r][col])))
        //                            //    {
        //                            //        strQueryUpdate += dr[i]["TblFieldName"] + "='" + Convert.ToString(dt.Rows[r][col]) + "',";
        //                            //        strQueryValueFields += "'" + Convert.ToString(dt.Rows[r][col]) + "',";
        //                            //    }
        //                            //    else
        //                            //    {
        //                            //        strQueryUpdate += dr[i]["TblFieldName"] + "=null,";
        //                            //        strQueryValueFields += "null,";
        //                            //    }

        //                            //    if (Convert.ToBoolean(dr[i]["IsIdentificationField"]))
        //                            //    {
        //                            //        if (string.IsNullOrEmpty(strQuerywhere.Trim()))
        //                            //        {
        //                            //            strQuerywhere = " WHERE ISNULL(" + Convert.ToString(dr[i]["TblFieldName"]) + ",'') = '" + dt.Rows[r][col] + "'";
        //                            //        }
        //                            //        else
        //                            //        {
        //                            //            strQuerywhere = strQuerywhere + " AND ISNULL(" + Convert.ToString(dr[i]["TblFieldName"]) + ",'') = '" + dt.Rows[r][col] + "'";
        //                            //        }
        //                            //    }
        //                            //}

        //                        }
        //                    }
        //                }



        //                StringBuilder strFinalQuery = new StringBuilder();

        //                if (!string.IsNullOrEmpty(strQueryFileds) && !string.IsNullOrEmpty(strQueryValueFields) && !string.IsNullOrEmpty(strQueryUpdate) && isvalid)
        //                {
        //                    if (strTableName.ToUpper() == "PEOPLE")
        //                    {
        //                        strQueryFileds += "CreatedBy,CreateDate";
        //                        strQueryValueFields += "@UserID,GetDate()";
        //                        strQueryUpdate += "ModifiedBy=@UserID,ModifiedDate=GetDate()";
        //                    }
        //                    else if (!(strTableName.Trim().ToUpper().StartsWith("TRAN_") && strTableName.Trim().ToUpper().EndsWith("_DETAIL")))
        //                    {
        //                        strQueryFileds += "CreatedBy,CreatedDate";
        //                        strQueryValueFields += "@UserID,GetDate()";
        //                        strQueryUpdate += "UpdatedBy=@UserID,UpdatedDate=GetDate()";
        //                    }

        //                    if (!string.IsNullOrEmpty(strQuerywhere))
        //                    {
        //                        strFinalQuery.AppendLine($"IF NOT EXISTS (SELECT * FROM {strTableName} {strQuerywhere})");
        //                        strFinalQuery.AppendLine("BEGIN");
        //                        strFinalQuery.AppendLine($"  INSERT INTO {strTableName} ({strQueryFileds.TrimEnd(',')})");
        //                        strFinalQuery.AppendLine($"  VALUES ({strQueryValueFields.TrimEnd(',')})");
        //                        strFinalQuery.AppendLine("END");
        //                        strFinalQuery.AppendLine("ELSE");
        //                        strFinalQuery.AppendLine("BEGIN");
        //                        strFinalQuery.AppendLine($"  UPDATE {strTableName}");
        //                        strFinalQuery.AppendLine($"  SET {strQueryUpdate.TrimEnd(',')}");
        //                        strFinalQuery.AppendLine($" {strQuerywhere}");
        //                        strFinalQuery.AppendLine("END");
        //                    }
        //                    else
        //                    {
        //                        strFinalQuery.AppendLine("INSERT INTO " + strTableName + " ( " + strQueryFileds.TrimEnd(',') + " )");
        //                        strFinalQuery.AppendLine("VALUES (" + strQueryValueFields.TrimEnd(',') + ")");
        //                    }
        //                }

        //                if (!string.IsNullOrEmpty(strFinalQuery.ToString()))
        //                {
        //                    try
        //                    {
        //                        using (SqlConnection connection = new SqlConnection(strConnection))
        //                        {
        //                            SqlCommand sqlcmd = new SqlCommand(strFinalQuery.ToString(), connection);
        //                            connection.Open();
        //                            sqlcmd.Parameters.Add(new SqlParameter("@UserID", UserID));
        //                            sqlcmd.Parameters.Add(new SqlParameter("@IntegrationHeaderID", HeaderID));
        //                            sqlcmd.CommandType = CommandType.Text;
        //                            object obj = sqlcmd.ExecuteScalar();
        //                            connection.Close();
        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {

        //                        StringBuilder strerrorMsg = new StringBuilder();
        //                        strerrorMsg.AppendLine(ex.Message);
        //                        strerrorMsg.AppendLine(ex.StackTrace);
        //                        if (ex.InnerException != null)
        //                        {
        //                            strerrorMsg.AppendLine(ex.InnerException.Message);
        //                            strerrorMsg.AppendLine(ex.InnerException.StackTrace);
        //                        }
        //                        IntegrationTask.SaveIntegrationTaskStatuswithFailure(strConnection, HeaderID.ToString(), DateTime.Now.Date, customerID.ToString(), true, true, "REST Pull Integration");
        //                        IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, "Error occured while saving data into table. " + strerrorMsg.ToString(), strFinalQuery.ToString());
        //                        ErrorLog.ErrorCollation(errors, JsonConvert.SerializeObject(dt.Rows[r]), string.Empty, strerrorMsg.ToString());
        //                        ErrorLog.CreateCsvLogFile(strConnection, HeaderID, integrationName, IntegrationType.RestGet, customerID, UserID, errors);
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        StringBuilder strerrorMsg = new StringBuilder();
        //        strerrorMsg.AppendLine(ex.Message);
        //        strerrorMsg.AppendLine(ex.StackTrace);
        //        if (ex.InnerException != null)
        //        {
        //            strerrorMsg.AppendLine(ex.InnerException.Message);
        //            strerrorMsg.AppendLine(ex.InnerException.StackTrace);
        //        }
        //        IntegrationTask.SaveIntegrationTaskStatuswithFailure(strConnection, HeaderID.ToString(), DateTime.Now.Date, customerID.ToString(), true, true, "REST Pull Integration");
        //        IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, strerrorMsg.ToString(), "Error occured while saving data into table");

        //        ErrorLog.ErrorCollation(errors, "", string.Empty, strerrorMsg.ToString());
        //        ErrorLog.CreateCsvLogFile(strConnection, HeaderID, integrationName, IntegrationType.RestGet, customerID, UserID, errors);
        //    }
        //}

        private static string Authorization(string strConnection, int intAuthenticationType, DataTable dtHeaderData, int HeaderID, List<ErrorLog> errors)
        {
            var authorizationToken = "";

            try
            {
                #region UserName/Password Authorization Method 
                if (intAuthenticationType == Convert.ToInt16(AuthenticationType.UserNamePassword))
                {
                    var parameterTypeForLoginAPI = dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"] != DBNull.Value
                        ? Convert.ToInt32(dtHeaderData.Rows[0]["ParameterTypeForLoginAPI"])
                        : -1;

                    var keyNameOfUserName = dtHeaderData.Rows[0]["KeyNameOfUserName"] != DBNull.Value && dtHeaderData.Rows[0]["KeyNameOfUserName"] != null
                       ? Convert.ToString(dtHeaderData.Rows[0]["KeyNameOfUserName"]).Trim()
                       : "";

                    var keyValueOfUserName = dtHeaderData.Rows[0]["KeyValueOfUserName"] != DBNull.Value && dtHeaderData.Rows[0]["KeyValueOfUserName"] != null
                       ? Convert.ToString(dtHeaderData.Rows[0]["KeyValueOfUserName"]).Trim()
                       : "";

                    var keyNameOfPassword = dtHeaderData.Rows[0]["KeyNameOfPassword"] != DBNull.Value && dtHeaderData.Rows[0]["KeyNameOfPassword"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["KeyNameOfPassword"])
                        : "";

                    var keyValueOfPassword = dtHeaderData.Rows[0]["KeyValueOfPassword"] != DBNull.Value && dtHeaderData.Rows[0]["KeyValueOfPassword"] != null
                       ? Convert.ToString(dtHeaderData.Rows[0]["KeyValueOfPassword"]).Trim()
                       : "";

                    var loginApiUrl = dtHeaderData.Rows[0]["LoginAPIURL"] != DBNull.Value && dtHeaderData.Rows[0]["LoginAPIURL"] != null
                       ? Convert.ToString(dtHeaderData.Rows[0]["LoginAPIURL"]).Trim()
                       : "";

                    var tokenKeyNameAfterLogin = dtHeaderData.Rows[0]["TokenKeyNameAfterLogin"] != DBNull.Value && dtHeaderData.Rows[0]["TokenKeyNameAfterLogin"] != null
                       ? Convert.ToString(dtHeaderData.Rows[0]["TokenKeyNameAfterLogin"]).Trim()
                       : "";

                    var encryptToBase64 = "";
                    var decryptedADPassword = string.Empty;
                    if (!string.IsNullOrEmpty(keyValueOfPassword.Trim()))
                    {
                        decryptedADPassword = Decrypt(strConnection, keyValueOfPassword, "15582558");
                    }

                    if (!string.IsNullOrEmpty(decryptedADPassword))
                    {
                        encryptToBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(decryptedADPassword));
                    }

                    var urlParams = "";

                    if (parameterTypeForLoginAPI == Convert.ToInt32(RESTPullAPIParameterType.URLParameter))
                    {
                        urlParams += (string.IsNullOrEmpty(urlParams) ? "?" : "&") + keyNameOfUserName + "=" + keyValueOfUserName + "&" + keyNameOfPassword + "=" + encryptToBase64;
                    }

                    urlParams += (string.IsNullOrEmpty(urlParams) ? "?" : "&") + "format=json";

                    loginApiUrl = loginApiUrl + urlParams;
                    var clientForUserToken = new RestClient();
                    var requestForUserToken = new RestRequest(loginApiUrl, Method.Post);
                    requestForUserToken.AddHeader("Content-Type", "application/json");

                    if (parameterTypeForLoginAPI == Convert.ToInt32(RESTPullAPIParameterType.HeaderParameter))
                    {
                        var objLogin = new DataTable();
                        objLogin.Columns.Add(keyNameOfUserName);
                        objLogin.Columns.Add(keyNameOfPassword);

                        DataRow drLogin;
                        drLogin = objLogin.NewRow();

                        drLogin[keyNameOfUserName] = keyValueOfUserName;
                        drLogin[keyNameOfPassword] = encryptToBase64;
                        objLogin.Rows.Add(drLogin);

                        var body = JsonConvert.SerializeObject(objLogin);

                        if (body.StartsWith("["))
                        {
                            body = body.Remove(0, 1);
                        }
                        if (body.EndsWith("]"))
                        {
                            body = body.Remove(body.Length - 1, 1);
                        }

                        requestForUserToken.AddStringBody(body, DataFormat.Json);
                    }

                    RestResponse responseForUserToken = clientForUserToken.Execute(requestForUserToken);

                    if (responseForUserToken != null && !string.IsNullOrEmpty(responseForUserToken.Content) && responseForUserToken.ResponseStatus == ResponseStatus.Completed)
                    {
                        dynamic objLoginData = JObject.Parse(responseForUserToken.Content);
                        authorizationToken = objLoginData[tokenKeyNameAfterLogin];
                    }
                    else
                    {
                        var errorMessage = new StringBuilder();
                        errorMessage.AppendLine($"Response Status: {responseForUserToken.ResponseStatus.ToString()}");

                        if (!string.IsNullOrEmpty(responseForUserToken.Content))
                        {
                            errorMessage.AppendLine($"Response Content: {responseForUserToken.Content}");
                        }

                        if (!string.IsNullOrEmpty(responseForUserToken.ErrorMessage))
                        {
                            errorMessage.AppendLine($"Error Message: {responseForUserToken.ErrorMessage}");
                        }

                        if (responseForUserToken.ErrorException != null)
                        {
                            errorMessage.AppendLine($"Error Exception Message: {responseForUserToken.ErrorException.Message}");
                        }

                        ErrorLog.ErrorCollation(errors, string.Empty, string.Empty, errorMessage.ToString());

                        IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, errorMessage.ToString(), "Error occured while authorizing OAuth method.");
                    }
                }
                #endregion

                #region OAuth 2.0 Authorization Method 
                else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.OAUTH))
                {
                    var accessTokenURL = dtHeaderData.Rows[0]["AccessTokenURL"] != DBNull.Value && dtHeaderData.Rows[0]["AccessTokenURL"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["AccessTokenURL"]).Trim()
                        : "";

                    var clientID = dtHeaderData.Rows[0]["ClientID"] != DBNull.Value && dtHeaderData.Rows[0]["ClientID"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["ClientID"]).Trim()
                        : "";

                    var scope = dtHeaderData.Rows[0]["Scope"] != DBNull.Value && dtHeaderData.Rows[0]["Scope"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["Scope"]).Trim()
                        : "";

                    var grantType = dtHeaderData.Rows[0]["GrantType"] != DBNull.Value && dtHeaderData.Rows[0]["GrantType"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["GrantType"]).Trim()
                        : "";

                    var clientSecret = dtHeaderData.Rows[0]["ClientSecret"] != DBNull.Value && dtHeaderData.Rows[0]["ClientSecret"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["ClientSecret"]).Trim()
                        : "";

                    var clientAuthentication = dtHeaderData.Rows[0]["ClientAuthentication"] != DBNull.Value && dtHeaderData.Rows[0]["ClientAuthentication"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["ClientAuthentication"]).Trim()
                        : "";

                    var tokenName = dtHeaderData.Rows[0]["TokenNameOAuth"] != DBNull.Value && dtHeaderData.Rows[0]["TokenNameOAuth"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["TokenNameOAuth"]).Trim()
                        : "";

                    var clientForOAuthToken = new RestClient();
                    var requestForOAuthToken = new RestRequest(accessTokenURL, Method.Post);
                    requestForOAuthToken.AddHeader("Content-Type", "application/x-www-form-urlencoded");
                    requestForOAuthToken.AddParameter("scope", scope);
                    requestForOAuthToken.AddParameter("grant_type", grantType);
                    requestForOAuthToken.AddParameter("client_id", clientID);
                    requestForOAuthToken.AddParameter("client_secret", clientSecret);
                    requestForOAuthToken.AddParameter("token_name", tokenName);
                    RestResponse responseForOAuthToken = clientForOAuthToken.Execute(requestForOAuthToken);

                    if (responseForOAuthToken != null && !string.IsNullOrEmpty(responseForOAuthToken.Content) && responseForOAuthToken.ResponseStatus == ResponseStatus.Completed)
                    {
                        dynamic obj = Newtonsoft.Json.Linq.JObject.Parse(responseForOAuthToken.Content);
                        var bearer = obj.access_token;

                        authorizationToken = $"Bearer {bearer}";
                    }
                    else
                    {
                        var errorMessage = new StringBuilder();
                        errorMessage.AppendLine($"Response Status: {responseForOAuthToken.ResponseStatus.ToString()}");

                        if (!string.IsNullOrEmpty(responseForOAuthToken.Content))
                        {
                            errorMessage.AppendLine($"Response Content: {responseForOAuthToken.Content}");
                        }

                        if (!string.IsNullOrEmpty(responseForOAuthToken.ErrorMessage))
                        {
                            errorMessage.AppendLine($"Error Message: {responseForOAuthToken.ErrorMessage}");
                        }

                        if (responseForOAuthToken.ErrorException != null)
                        {
                            errorMessage.AppendLine($"Error Exception Message: {responseForOAuthToken.ErrorException.Message}");
                        }

                        ErrorLog.ErrorCollation(errors, string.Empty, string.Empty, errorMessage.ToString());

                        IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, errorMessage.ToString(), "Error occured while authorizing OAuth method.");
                    }
                }
                #endregion

                #region Basic Authorization Method 
                else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.BasicAuth))
                {
                    var basicAuthUserName = dtHeaderData.Rows[0]["BasicAuthUsername"] != DBNull.Value && dtHeaderData.Rows[0]["BasicAuthUsername"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["BasicAuthUsername"]).Trim()
                        : "";

                    var encryptedBasicAuthPassWord = dtHeaderData.Rows[0]["BasicAuthPassword"] != DBNull.Value && dtHeaderData.Rows[0]["BasicAuthPassword"] != null
                        ? Convert.ToString(dtHeaderData.Rows[0]["BasicAuthPassword"]).Trim()
                        : "";
                    string decryptedBasicAuthPassWord;

                    // 🔹 If you are testing with plain password ("1234") in DB:
                    if (encryptedBasicAuthPassWord == "passwd")
                    {
                        decryptedBasicAuthPassWord = "passwd";
                    }
                    else
                    {
                        // 🔹 Otherwise decrypt it normally
                        decryptedBasicAuthPassWord = Decrypt(strConnection, encryptedBasicAuthPassWord, "15582558");
                    }

                    // 🔹 Encode username:password to Base64
                    var credentials = $"{basicAuthUserName}:{decryptedBasicAuthPassWord}";
                    var svcCredentials = Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials));

                    authorizationToken = $"Basic {svcCredentials}";
                    // var decryptedBasicAuthPassWord = string.Empty;
                    // if (!string.IsNullOrEmpty(encryptedBasicAuthPassWord))
                    // {
                    //     decryptedBasicAuthPassWord = Decrypt(strConnection, encryptedBasicAuthPassWord, "15582558");
                    // }

                    // var svcCredentials = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(basicAuthUserName + ":" + "1234"));

                    //authorizationToken = $"Basic {svcCredentials}";
                }
                //else if (intAuthenticationType == Convert.ToInt16(AuthenticationType.BasicAuth))
                //{
                //    // ❌ Remove old BasicAuth logic

                //    // Suppose you have already obtained the JWT token via login:
                //    string jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjEiLCJlbWFpbCI6ImFkbWluQGFkbWluLmNvbSIsImZpcnN0TmFtZSI6IlByZXN0b24iLCJsYXN0TmFtZSI6IkNhc3RybyIsInBob25lTm8iOiI4MDkwOTA5MDg3NyIsInJvbGVJZCI6IjEiLCJQYXNzd29yZEV4cGlyZWQiOiJmYWxzZSIsImV4cCI6MTc5MTYwODQ3OCwiaXNzIjoieW91cmRvbWFpbi5jb20iLCJhdWQiOiJ5b3VyZG9tYWluLmNvbSJ9.3pnJLo6C3NYXkvSBxdEvAGp2rQy3nnMuFlUZhLLEy68";

                //    // Set authorization token
                //    authorizationToken = $"Bearer {jwtToken}";
                //}

                #endregion
            }
            catch (Exception ex)
            {
                StringBuilder strerrorMsg = new StringBuilder();
                strerrorMsg.AppendLine(ex.Message);
                strerrorMsg.AppendLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    strerrorMsg.AppendLine(ex.InnerException.Message);
                    strerrorMsg.AppendLine(ex.InnerException.StackTrace);
                }
                IntegrationTask.InsertRESTPullIntegrationLog(strConnection, HeaderID, strerrorMsg.ToString(), "Error occured in rest pull integration authorization");
                ErrorLog.ErrorCollation(errors, "", string.Empty, strerrorMsg.ToString());
            }
            return authorizationToken;
        }
        public static int GetEntityId(string strConnection, int HeaderID)
        {
            int entityId = 0;

            string query = @"
       SELECT TOP 1 TableId
FROM FileIntegrationDetail 
WHERE  HeaderConfigurationID= @ID";

            using (SqlConnection connection = new SqlConnection(strConnection))
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.Parameters.AddWithValue("@ID", HeaderID);
                    connection.Open();

                    var result = cmd.ExecuteScalar();
                    if (result != null && int.TryParse(result.ToString(), out int id))
                    {
                        entityId = id;
                    }
                }
            }

            return entityId;
        }
        public static List<IntegrationHeaderModel> GetEntityFieldsFromAPI(int entityId)
        {
            var results = new List<IntegrationHeaderModel>();

            using (HttpClient client = new HttpClient())
            {
                string apiUrl = $"https://localhost:44311/table/IntegartionGetEntityFields?entityId={entityId}";

                try
                {
                    var response = client.GetAsync(apiUrl).Result;

                    if (response.IsSuccessStatusCode)
                    {
                        var json = response.Content.ReadAsStringAsync().Result;
                        var apiResponse = Newtonsoft.Json.JsonConvert.DeserializeObject<EntityFieldApiResponse>(json);

                        if (apiResponse?.Result != null)
                        {
                            results.AddRange(apiResponse.Result);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("API Error: " + ex.Message);
                }
            }

            return results;
        }
        public class IntegrationHeaderModel
        {
            public string FieldName { get; set; }
            public int FieldId { get; set; }
            public string FieldType { get; set; }
            public int? Length { get; set; }
            public bool Mandatory { get; set; }
            public string DetailTableFieldName { get; set; }
            public string HeaderTableName { get; set; }
            public string DetailEntityName { get; set; }
        }

        public class EntityFieldApiResponse
        {
            public List<IntegrationHeaderModel> Result { get; set; }
        }
        #endregion

    }
}
