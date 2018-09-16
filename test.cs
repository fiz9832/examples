using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Event = Bloomberglp.Blpapi.Event;
using Message = Bloomberglp.Blpapi.Message;
using Name = Bloomberglp.Blpapi.Name;
using Request = Bloomberglp.Blpapi.Request;
using Service = Bloomberglp.Blpapi.Service;
using Session = Bloomberglp.Blpapi.Session;
using Datetime = Bloomberglp.Blpapi.Datetime;
using DataType = Bloomberglp.Blpapi.Schema.Datatype;
using Bloomberglp;
using Bloomberglp.Blpapi;

namespace Service
{
    public class RefDataItem
    {
        public string Security { get; set; }
        public string Field { get; set; }
        public string Value { get; set; }
    }

    public enum BlpTailType { Index, Comdty, Curncy };

    public class BlpService
    {
        
 
        private static readonly Name SECURITY_DATA = new Name("securityData");
        private static readonly Name SECURITY = new Name("security");
        private static readonly Name FIELD_DATA = new Name("fieldData");
        private static readonly Name RESPONSE_ERROR = new Name("responseError");
        private static readonly Name SECURITY_ERROR = new Name("securityError");
        private static readonly Name FIELD_EXCEPTIONS = new Name("fieldExceptions");
        private static readonly Name FIELD_ID = new Name("fieldId");
        private static readonly Name ERROR_INFO = new Name("errorInfo");
        private static readonly Name CATEGORY = new Name("category");
        private static readonly Name MESSAGE = new Name("message");
        private static readonly Name DATE = new Name("date");

        private Session session;
        private Service service;

        public void Connect()
        {
            string serverHost = "api.bloomberg.com";
            int serverPort = 0;

            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = serverHost;
            sessionOptions.ServerPort = serverPort;

            Log("Connecting to " + serverHost + ":" + serverPort);
            session = new Session(sessionOptions);
            bool sessionStarted = session.Start();
            if (!sessionStarted)
                throw new Exception("Failed to start Bloomberg API session.");
            if (!session.OpenService("//blp/refdata"))
                throw new Exception("Failed to open //blp/refdata");
            this.service = session.GetService("//blp/refdata");
        }

        public IEnumerable<RefDataItem> GetRefData(IList<string> tickers, IList<string> fields)
        {
            var req = this.service.CreateRequest("ReferenceDataRequest");
            var rsecurities = req.GetElement("securities");
            foreach (var tk in tickers)
                rsecurities.AppendValue(tk.Replace("_", " "));
            var rfields = req.GetElement("fields");
            foreach (var f in fields)
                rfields.AppendValue(f);
            session.SendRequest(req, null);
            while (true)
            {
                var e = session.NextEvent();
                if (e.Type == Event.EventType.RESPONSE || 
                    e.Type == Event.EventType.PARTIAL_RESPONSE)
                {
                    foreach (var msg in e)
                    {
                        Log("--" + msg.MessageType.ToString());
                        var securities = msg.GetElement(SECURITY_DATA);
                        for (int i = 0; i < securities.NumValues; i++)
                        {
                            var sec = securities.GetValueAsElement(i);
                            var tk = sec.GetElementAsString(SECURITY);
                            if (sec.HasElement("securityError"))
                            {
                                Log("Invalid Security: " + tk, InfoLevel.Warn);
                                continue;
                            }
                            var ffs = sec.GetElement(FIELD_DATA);
                            if (ffs.NumElements > 0)
                            {
                                for (int j = 0; j < ffs.NumElements; j++)
                                {
                                    var fieldData = ffs.GetElement(j);
                                    var fieldName = fieldData.Name.ToString();
                                    var fieldValue = fieldData.GetValueAsString();
                                    Log("---- " + fieldName + ": " + fieldValue);

                                    yield return new RefDataItem() { Security = tk, 
					    Field = fieldName, Value = fieldValue };
                                }
                            }
                            var ferrs = sec.GetElement(FIELD_EXCEPTIONS);
                            for(int j = 0; j < ferrs.NumValues; j++)
                            {
                                var ferr = ferrs.GetValueAsElement(j);
                                var ferrName = ferr.GetElementAsString(FIELD_ID);
                                var ferrInfo = ferr.GetElement(ERROR_INFO)
                                                   .GetElementAsString("message");
                                Log("Field Error for '" + tk + 
						"': [" + ferrName + "] " + ferrInfo, InfoLevel.Error);
                            }

                        }
                    }
                }
                //--- this means the complete message is done
                if (e.Type == Event.EventType.RESPONSE)
                    yield break;
            }
        }

        public IEnumerable<Tuple<DateTime, string>> GetDailyHistory(string ticker, 
			string field,
            		string startDt = "19700101", 
			string endDt = "")
        {
            if (endDt.Length == 0)
                endDt = DateTime.Now.ToString("yyyyMMdd");

            var req = this.service.CreateRequest("HistoricalDataRequest");
            req.GetElement("securities").AppendValue(ticker);
            req.GetElement("fields").AppendValue(field);
            req.Set("startDate", startDt);
            req.Set("endDate", endDt);
            session.SendRequest(req, null);

            while (true)
            {
                var e = session.NextEvent();
                Log("-" + e.Type.ToString());
                if (e.Type == Event.EventType.RESPONSE ||
                    e.Type == Event.EventType.PARTIAL_RESPONSE)
                {
                    foreach (var msg in e)
                    {
                        if (msg.HasElement(RESPONSE_ERROR))
                        {
                            Log($"EOD History Error: {msg.GetElement(RESPONSE_ERROR).ToString()}", 
					    InfoLevel.Error);
                            yield break;
                        }
                        var sec = msg.GetElement(SECURITY_DATA);
                        var tk = sec.GetElementAsString(SECURITY);

                        if (sec.HasElement("securityError"))
                        {
                            Log($"Invalid Security: {tk}", InfoLevel.Warn);
                            yield break;
                        }

                        Log("--- " + tk);
                        var ffs = sec.GetElement(FIELD_DATA);
                        for (int j = 0; j < ffs.NumValues; j++)
                        {
                            var element = ffs.GetValueAsElement(j);
                            var dt = element.GetElementAsDatetime(DATE).ToSystemDateTime();
                            if (!element.HasElement(field))
                            {
                                Log($"Invalid Field: {field}");
                                yield break;
                            }
                            var ff = element.GetElement(field);
                            yield return new Tuple<DateTime, string>(dt, ff.GetValueAsString());
                        }
                    }
                }
                //--- this means the complete message is done
                if (e.Type == Event.EventType.RESPONSE)
                    yield break;
            }
        }
    }
}
