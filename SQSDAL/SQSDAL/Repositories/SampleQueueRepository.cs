using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDAL.Repositories
{
    public class SampleQueueRepository<T> : BaseSQSRepository<T>
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();

        private static string sqsQueueName = appSettingsReader.GetValue("SQSQueueName", typeof(string)).ToString();

        public SampleQueueRepository(int userId, string queueName)
            : base(userId, queueName)
        {

        }
    }
}
