using SQSDAL.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDALExample.Repositories
{
    class MyTestQueueRepository<MyMessageModel> : BaseSQSRepository<MyMessageModel>
    {
        public MyTestQueueRepository(int userId, string queueName, int defaultVisibilityTimeout) : base(userId, queueName, defaultVisibilityTimeout )
        {

        }
    }
}
