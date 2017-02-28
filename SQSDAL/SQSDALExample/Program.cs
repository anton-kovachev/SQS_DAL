using SQSDALExample.MessageModels;
using SQSDALExample.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDALExample
{
    class Program
    {
        static void Main(string[] args)
        {
            MessageDiary myTestDiary = MessageDiary.CreateMessageDiaryForTesting();
            MessageDiary myTestDiary1 = MessageDiary.CreateMessageDiaryForTesting();
            MessageDiary myTestDiary2 = MessageDiary.CreateMessageDiaryForTesting();
            
            IEnumerable<MessageDiary> diaryList = new List<MessageDiary> () { myTestDiary, myTestDiary1, myTestDiary2 };

            int userId = 1;
            string queueName = "myDiaryQueu";
            int messageVisibilityTimeout = 3 * 60;

            MyTestQueueRepository<MessageDiary> diaryQueue = new MyTestQueueRepository<MessageDiary>(userId, queueName, messageVisibilityTimeout);
            diaryQueue.SaveMessages(diaryList);

            diaryQueue.GetAndProcessMessages(md => { Console.WriteLine(md.ToString()); return true; }, 0);
        }
    }
}
