using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDALExample.MessageModels
{
    class MessageDiary
    {
        public MessageDiary()
        {
            this.Diary = new Dictionary<DateTime, List<MyMessageModel>>();
        }

        public Dictionary<DateTime, List<MyMessageModel>> Diary { get; set; }

        public static MessageDiary CreateMessageDiaryForTesting()
        {
            MessageDiary messageDiary = new MessageDiary();

            for(int i = 0; i < 20; i++)
            {
                DateTime messageDate = DateTime.UtcNow.AddDays(-i);
                MyMessageModel message = new MyMessageModel();

                message.MyMessage = "Hi!The date is " + messageDate.ToString("dd/mmm/yyyy hh:mm:ss");

                if (messageDiary.Diary.ContainsKey(messageDate))
                {
                    messageDiary.Diary[messageDate].Add(message);
                }
                else
                {
                    messageDiary.Diary[messageDate] = new List<MyMessageModel>() { message };
                }
            }

            return messageDiary;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            foreach(DateTime messageDate in Diary.Keys)
            {
                sb.Append("\n");
                sb.Append(messageDate);
                sb.Append(" ");

                foreach(MyMessageModel message in Diary[messageDate])
                {
                    sb.Append(message.MyMessage);
                    sb.Append(" ");
                }
            }

            return sb.ToString();
        }
    }

}
