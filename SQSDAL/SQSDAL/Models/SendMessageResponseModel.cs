using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDAL.Models
{
    public class SendMessageResponseModel<T>
    {
        public List<T> SendMessagesSuccessfully { get; set; }
        public List<T> UnprocessedMessages { get; set; }

        public SendMessageResponseModel()
        {
            SendMessagesSuccessfully = new List<T>();
            UnprocessedMessages = new List<T>();
        }
    }
}
