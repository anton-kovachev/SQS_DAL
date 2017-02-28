using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQSDAL.Models
{
    public class DeleteMessagesResponseModel<T>
    {
        public List<T> Successful { get; set; }
        public List<T> Failed { get; set; }

        public DeleteMessagesResponseModel()
        {
            Successful = new List<T>();
            Failed = new List<T>();
        }
    }
}
