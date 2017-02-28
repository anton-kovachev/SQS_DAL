using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SQSDAL.Models;

namespace SQSDAL.Repositories
{
    /// <summary>
    /// The base class for managing operations with Amazon SQS 
    /// In order to use it you must either extend(inherit) with your own custom defined class or instantiate it with
    /// some of the provided ctors
    /// Prerequisites: You must enter in your application's App.config or Web.config the folloing keys
    ///    <add key="AWSProfileName" value="your_aws_profilename" />
    ///    <add key="AWSAccessKey" value="your_aws_access_key" />
    ///    <add key="AWSSecretKey" value="your_aws_secret_key" />
    ///    <!-- the aws region where you want the tables to be created -->
    ///    <add key="AWSRegion" value="us-east-1" />
    ///    
    ///    the class searches first for the "AWSRegion" key for a location where to create/use the queue with the specified queue name (if you do not specity an AWSRegion in your app's config file you must use one of the two BaseSQSRepository constructors that accept an RegionEndpoin as parameter.)
    ///    , if it does not find that key the library thorws an error.
    ///    If the instance(EC2 eventually) running the application that uses the library is configured with an IAM role that grants it access to AWS SQS service you can skip the  AWSProfileName AWSAccessKey AWSSecretKey keys. 
    ///    Supported AWS regions are: us-east-1, us-west-1, us-west-2, ap-south-1, ap-northeast-1, ap-northeast-2, ap-southeast-1, ap-southeast-2, eu-central-1, eu-west-1, sa-east-1 
    ///    Example: 
    ///     <add key="CurrentAmazonRegionalEndPoint" value="EU" /> -> will perform the sqs operations in the EU(Ireland) aws region.
    ///     <add key="AWSRegion" value="us-east-1" />
    /// </summary>
    /// <typeparam name="T">The type of the messages you want to save and read from sqs.</typeparam>
    public class BaseSQSRepository<T>
    {
        private UnitOfWork<T> unitOfWork;

        //public BaseSQSRepository(int userId)
        //{
        //    this.unitOfWork = new UnitOfWork<T>(userId);
        //}

        /// <summary>
        /// Creates a new BaseSQSRepository object with the specified userId and the specified queue name.
        /// If a queue in the "CurrentAmazonRegionalEndPoint" with the same name does not exists , the class creates a new queue with "queuename" ,
        /// if a queue with that name does exists the class will use it with all her messages.
        /// </summary>
        /// <param name="userId">The userId of the user that creates the repository.</param>
        /// <param name="queueName">The name of the sqs queue.</param>
        public BaseSQSRepository(int userId, string queueName)
        {
            this.unitOfWork = new UnitOfWork<T>(userId, queueName);
        }

        /// <summary>
        /// Creates a new BaseSQSRepository object with the specified userId and the specified queue name.
        /// If a queue in the "CurrentAmazonRegionalEndPoint" with the same name does not exists , the class creates a new queue with "queuename" ,
        /// if a queue with that name does exists the class will use it with all her messages.
        /// After a connection with the SQS queue is established , the queue's visibility timeot property value will be set with the value of "defaultVisibilityTimeOutInHours" 
        /// parameter.
        /// </summary>
        /// <param name="userId">The userId of the user that creates the repository.</param>
        /// <param name="queueName">The name of the sqs queue.</param>
        ///<param name="defaultVisibilityTimeOutInHours">The visibility time out of the messages in queue , after they have been red from some client.
        ///For more information see AWS SQS docs.</param>
        public BaseSQSRepository(int userId, string queueName, int defaultVisibilityTimeOutInHours)
        {
            this.unitOfWork = new UnitOfWork<T>(userId, queueName, defaultVisibilityTimeOutInHours);
        }

        /// <summary>
        /// Creates a new BaseSQSRepository object with the specified userId and the specified queue name.
        /// If a queue in the "CurrentAmazonRegionalEndPoint" with the same name does not exists , the class creates a new queue with "queuename" ,
        /// if a queue with that name does exists the class will use it with all her messages.
        /// </summary>
        /// <param name="userId">The userId of the user that creates the repository.</param>
        /// <param name="queueName">The name of the sqs queue.</param>
        public BaseSQSRepository(int userId, string queueName, RegionEndpoint region)
        {
            this.unitOfWork = new UnitOfWork<T>(userId, queueName, region);
        }

        /// <summary>
        /// Creates a new BaseSQSRepository object with the specified userId and the specified queue name.
        /// If a queue in the "CurrentAmazonRegionalEndPoint" with the same name does not exists , the class creates a new queue with "queuename" ,
        /// if a queue with that name does exists the class will use it with all her messages.
        /// After a connection with the SQS queue is established , the queue's visibility timeot property value will be set with the value of "defaultVisibilityTimeOutInHours" 
        /// parameter.
        /// </summary>
        /// <param name="userId">The userId of the user that creates the repository.</param>
        /// <param name="queueName">The name of the sqs queue.</param>
        ///<param name="defaultVisibilityTimeOutInHours">The visibility time out of the messages in queue , after they have been red from some client.
        ///For more information see AWS SQS docs.</param>
        public BaseSQSRepository(int userId, string queueName, int defaultVisibilityTimeOutInHours, RegionEndpoint region)
        {
            this.unitOfWork = new UnitOfWork<T>(userId, queueName, defaultVisibilityTimeOutInHours, region);
        }


        /// <summary>
        /// Creates a new BaseSQSRepository with a connection established to the same queue of which "baseSQSRepository" is already connected.
        /// Useful when you want to extend BaseSQSRepository<T> with inheritance and allow classes with different method implementations to access the same queue or
        /// just share the working queue of the current object with another one.
        /// </summary>
        /// <param name="baseSQSRepository">The already existing BaseSQSRepository object.</param>
        public BaseSQSRepository(BaseSQSRepository<T> baseSQSRepository)
        {
            this.unitOfWork = baseSQSRepository.unitOfWork;
        }

        /// <summary>
        /// Returns a list with the names of all queues existing in your aws region, which is specified by "CurrentAmazonRegionalEndPoint" key.
        /// </summary>
        /// <returns></returns>
        public List<string> GetAllQueues()
        {
            return this.unitOfWork.GetAvailableQueues(); 
        }

        /// <summary>
        /// Saves a message of type T in the current sqs queue.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool SaveMessage(T message)
        {
            bool result = this.unitOfWork.Save(message);
            return result;
        }

        /// <summary>
        /// Saves the messages of type T to the current SQS queue in batches of ten.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages"></param>
        /// <returns></returns>
        public SendMessageResponseModel<T> SaveMessages(IEnumerable<T> messages)
        {
            IEnumerable<T> messagesBatch = new List<T>(10);
            SendMessageResponseModel<T> globalBatchResponse = new SendMessageResponseModel<T>();

            int skipNumber = 0;
            int batchSize = 10;

            do 
            {
                messagesBatch = messages.Skip(skipNumber).Take(batchSize);
                SendMessageResponseModel<T> response = this.unitOfWork.Save(messagesBatch);

                globalBatchResponse.SendMessagesSuccessfully.AddRange(response.SendMessagesSuccessfully);
                globalBatchResponse.UnprocessedMessages.AddRange(response.UnprocessedMessages);

                skipNumber += batchSize;
            }
            while(skipNumber < messages.Count());


            return globalBatchResponse;
        }

        /// <summary>
        /// Method that reads the available messages from sqs and process them with the "handler" func.
        /// If a message is processed successfully by the "handler" Func (the "handler" returns "true" ) and "deleteMessagesFromQueue" is set to true , the messages will be deleted from the queue 
        /// as soons as all messages from the current red messages batch are processed.
        /// If the "handler" Func for a processed messages returns "false" , the message will not be deleted from the queue, mo matter what is the value of the deleteMessagesFromQueue parameter"  
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="handler">The operation which is applied of every message that is red.</param>
        /// <param name="timeOut">The wait time between the end of the processing of the last messages and the start of the next one in millisconds.Default is 0.</param>
        /// <returns></returns>
        public IEnumerable<string> GetAndProcessMessages(Func<T, bool> handler , int timeOut = 0, bool deleteMessagesFromQueue = true)
        {
            IEnumerable<Message> messagesForProcessing = new List<Message>();
            List<string> processedMessagesReceiptHandlers = new List<string>();

            do
            {
                messagesForProcessing = this.unitOfWork.GetNextMessages();
                
                foreach (Message message in messagesForProcessing)
                {
                    T messageContent = JsonConvert.DeserializeObject<T>(message.Body);

                   bool result = handler(messageContent);

                    if(result)
                    {
                        processedMessagesReceiptHandlers.Add(message.ReceiptHandle);
                    }

                    Thread.Sleep(timeOut);
                }

                if (messagesForProcessing.Any())
                {
                    if (deleteMessagesFromQueue)
                    {
                        this.DeleteMessagesByReceiptHandler(processedMessagesReceiptHandlers);
                        processedMessagesReceiptHandlers.Clear();
                    }
                }
            }
            while (messagesForProcessing.Any());

            return processedMessagesReceiptHandlers;
        }

        /// <summary>
        /// Reads al the available messages from the SQS queue in batches of 10 until all avalaible messages are red.
        /// Messages that change their status from In Flight to Available during the operation are also red.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns>Returns the retrieved messages from the sqs queue of type T.</returns>
        public IEnumerable<T> GetMessages()
        {
            IEnumerable<Message> messagesForProcessing = new List<Message>();
            List<T> messagesForRetrieve = new List<T>();
            List<string> processedMessagesReceiptHandlers = new List<string>();

            do
            {
                messagesForProcessing = this.unitOfWork.GetNextMessages();

                foreach (Message message in messagesForProcessing)
                {
                    T messageContent = JsonConvert.DeserializeObject<T>(message.Body);

                    messagesForRetrieve.Add(messageContent);
                }
            }
            while (messagesForProcessing.Any());

            return messagesForRetrieve;
        }

        /// <summary>
        /// Checks if the queue has messages or it is empty
        /// </summary>
        /// <returns>True if the queue contains any messages, false otherwise.</returns>
        public bool HasAnyMessagesInQueue()
        {
            bool hasAnyMessagesInQueue = this.unitOfWork.HasAnyMessagesInQueue();
            return hasAnyMessagesInQueue;
        }


        /// <summary>
        /// Removes messages from the queue after processing.
        /// </summary>
        /// <param name="receiptHandlers">The receiptHandlers received when a messages is red from the sqs queue.</param>
        /// <returns>The number of successful deleted messages and the number of messages for which the delete operation failed.</returns>
        public DeleteMessagesResponseModel<string> DeleteMessagesByReceiptHandler(IEnumerable<string> receiptHandlers)
        {
            DeleteMessagesResponseModel<string> deleteMessagesBatchGlobalResponse = new DeleteMessagesResponseModel<string>();

            IEnumerable<string> receiptHandlersBatch = null;

            int skipNumber = 0;
            int batchSize = 10;

            do {

                receiptHandlersBatch = receiptHandlers.Skip(skipNumber).Take(batchSize);

                DeleteMessagesResponseModel<string> deleteMessagesBatchResponse = this.unitOfWork.DeleteMessages(receiptHandlersBatch);

                deleteMessagesBatchGlobalResponse.Successful.AddRange(deleteMessagesBatchResponse.Successful);
                deleteMessagesBatchGlobalResponse.Failed.AddRange(deleteMessagesBatchResponse.Failed);

                skipNumber += batchSize;
            }
            while (skipNumber < receiptHandlers.Count());

             if (deleteMessagesBatchGlobalResponse.Failed.Count > 0)
             {
                 skipNumber = 0;

                 do 
                 {
                     receiptHandlersBatch = deleteMessagesBatchGlobalResponse.Failed.Take(batchSize);

                     foreach (string receiptHandler in receiptHandlersBatch)
                     {
                         deleteMessagesBatchGlobalResponse.Failed.Remove(receiptHandler);
                     }

                     DeleteMessagesResponseModel<string> deleteMessagesBatchResponse = this.unitOfWork.DeleteMessages(receiptHandlersBatch);

                     deleteMessagesBatchGlobalResponse.Successful.AddRange(deleteMessagesBatchResponse.Successful);
                     deleteMessagesBatchGlobalResponse.Failed.AddRange(deleteMessagesBatchResponse.Failed);

                 }
                 while( deleteMessagesBatchGlobalResponse.Failed.Count > 0 );
             }

            return deleteMessagesBatchGlobalResponse; 
        }

    }
}
