using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System.Net;
using SQSDAL.Models;
using System.Configuration;
using Amazon;

namespace SQSDAL.Repositories
{
     class UnitOfWork<T>
    {
        private int userId;
        private AmazonSQSClient sqsClient;
        private string queueName;
        private string queueUrl;

        //public UnitOfWork(int userId)
        //{
        //    this.userId = userId;
        //    this.sqsClient = CreateSQSClient();
        //}

        public UnitOfWork(int userId, string queueName)
        {
            SetBasicSettings(userId, queueName);

            CreateOrGetQueueByName(queueName);
        }

        public UnitOfWork(int userId, string queueName, RegionEndpoint awsRegion)
        {
            if (IsAWSRegionSupported(awsRegion))
            {
                SetBasicSettings(userId, queueName, awsRegion);

                CreateOrGetQueueByName(queueName);
            }
            else
            {
                throw new InvalidOperationException("AWS region not supported!");
            }
        } 

        public UnitOfWork(int userId, string queueName, int visibilityTimeOutInMinutes)
        {
            SetBasicSettings(userId, queueName);

            CreateOrGetQueueByName(queueName);

            SetQueueVisibilityTimeoutAttribute(visibilityTimeOutInMinutes);
        }
        
         public UnitOfWork(int userId, string queueName, int visibilityTimeOutInMinutes, RegionEndpoint awsRegion)
        {
             if(IsAWSRegionSupported(awsRegion)) 
             {
                SetBasicSettings(userId, queueName, awsRegion);

                CreateOrGetQueueByName(queueName);

                SetQueueVisibilityTimeoutAttribute(visibilityTimeOutInMinutes);
             }
             else 
             {
                 throw new InvalidOperationException("AWS region not supported!");
             }
        }

        private void SetBasicSettings(int userId, string queueName)
        {
            this.userId = userId;
            this.sqsClient = CreateSQSClient();
            this.queueName = queueName;
        }

        private void SetBasicSettings(int userId, string queueName, RegionEndpoint region)
        {
            this.userId = userId;
            this.sqsClient = CreateSQSClient(region);
            this.queueName = queueName;
        }

        private void SetQueueVisibilityTimeoutAttribute(int visibilityTimeOutInMinutes)
        {
            Dictionary<string, string> queueAttributes = new Dictionary<string, string>();
            queueAttributes.Add("VisibilityTimeout", (visibilityTimeOutInMinutes * 60).ToString());

            SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest(this.queueUrl, queueAttributes);
            SetQueueAttributesResponse setQueueAttributesResponse = this.sqsClient.SetQueueAttributes(setQueueAttributesRequest);

            if (setQueueAttributesResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException("Queue was created if not existed but unable to set visibility time out!");
            }
        }


        private void CreateOrGetQueueByName(string queueName)
        {
            CreateQueueResponse createQueueResponse = this.sqsClient.CreateQueue(queueName);

            if (createQueueResponse.QueueUrl != null && createQueueResponse.QueueUrl != string.Empty)
            {
                this.queueUrl = createQueueResponse.QueueUrl;
            }
            else
            {
                GetQueueUrlRequest queueUrlRquest = new GetQueueUrlRequest(this.queueName);
                GetQueueUrlResponse queueUrlReposne = this.sqsClient.GetQueueUrl(queueUrlRquest);

                this.queueUrl = queueUrlReposne.QueueUrl;
            }
        }

        private AmazonSQSClient CreateSQSClient()
        {
            RegionEndpoint CurrentAmazonRegion = GetAmazonRegionEndPoint();
            AmazonSQSClient client = new AmazonSQSClient(CurrentAmazonRegion);
            return client;
        }

        private AmazonSQSClient CreateSQSClient(RegionEndpoint region)
        {
            AmazonSQSClient client = new AmazonSQSClient(region);
            return client;
        }

        private static bool IsAWSRegionSupported(RegionEndpoint region)
        {
            bool isAWSRegionSupported = false;

            switch (region.DisplayName)
            {
                case "us-east-1":
                    isAWSRegionSupported = true;
                    break;
                case "us-west-1":
                    isAWSRegionSupported = true;
                    break;
                case "us-west-2":
                    isAWSRegionSupported = true;
                    break;
                case "ap-south-1":
                    isAWSRegionSupported = true;
                    break;
                case "ap-northeast-1":
                    isAWSRegionSupported = true;
                    break;
                case "ap-northeast-2": 
                    isAWSRegionSupported = true;
                    break;
                case "ap-southeast-1":  
                    isAWSRegionSupported = true;
                    break;
                case "ap-southeast-2":
                    isAWSRegionSupported = true;
                    break;
                case "eu-central-1":
                   isAWSRegionSupported = true;
                    break;
                case "eu-west-1":
                    isAWSRegionSupported = true; 
                    break;
                case "sa-east-1":
                    isAWSRegionSupported = true;
                    break;
                default:
                    isAWSRegionSupported = false;
                    break;
            }

            return isAWSRegionSupported;
        }


        protected static RegionEndpoint GetAmazonRegionEndPoint()
        {
            AppSettingsReader appSettingsReader = new AppSettingsReader();
            string amazonRegionFromConfig = appSettingsReader.GetValue("AWSRegion", typeof(string)).ToString();

            RegionEndpoint currentAmazonRegion;

            switch (amazonRegionFromConfig)
            {
                case "us-east-1":
                    currentAmazonRegion = RegionEndpoint.USEast1;
                    break;
                case "us-west-1":
                    currentAmazonRegion = RegionEndpoint.USWest1;
                    break;
                case "us-west-2":
                    currentAmazonRegion = RegionEndpoint.USWest2;
                    break;
                case "ap-south-1":
                    currentAmazonRegion = RegionEndpoint.APSouth1;
                    break;
               case "ap-northeast-1":
                    currentAmazonRegion = RegionEndpoint.APNortheast1;
                    break;
               case "ap-northeast-2":
                    currentAmazonRegion = RegionEndpoint.APNortheast2;
                    break;
               case "ap-southeast-1":
                    currentAmazonRegion = RegionEndpoint.APSoutheast1;
                    break;
               case "ap-southeast-2":
                    currentAmazonRegion = RegionEndpoint.APSoutheast2;
                    break;
               case "eu-central-1":
                    currentAmazonRegion = RegionEndpoint.EUCentral1;
                    break;
               case "eu-west-1":
                    currentAmazonRegion = RegionEndpoint.EUWest1;
                    break;
                case "sa-east-1":
                    currentAmazonRegion = RegionEndpoint.SAEast1;
                    break;
                default:
                    throw new ArgumentException("Unknown region for amazon services in config file.");
            }

            return currentAmazonRegion;
        }

        public int UserID
        {
            get { return this.userId; }
        }


        public  List<string> GetAvailableQueues()
        {
            ListQueuesRequest listQueueRequest = new ListQueuesRequest();
            ListQueuesResponse listQueueResponse = this.sqsClient.ListQueues(listQueueRequest);
            return listQueueResponse.QueueUrls;
        }

        public bool Save(T message)
        {
            SendMessageRequest sendMessageRequest = new SendMessageRequest( this.queueUrl , JsonConvert.SerializeObject(message));
            SendMessageResponse sendMessageResponse = this.sqsClient.SendMessage(sendMessageRequest);

            return true;
        }

        public SendMessageResponseModel<T> Save(IEnumerable<T> messages)
        {
            List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntries = new List<SendMessageBatchRequestEntry>();
            int i = 0;

            foreach( T message in messages )
            {
                i++;
                SendMessageBatchRequestEntry sendMessageBatchRequestEntry = new SendMessageBatchRequestEntry(i.ToString(), JsonConvert.SerializeObject(message));
                sendMessageBatchRequestEntries.Add(sendMessageBatchRequestEntry);
            }

            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(this.queueUrl, sendMessageBatchRequestEntries);
            SendMessageBatchResponse sendMessageBatchResponse = this.sqsClient.SendMessageBatch(sendMessageBatchRequest);

            SendMessageResponseModel<T> sendMessageBatchResponseModel = ConstructSendResponseModel<T>(sendMessageBatchRequestEntries, sendMessageBatchResponse);

            return sendMessageBatchResponseModel;
        }

        private  SendMessageResponseModel<T> ConstructSendResponseModel<T>(List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntries, SendMessageBatchResponse sendMessageBatchResponse)
        {
            SendMessageResponseModel<T> sendMessageBatchResponseModel = new SendMessageResponseModel<T>();

            sendMessageBatchResponseModel.SendMessagesSuccessfully
                .AddRange(sendMessageBatchRequestEntries.Where(entry => sendMessageBatchResponse.Successful
                    .Select(re => re.Id).Contains(entry.Id)).Select(entry => JsonConvert.DeserializeObject<T>(entry.MessageBody)));

            sendMessageBatchResponseModel.UnprocessedMessages
                .AddRange(sendMessageBatchRequestEntries.Where(entry => sendMessageBatchResponse.Failed
                    .Select(re => re.Id).Contains(entry.Id)).Select(entry => JsonConvert.DeserializeObject<T>(entry.MessageBody)));
            return sendMessageBatchResponseModel;
        }


        public IEnumerable<Message> GetNextMessages()
        {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl);
            receiveMessageRequest.MaxNumberOfMessages = 10;

            ReceiveMessageResponse receiveMessageResponse = this.sqsClient.ReceiveMessage(receiveMessageRequest);
            
            if (receiveMessageResponse.HttpStatusCode == HttpStatusCode.OK) 
            {
                return receiveMessageResponse.Messages;
            }

            return new List<Message>();
        }

        public bool HasAnyMessagesInQueue()
        {
            GetQueueAttributesRequest request = new GetQueueAttributesRequest(this.queueUrl, new List<string> () {"ApproximateNumberOfMessages"} );
            GetQueueAttributesResponse response = this.sqsClient.GetQueueAttributes(request);

            bool hasAnyMessagesInQueue = response.ApproximateNumberOfMessages > 0;

            return hasAnyMessagesInQueue;
        }

        public bool DeleteMessage(string receiptHandle)
        {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(this.queueUrl, receiptHandle);
            DeleteMessageResponse deleteMessageResponse = this.sqsClient.DeleteMessage(deleteMessageRequest);

            if (deleteMessageResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                return true;
            }

            return false;
        }

        public DeleteMessagesResponseModel<string> DeleteMessages(IEnumerable<string> receiptHandles)
        {
            List<DeleteMessageBatchRequestEntry> deleteMessageBatchEntries = new List<DeleteMessageBatchRequestEntry>();
            int i = 0;

            foreach(string receiptHandle in receiptHandles) 
            {
                i++;
                deleteMessageBatchEntries.Add( new DeleteMessageBatchRequestEntry() { Id = i.ToString() , ReceiptHandle = receiptHandle });
            }

            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(this.queueUrl, deleteMessageBatchEntries);

            DeleteMessageBatchResponse deleteMessageBatchResponse = sqsClient.DeleteMessageBatch(deleteMessageBatchRequest);

            DeleteMessagesResponseModel<string> deleteMessagesResponseModel = ConstructDeleteBatchResponseModel(deleteMessageBatchEntries, deleteMessageBatchResponse);

            return deleteMessagesResponseModel;
        }

        private DeleteMessagesResponseModel<string> ConstructDeleteBatchResponseModel(List<DeleteMessageBatchRequestEntry> deleteMessageBatchEntries, DeleteMessageBatchResponse deleteMessageBatchResponse)
        {
            DeleteMessagesResponseModel<string> deleteMessageBatchResponseModel = new DeleteMessagesResponseModel<string>();

            deleteMessageBatchResponseModel.Successful
                .AddRange(deleteMessageBatchEntries.Where(entry => deleteMessageBatchResponse.Successful
                    .Select(re => re.Id).Contains(entry.Id)).Select(entry => entry.ReceiptHandle));

            deleteMessageBatchResponseModel.Failed
                .AddRange(deleteMessageBatchEntries.Where(entry => deleteMessageBatchResponse.Failed
                    .Select(re => re.Id).Contains(entry.Id)).Select(entry => entry.ReceiptHandle));

            return deleteMessageBatchResponseModel;
        }
    }
}
