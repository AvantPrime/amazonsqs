using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Script.Serialization;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace AmazonSqs {
    public class ObjectQueue : IDisposable {
	    private readonly string _queueName;
	    private static readonly Lazy<JavaScriptSerializer> JsonSerializer = new Lazy<JavaScriptSerializer>();
        private const int MaxMessageSize = 262144; // 256K
        private readonly IAmazonSQS _client;

		/// <summary>
		/// Gets the queue url.
		/// </summary>
	    public string QueueUrl { get; private set; }

	    /// <summary>
		/// Gets or sets whether this instance has
		/// been disposed.
		/// </summary>
		public bool IsDisposed { get; private set; }

		public ObjectQueue(string awsAccessKey, string awsSecretKey, RegionEndpoint region, string queueName) {
	        _queueName = queueName;
	        _client = new AmazonSQSClient(
                awsAccessKey,
                awsSecretKey,
				region
			);

			if(!QueueExists())
				CreateQueue(queueName);
        }

	    private JavaScriptSerializer Serializer {
            get {
                if (!JsonSerializer.IsValueCreated) {
                    JsonSerializer.Value.MaxJsonLength = MaxMessageSize;
                }
                return JsonSerializer.Value;
            }
        }

		public static bool QueueExists(string queueName, IAmazonSQS client)
		{
			var queueExists = false;
			var lqr = new ListQueuesRequest();
			var queues = client.ListQueues(lqr);

			if (queues.QueueUrls != null)
			{
				foreach (string queue in queues.QueueUrls)
				{
					if (queue.EndsWith(queueName) && queue.Substring(0, queue.Length-queueName.Length).EndsWith("/"))
					{
						queueExists = true;
						break;
					}
				}
			}

			return queueExists;
		}

		public static bool QueueExists(Uri queueUrl, IAmazonSQS client)
		{
			var queueExists = false;
			var lqr = new ListQueuesRequest();
			var queues = client.ListQueues(lqr);
			
			if (queues.QueueUrls != null)
			{
				foreach (string queue in queues.QueueUrls)
				{
					if (new Uri(queue) == queueUrl)
					{
						queueExists = true;
						break;
					}
				}
			}

			return queueExists;
		}

		private bool QueueExists()
        {
	        return QueueExists(_queueName, _client);
        }

        public int GetMessageCount()
        {
            var sqsRequest = new GetQueueAttributesRequest { QueueUrl = QueueUrl };
            sqsRequest.AttributeNames.Add("ApproximateNumberOfMessages");
            var sqsResponse = _client.GetQueueAttributes(sqsRequest);
            return sqsResponse.ApproximateNumberOfMessages;
        }

        public void DeleteMessage(string receiptHandle) {
	        var dmr = new DeleteMessageRequest
	        {
		        QueueUrl = QueueUrl,
		        ReceiptHandle = receiptHandle
	        };

	        this._client.DeleteMessage(dmr);
        }

        public void Enqueue<T>(T submission) where T : new() {
            try {
                SendMessageRequest req = new SendMessageRequest();
                req.QueueUrl = QueueUrl;

                req.MessageBody = Serializer.Serialize(submission);

                _client.SendMessage(req);
            } catch (AmazonSQSException ex) {
                throw new QueueException(
                    "Could not queue request.",
                    ex
                );
            } catch (InvalidOperationException ex) {
                throw new QueueException(
                    "The maximum size of the object graph must not exceed " + Serializer.MaxJsonLength + " bytes.",
                    ex
                );
            } catch (ArgumentException ex) {
                throw new QueueException(
                    "The maximum object depth (" + Serializer.RecursionLimit + ") was reached.",
                    ex
                );
            }
        }

        public ObjectMessage<T> Peek<T>() where T : new() {
            return Next<T>(false);

        }

        public T DequeueOne<T>() where T : new() {
            return Next<T>(true).Object;
        }

        private ObjectMessage<T> Next<T>(bool delete) where T : new() {
	        var rmr = new ReceiveMessageRequest {QueueUrl = QueueUrl};
	        rmr.AttributeNames.Add("SentTimestamp");
            rmr.AttributeNames.Add("ApproximateReceiveCount");
            rmr.AttributeNames.Add("ApproximateFirstReceiveTimestamp");

            var response = _client.ReceiveMessage(rmr);
            if (response.Messages != null && response.Messages.Any())
            {
                ObjectMessage<T> value = new ObjectMessage<T>();
                Message m = response.Messages[0];
                DateTime epochDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                value.Object = Serializer.Deserialize<T>(m.Body);
                value.ReceiptHandle = m.ReceiptHandle;

                foreach (KeyValuePair<string, string> att in m.Attributes) {
                    switch (att.Key) {
                        case "SentTimestamp":
                            value.Sent = epochDate.AddMilliseconds(double.Parse(att.Value));
                            break;
                        case "ApproximateReceiveCount":
                            value.ApproximateReceiveCount = Int32.Parse(att.Value);
                            break;
                        case "ApproximateFirstReceiveTimestamp":
                            value.FirstReceived = epochDate.AddMilliseconds(double.Parse(att.Value));
                            break;
                    }
                }

                if(delete)
                    DeleteMessage(m.ReceiptHandle);

                return value;
            }

            return default(ObjectMessage<T>);
        }

        /// <summary>
        /// Retrieves up to <paramref name="maxMessagesToProcess"/> messages from the queue and deletes the messages.
        /// </summary>
        /// <typeparam name="T">Deserialization type of message. Must be between 1 and 10.</typeparam>
        /// <param name="maxMessagesToProcess">The maximum number of messages to process</param>
        /// <returns>List of Messages retrieved from queue, deserialized to Type <see cref="T"/></returns>
        public List<T> Dequeue<T>(int maxMessagesToProcess = 1) where T : new() {
            if (maxMessagesToProcess < 1 || maxMessagesToProcess > 10) {
                throw new ArgumentOutOfRangeException("maxMessagesToProcess", "maxMessages must be between 1 and 10.");
            }

            var rmr = new ReceiveMessageRequest {QueueUrl = QueueUrl, MaxNumberOfMessages = maxMessagesToProcess};
            List<T> retval = new List<T>();

            var response = _client.ReceiveMessage(rmr);
            if (response.Messages != null && response.Messages.Any()) {
                retval.Capacity = response.Messages.Count;
                foreach (Message m in response.Messages) {
                    T value = Serializer.Deserialize<T>(m.Body);
                    DeleteMessage(m.ReceiptHandle);
                    retval.Add(value);
                }
            }

            return retval;
        }

		/// <summary>
		/// Deletes the queue.
		/// </summary>
		/// <returns></returns>
	    public DeleteQueueResponse DeleteQueue()
	    {
		    return _client.DeleteQueue(new DeleteQueueRequest(QueueUrl));
	    }

		/// <summary>
		/// Create a queue.
		/// </summary>
		/// <param name="queueName">The name of the queue.</param>
		private void CreateQueue(string queueName)
		{
			try
			{
				var response = _client.CreateQueue(queueName);
				
				if (!string.IsNullOrEmpty(response.QueueUrl))
				{
					QueueUrl = response.QueueUrl;
				}
				else
				{
					throw new QueueException("Queue could not be created.");
				}
			}
			catch (Exception ex)
			{
				throw new QueueException("Queue could not be created.", ex);
			}
		}

		#region Implementation of IDisposable

		/// <summary>
		/// Performs application-defined tasks associated with freeing, 
		/// releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);

			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, 
		/// releasing, or resetting unmanaged resources.
		/// </summary>
		private void Dispose(bool disposing)
		{
			if (!IsDisposed)
			{
				if (disposing)
				{
					_client.Dispose();
				}

				IsDisposed = true;
			}
		}

		#endregion
	}
}
