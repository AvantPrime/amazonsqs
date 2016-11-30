using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using Amazon;
using Amazon.SQS;

namespace AmazonSqs.Tests {
    [TestClass]
    public class ObjectQueueTests {
        private static ObjectQueue _queue;

        [ClassInitialize]
        public static void OpenQueue(TestContext testContext) {
            _queue = new ObjectQueue(
                ConfigurationManager.AppSettings["AWSAccessKey"],
                ConfigurationManager.AppSettings["AWSSecretKey"],
				RegionEndpoint.EUWest1,
				"AmazonSqs-ObjectQueue-UnitTests"
            );
        }

		[ClassCleanup]
		public static void CleanupQueue()
		{
			_queue.DeleteQueue();
		}

		[TestMethod]
        public void CanQueueOneObject() {
            _queue.Enqueue(new TestObject() {
                Id = 1,
                Name = "Object 1",
                IgnoredProperty = "Not available!"
            });
        }

        [TestMethod]
        public void CanEnqueueGenericList() {
            _queue.Enqueue(new List<string>(new[] { "hello", "world", "how", "are", "you" }));
        }

        [TestMethod]
        public void CanQueueTwoObjects() {
            _queue.Enqueue(new TestObject() {
                Id = 2,
                Name = "Object 2"
            });

            _queue.Enqueue(new TestObject() {
                Id = 3,
                Name = "Object 3",
                NestedObject = new TestObject() {
                    Id = 31,
                    Name = "Object 3_1"
                }
            });
        }

        [TestMethod]
        [ExpectedException(typeof(QueueException))]
        public void CircularReferenceThrowsQueueException() {
            TestObject o1 = new TestObject() {
                Id = 4,
                Name = "c1"
            };
            TestObject o2 = new TestObject() {
                Id = 5,
                Name = "c2"
            };

            o1.NestedObject = o2;
            o2.NestedObject = o1;

            _queue.Enqueue(o1);
        }

        [TestMethod]
        [ExpectedException(typeof(QueueException))]
        public void MessageOver256KThrowsQueueException() {
            string bigstring = new string('0', 263168);

            TestObject toobig = new TestObject() {
                Id = 6,
                Name = bigstring
            };

            _queue.Enqueue(toobig);
        }

	    //[TestMethod]
	    //public void CheckIfQueueExists()
	    //{
		   // var queueName = Guid.NewGuid().ToString();

		   // using (var client = new AmazonSQSClient(
			  //  ConfigurationManager.AppSettings["AWSAccessKey"],
			  //  ConfigurationManager.AppSettings["AWSSecretKey"],
			  //  RegionEndpoint.EUWest1))
		   // {
			  //  Assert.IsFalse(ObjectQueue.QueueExists(queueName, client));

			  //  using (var queue = new ObjectQueue(
				 //   ConfigurationManager.AppSettings["AWSAccessKey"],
				 //   ConfigurationManager.AppSettings["AWSSecretKey"],
				 //   RegionEndpoint.EUWest1,
				 //   queueName
			  //  ))
			  //  {

				 //   Assert.IsTrue(ObjectQueue.QueueExists(queueName, client));
				 //   queue.DeleteQueue();
				 //   Assert.IsFalse(ObjectQueue.QueueExists(queueName, client));
			  //  }
		   // }
	    //}
    }
}
