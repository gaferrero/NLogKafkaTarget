using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;

namespace NlogKafkaTarget
{
	[Target("KafkaWrapper")]
	public sealed class KafkaTarget : TargetWithLayout
	{
		public KafkaTarget()
		{
			this.Host = "localhost";
			this.Topic = "Test";
		}

		[RequiredParameter]
		public string Host { get; set; }

		[RequiredParameter]
		public string Topic { get; set; }


		protected override void Write(LogEventInfo logEvent)
		{			
			string logMessage = this.Layout.Render(logEvent);

			SendTheMessageToRemoteHost(logMessage);
		}
		

		private void SendTheMessageToRemoteHost(string message)
		{
			 
			var uri = new Uri(Host);


			var options = new KafkaOptions(uri);
			var msg = new Message(message);

			var router = new BrokerRouter(options);
			var client = new Producer(router);
			client.SendMessageAsync(Topic, new List<Message> { msg }).Wait();

		}
	}
}
