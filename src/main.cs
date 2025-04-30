using System.Net;
using System.Net.Sockets;

TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Socket client = server.AcceptSocket();

int messageSize = 0;
int correlationId = 7;

client.Send(BitConverter.GetBytes(messageSize).Reverse().ToArray());
client.Send(BitConverter.GetBytes(correlationId).Reverse().ToArray());
client.Close();