using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Socket client = server.AcceptSocket();

byte[] request = new byte[1024];
int requestSize = client.Receive(request);

int requestMessageSize = BinaryPrimitives.ReadInt32BigEndian(request.AsSpan()[0..4]);
short requestApiKey = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan()[4..6]);
short requestApiVersion = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan()[6..8]);
int correlationId = BinaryPrimitives.ReadInt32BigEndian(request.AsSpan()[8..12]);

int messageSize = 0;
client.Send(BitConverter.GetBytes(messageSize).Reverse().ToArray());
client.Send(BitConverter.GetBytes(correlationId).Reverse().ToArray());
client.Close();
