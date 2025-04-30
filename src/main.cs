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

if (requestApiKey == 18)
{
    byte[] buffer = new byte[1024];
    var response = buffer.AsSpan();
    int offset = 4; // reserve 32-bits for message size

    BinaryPrimitives.WriteInt32BigEndian(response[offset..], correlationId);
    offset += 4;

    short errorCode = (short)((requestApiVersion < 0 || requestApiVersion > 4) ? 35 : 0);
    BinaryPrimitives.WriteInt16BigEndian(response[offset..], errorCode);
    offset += 2;

    if (errorCode == 0)
    {
        byte apiKeysCount = 2;
        short apiKey = 18;
        short minVer = 0;
        short maxVer = 4;
        int throttleTimeMs = 0;
        response[offset++] = apiKeysCount;
        BinaryPrimitives.WriteInt16BigEndian(response[offset..], apiKey);
        offset += 2;
        BinaryPrimitives.WriteInt16BigEndian(response[offset..], minVer);
        offset += 2;
        BinaryPrimitives.WriteInt16BigEndian(response[offset..], maxVer);
        offset += 2;
        response[offset++] = 0; // reserved for tagged fields
        BinaryPrimitives.WriteInt32BigEndian(response[offset..], throttleTimeMs);
        offset += 4;
        response[offset++] = 0; // reserved for tagged fields
    }

    // the "message size" field itself does not count for the total size on the response
    int messageSize = offset - 4;
    BinaryPrimitives.WriteInt32BigEndian(response, messageSize);
    client.Send(response[0..offset]);
}

client.Close();
