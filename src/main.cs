using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

internal class Program
{
    private static void Main(string[] args)
    {
        TcpListener server = new TcpListener(IPAddress.Any, 9092);
        server.Start();

        while (true)
        {
            Socket client = server.AcceptSocket();
            _ = Task.Run(() => HandleClient(client));
        }
    }

    private static void HandleClient(Socket client)
    {
        byte[] requestSizeBuffer = new byte[4];

        while (true)
        {
            client.Receive(requestSizeBuffer);

            int requestMessageSize = BinaryPrimitives.ReadInt32BigEndian(requestSizeBuffer);

            byte[] request = new byte[requestMessageSize];
            client.Receive(request);

            RequestHeader header = RequestHeader.Parse(request);

            Response response;
            if (header.ApiKey == (short)ApiKey.APIVersions)
            {
                if (header.ApiVersion < 0 || header.ApiVersion > 4)
                {
                    response = new ErrorResponse(header, ErrorCode.UNSUPPORTED_VERSION);
                }
                else
                {
                    response = new ApiKeysResponse(header);
                }
            }
            else
            {
                response = new ErrorResponse(header, ErrorCode.INVALID_REQUEST);
            }

            byte[] responseSize = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(responseSize, (int)response.Length);
            client.Send(responseSize);
            client.Send(response.ToArray());
        }
    }
}

internal enum ErrorCode
{
    NONE = 0,
    UNSUPPORTED_VERSION = 35,
    INVALID_REQUEST = 42,
}

internal class RequestHeader
{
    public int MessageSize;
    public short ApiKey;
    public short ApiVersion;
    public int CorrelationId;

    public static RequestHeader Parse(byte[] request)
    {
        return new()
        {
            MessageSize = request.Length,
            ApiKey = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan()[0..]),
            ApiVersion = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan()[2..]),
            CorrelationId = BinaryPrimitives.ReadInt32BigEndian(request.AsSpan()[4..])
        };
    }
}

internal abstract class Response
{
    MemoryStream memoryStream;
    BinaryWriter writer;

    protected Response()
    {
        memoryStream = new MemoryStream();
        writer = new(memoryStream);
    }

    public long Length { get => memoryStream.Length; }

    protected void Write(byte x) => writer.Write(x);

    protected void Write(short x) => writer.Write(BinaryPrimitives.ReverseEndianness(x));

    protected void Write(int x) => writer.Write(BinaryPrimitives.ReverseEndianness(x));

    public byte[] ToArray()
    {
        memoryStream.Position = 0;
        return memoryStream.ToArray();
    }
}

internal class ErrorResponse : Response
{
    public ErrorResponse(RequestHeader header, ErrorCode errorCode)
    {
        Write(header.CorrelationId);
        Write((short)errorCode);
    }
}

internal enum ApiKey
{
    APIVersions = 18,
    DescribeTopicPartitions = 75,
}

internal class ApiKeysResponse : Response
{
    internal record ApiVersion(ApiKey Key, short MinVer, short MaxVer);

    static readonly ApiVersion[] versions =
    {
        new (ApiKey.APIVersions, 0, 4),
        new (ApiKey.DescribeTopicPartitions, 0, 0),
    };

    public ApiKeysResponse(RequestHeader header)
    {
        short errorCode = (short)ErrorCode.NONE;
        byte apiKeysCount = (byte)(versions.Length + 1);
        int throttleTimeMs = 0;
        Write(header.CorrelationId);
        Write(errorCode);
        Write(apiKeysCount);
        foreach (ApiVersion version in versions)
        {
            Write((short)version.Key);
            Write(version.MinVer);
            Write(version.MaxVer);
            Write((byte)0); // reserved for tagged fields
        }
        Write(throttleTimeMs);
        Write((byte)0); // reserved for tagged fields
    }
}