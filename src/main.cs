using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

internal class Program
{

    const string metadataPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    static ClusterMetadata? metadata;

    private static void Main(string[] args)
    {
        if (File.Exists(metadataPath))
        {
            byte[] bytes = File.ReadAllBytes(metadataPath);
            Console.WriteLine($"Read metadata: {bytes.Length} bytes");
            KafkaProtocolReader reader = new KafkaProtocolReader(bytes);
            metadata = new ClusterMetadata(reader);
        }

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
            KafkaProtocolReader reader = new KafkaProtocolReader(request);

            RequestHeader header = new(reader);

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
            else if (header.ApiKey == (short)ApiKey.DescribeTopicPartitions)
            {
                DescribeTopicPartitionsRequest desc = new(header, reader);
                response = new DescribeTopicPartitionsResponse(header, desc, metadata);
            }
            else
            {
                response = new ErrorResponse(header, ErrorCode.INVALID_REQUEST);
            }

            byte[] responseSize = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(responseSize, (int)response.Length);
            // Console.WriteLine(response.Length);
            // Console.WriteLine(BitConverter.ToString(response.ToArray()));
            client.Send(responseSize);
            client.Send(response.ToArray());
        }
    }
}

internal class Topic(byte[] uuid, string name)
{
    public byte[] UUID = uuid;
    public string Name = name;
    public List<Partition> Partitions = new();
}

internal class Partition
{
    public int PartitionIndex;
    public int LeaderID;
    public int LeaderEpoch;
    public int PartitionEpoch;
    public List<int> ReplicaNodes = new();
    public List<int> InSyncReplicaNodes = new();
    public List<int> EligibleLeaderReplicas = new();
    public List<int> LastKnownELR = new();
    public List<int> OfflineReplicas = new();
    public List<int> AddingReplicas = new();
    public List<int> RemovingReplicas = new();
}

internal class ClusterMetadata
{
    public readonly Dictionary<string, Topic> TopicsByUUID = new();
    public readonly Dictionary<string, Topic> TopicsByName = new();

    public ClusterMetadata(KafkaProtocolReader reader)
    {
        while (!reader.AtEnd())
        {
            long BaseOffset = reader.ReadInt64();
            int BatchLength = reader.ReadInt32();
            int PartitionLeaderEpoch = reader.ReadInt32();
            byte MagicByte = reader.ReadByte();
            int CRC = reader.ReadInt32();
            short BatchAttributes = reader.ReadInt16();
            int LastOffsetDelta = reader.ReadInt32();
            long BaseTimestamp = reader.ReadInt64();
            long MaxTimestamp = reader.ReadInt64();
            long ProducerID = reader.ReadInt64();
            short ProducerEpoch = reader.ReadInt16();
            int BaseSequence = reader.ReadInt32();
            int RecordsLength = reader.ReadInt32();
            for (int i = 0; i < RecordsLength; i++)
            {
                int Length = reader.ReadVarInt();
                byte RecordAttributes = reader.ReadByte();
                int TimestampDelta = reader.ReadVarInt();
                int OffsetDelta = reader.ReadVarInt();
                int KeyLength = (int)reader.ReadUVarInt() - 1;
                byte[]? Key = reader.ReadByteArray(KeyLength);
                int ValueLength = reader.ReadVarInt();
                // byte[]? Value = reader.ReadByteArray(ValueLength);
                // Console.WriteLine(Value?.Length);
                // Console.WriteLine(BitConverter.ToString(Value));

                var FrameVersion = reader.ReadByte();
                var Type = reader.ReadByte();

                if (Type == 12) // Feature Level Record
                {
                    var Version = reader.ReadByte();
                    var Name = reader.ReadCompactString();
                    // Console.WriteLine("Feature Level Record - Name: " + Name);
                    var FeatureLevel = reader.ReadInt16();
                    reader.ReadTaggedFields();
                }
                else if (Type == 2) // Topic Record
                {
                    var Version = reader.ReadByte();
                    var TopicName = reader.ReadCompactString();
                    // Console.WriteLine("Topic Record - TopicName: " + TopicName);
                    byte[]? TopicUUID = reader.ReadByteArray(16)!;
                    string strUUID = BitConverter.ToString(TopicUUID);
                    // Console.WriteLine("Topic Record - TopicUUID: " + strUUID);
                    reader.ReadTaggedFields();
                    if (TopicUUID != null)
                    {
                        Topic topic = new(TopicUUID, TopicName);
                        TopicsByUUID[strUUID] = topic;
                        TopicsByName[TopicName] = topic;
                    }
                }
                else if (Type == 3) // Partition Record
                {
                    var Version = reader.ReadByte();
                    Partition partition = new();
                    partition.PartitionIndex = reader.ReadInt32();
                    // Console.WriteLine("Partition Record - ID " + partition.PartitionIndex);
                    string TopicUUID = BitConverter.ToString(reader.ReadByteArray(16)!);
                    // Console.WriteLine("Partition Record - TopicUUID: " + TopicUUID);
                    if (TopicUUID != null && TopicsByUUID.TryGetValue(TopicUUID, out Topic? topic))
                    {
                        topic.Partitions.Add(partition);
                    }
                    var LengthOfReplicaArray = reader.ReadUVarInt() - 1;
                    for (int j = 0; j < LengthOfReplicaArray; j++)
                    {
                        partition.ReplicaNodes.Add(reader.ReadInt32());
                    }
                    var LengthOfInSyncReplicaArray = reader.ReadUVarInt() - 1;
                    for (int j = 0; j < LengthOfInSyncReplicaArray; j++)
                    {
                        partition.InSyncReplicaNodes.Add(reader.ReadInt32());
                    }
                    var LengthOfRemovingReplicasArray = reader.ReadUVarInt() - 1;
                    for (int j = 0; j < LengthOfRemovingReplicasArray; j++)
                    {
                        partition.RemovingReplicas.Add(reader.ReadInt32());
                    }
                    var LengthOfAddingReplicasID = reader.ReadUVarInt() - 1;
                    for (int j = 0; j < LengthOfAddingReplicasID; j++)
                    {
                        partition.AddingReplicas.Add(reader.ReadInt32());
                    }
                    partition.LeaderID = reader.ReadInt32();
                    partition.LeaderEpoch = reader.ReadInt32();
                    partition.PartitionEpoch = reader.ReadInt32();
                    var LengthOfDirectoriesArray = reader.ReadUVarInt() - 1;
                    for (int j = 0; j < LengthOfDirectoriesArray; j++)
                    {
                        byte[]? DirectoryUUID = reader.ReadByteArray(16);
                    }
                    reader.ReadTaggedFields();
                }
                else
                {
                    throw new NotImplementedException("Type: " + Type);
                }
                int HeadersArrayCount = reader.ReadVarInt();
            }
        }
    }
}

internal enum ErrorCode
{
    NONE = 0,
    UNKNOWN_TOPIC_OR_PARTITION = 3,
    UNSUPPORTED_VERSION = 35,
    INVALID_REQUEST = 42,
}

internal class KafkaProtocolReader
{
    int offset = 0;
    readonly byte[] buffer;

    public KafkaProtocolReader(byte[] request)
    {
        buffer = request;
    }

    public long Length { get { return buffer.Length; } }

    public byte ReadByte()
    {
        return buffer[offset++];
    }

    public short ReadInt16()
    {
        short x = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan()[offset..]);
        offset += 2;
        return x;
    }

    public int ReadInt32()
    {
        int x = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan()[offset..]);
        offset += 4;
        return x;
    }

    public long ReadInt64()
    {
        long x = BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan()[offset..]);
        offset += 8;
        return x;
    }

    public int ReadVarInt()
    {
        int value = 0;
        int shift = 0;
        bool continuationBit = true;
        for (int i = 0; i < 5 && continuationBit; i++)
        {
            if (offset >= buffer.Length)
            {
                throw new IndexOutOfRangeException("End of buffer reading varint.");
            }
            continuationBit = (buffer[offset] & 0x80) == 0x80;
            value |= (buffer[offset++] & 0x7f) << shift;
            shift += 7;
        }
        return (value >> 1) ^ -(value & 1);
    }

    internal uint ReadUVarInt()
    {
        uint value = 0;
        int shift = 0;

        bool continuationBit = true;
        for (int i = 0; i < 5 && continuationBit; i++)
        {
            if (offset >= buffer.Length)
            {
                throw new IndexOutOfRangeException("End of buffer reading varint.");
            }
            continuationBit = (buffer[offset] & 0x80) == 0x80;
            value |= (uint)(buffer[offset++] & 0x7f) << shift;
            shift += 7;
        }
        return value;
    }

    public string? ReadNullableString()
    {
        short length = ReadInt16();
        if (length == -1)
        {
            return null;
        }
        string s = Encoding.UTF8.GetString(buffer, offset, length);
        offset += length;
        return s;
    }

    internal void ReadTaggedFields()
    {
        // TODO: Properly handle tagged fields
        ReadByte();
    }

    internal string ReadCompactString()
    {
        int length = (int)ReadUVarInt() - 1;
        string s = Encoding.UTF8.GetString(buffer, offset, length);
        offset += length;
        return s;
    }

    internal bool AtEnd()
    {
        return offset >= buffer.Length;
    }

    internal byte[]? ReadByteArray(int keyLength)
    {
        if (keyLength == 0)
        {
            return null;
        }
        byte[] result = buffer[offset..(offset + keyLength)];
        offset += keyLength;
        return result;
    }
}

internal class RequestHeader
{
    public int MessageSize;
    public short ApiKey;
    public short ApiVersion;
    public int CorrelationId;
    public string? ClientId;

    public RequestHeader(KafkaProtocolReader request)
    {
        MessageSize = (int)request.Length;
        ApiKey = request.ReadInt16();
        ApiVersion = request.ReadInt16();
        CorrelationId = request.ReadInt32();
        ClientId = request.ReadNullableString();
        request.ReadTaggedFields();
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

    protected void Write(string str)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(str);
        WriteUVarInt(bytes.Length + 1);
        writer.Write(bytes);
    }

    protected void Write(byte[] bytes)
    {
        writer.Write(bytes);
    }

    protected void WriteUVarInt(int x)
    {
        while (x >= 0x80)
        {
            Write((byte)((x & 0x7F) | 0x80));
            x >>= 7;
        }
        Write((byte)x);
    }

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
            Write((byte)0); // empty tagged field array
        }
        Write(throttleTimeMs);
        Write((byte)0); // empty tagged field array
    }
}

internal class DescribeTopicPartitionsRequest
{
    public readonly List<string> Topics = new();

    public DescribeTopicPartitionsRequest(RequestHeader header, KafkaProtocolReader reader)
    {
        // Topics Array
        int topicCount = (int)reader.ReadUVarInt() - 1;
        for (int i = 0; i < topicCount; i++)
        {
            Topics.Add(reader.ReadCompactString());
            reader.ReadTaggedFields();
        }

        var ResponsePartitionLimit = reader.ReadInt32();

        // Cursor
        reader.ReadByte(); // TODO: handle nullable fields

        // Tag Buffer
        reader.ReadTaggedFields();
    }
}

internal class DescribeTopicPartitionsResponse : Response
{
    public DescribeTopicPartitionsResponse(RequestHeader header, DescribeTopicPartitionsRequest desc, ClusterMetadata? metadata)
    {
        // Response Header
        Write(header.CorrelationId);
        Write((byte)0); // empty tagged field array

        // Response Body
        int throttleTimeMs = 0;
        Write(throttleTimeMs);
        byte topicCount = (byte)(1 + 1);
        WriteUVarInt(topicCount);

        foreach (string topicName in desc.Topics)
        {
            short errorCode = (short)ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;
            byte[] topicID = new byte[16];
            byte isInternal = 0;
            Topic? topic = null;
            if (metadata != null && metadata.TopicsByName.TryGetValue(topicName, out topic) && topic != null)
            {
                errorCode = (short)ErrorCode.NONE;
                topicID = topic.UUID!;
            }
            Write(errorCode);
            Write(desc.Topics[0]);
            Write(topicID);
            Write(isInternal);
            if (topic != null && topic.Partitions.Count > 0)
            {
                WriteUVarInt(topic.Partitions.Count + 1);
                foreach (Partition p in topic.Partitions)
                {
                    short partitionErrorCode = (short)ErrorCode.NONE;
                    Write(partitionErrorCode);
                    Write(p.PartitionIndex);
                    Write(p.LeaderID);
                    Write(p.LeaderEpoch);
                    WriteUVarInt(p.ReplicaNodes.Count + 1);
                    foreach (int n in p.ReplicaNodes)
                    {
                        Write(n);
                    }
                    WriteUVarInt(p.InSyncReplicaNodes.Count + 1);
                    foreach (int n in p.InSyncReplicaNodes)
                    {
                        Write(n);
                    }
                    WriteUVarInt(p.EligibleLeaderReplicas.Count + 1);
                    foreach (int n in p.EligibleLeaderReplicas)
                    {
                        Write(n);
                    }
                    WriteUVarInt(p.LastKnownELR.Count + 1);
                    foreach (int n in p.LastKnownELR)
                    {
                        Write(n);
                    }
                    WriteUVarInt(p.OfflineReplicas.Count + 1);
                    foreach (int n in p.OfflineReplicas)
                    {
                        Write(n);
                    }
                    Write((byte)0); // empty tagged field array
                }
            }
            else
            {
                Write((byte)1); // empty partitions array (compact)
            }

            int authorizedOperations = 0x00000df8;
            Write(authorizedOperations);
            Write((byte)0); // empty tagged field array
        }

        Write((byte)0xFF); // next cursor (null)
        Write((byte)0); // empty tagged field array
    }
}