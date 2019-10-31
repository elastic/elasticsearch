package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class EncryptedBlobMetadata implements ToXContentObject {

    private static ConstructingObjectParser<EncryptedBlobMetadata, Void> PARSER =
            new ConstructingObjectParser<>("encrypted_blob_metadata", false,
                    args -> {
                        @SuppressWarnings("unchecked") Integer version = (Integer) args[0];
                        @SuppressWarnings("unchecked") String wrappedDataEncryptionKey = (String) args[1];
                        @SuppressWarnings("unchecked") Integer maximumPacketSizeInBytes = (Integer) args[2];
                        @SuppressWarnings("unchecked") Integer authenticationTagSizeInBytes = (Integer) args[3];
                        @SuppressWarnings("unchecked") List<PacketInfo> packetsInfo = (List<PacketInfo>) args[4];
                        return new EncryptedBlobMetadata(version, Base64.getDecoder().decode(wrappedDataEncryptionKey),
                                maximumPacketSizeInBytes, authenticationTagSizeInBytes, packetsInfo);
                    }
            );

    static {
        PARSER.declareInt(constructorArg(), Fields.VERSION);
        PARSER.declareString(constructorArg(), Fields.WRAPPED_DEK);
        PARSER.declareInt(constructorArg(), Fields.MAXIMUM_PACKET_SIZE);
        PARSER.declareInt(constructorArg(), Fields.AUTH_TAG_SIZE);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> PacketInfo.fromXContent(p), Fields.PACKETS_INFO);
    }

    public static EncryptedBlobMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int version;
    private final byte[] wrappedDataEncryptionKey;
    private final int maximumPacketSizeInBytes;
    private final int authenticationTagSizeInBytes;
    private List<PacketInfo> packetsInfo;

    public EncryptedBlobMetadata(int version, byte[] wrappedDataEncryptionKey, int maximumPacketSizeInBytes,
                                 int authenticationTagSizeInBytes, List<PacketInfo> packetsInfo) {
        this.version = version;
        this.wrappedDataEncryptionKey = wrappedDataEncryptionKey;
        this.maximumPacketSizeInBytes = maximumPacketSizeInBytes;
        this.authenticationTagSizeInBytes = authenticationTagSizeInBytes;
        this.packetsInfo = Collections.unmodifiableList(packetsInfo);
    }

    public byte[] getWrappedDataEncryptionKey() {
        return wrappedDataEncryptionKey;
    }

    public int getVersion() {
        return version;
    }

    public int getMaximumPacketSizeInBytes() {
        return maximumPacketSizeInBytes;
    }

    public List<PacketInfo> getPacketsInfo() {
        return packetsInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.VERSION.getPreferredName(), version);
        builder.field(Fields.WRAPPED_DEK.getPreferredName(), Base64.getEncoder().encodeToString(wrappedDataEncryptionKey));
        builder.field(Fields.MAXIMUM_PACKET_SIZE.getPreferredName(), maximumPacketSizeInBytes);
        builder.field(Fields.AUTH_TAG_SIZE.getPreferredName(), authenticationTagSizeInBytes);
        builder.startArray(Fields.PACKETS_INFO.getPreferredName());
        for (PacketInfo packetInfo : packetsInfo) {
            packetInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
        builder.endObject();
        return null;
    }

    private interface Fields {
        ParseField VERSION = new ParseField("version");
        ParseField WRAPPED_DEK = new ParseField("wrappedDataEncryptionKey");
        ParseField MAXIMUM_PACKET_SIZE = new ParseField("maximumPacketSizeInBytes");
        ParseField AUTH_TAG_SIZE = new ParseField("authenticationTagSizeInBytes");
        ParseField PACKETS_INFO = new ParseField("packetsInfo");
    }

    static class PacketInfo implements ToXContentObject {

        private static ConstructingObjectParser<PacketInfo, Void> PARSER =
                new ConstructingObjectParser<>("packet_info", false,
                        args -> {
                            @SuppressWarnings("unchecked") String iv = (String) args[0];
                            @SuppressWarnings("unchecked") String authenticationTag = (String) args[1];
                            @SuppressWarnings("unchecked") Integer sizeInBytes = (Integer) args[2];
                            return new PacketInfo(Base64.getDecoder().decode(iv), Base64.getDecoder().decode(authenticationTag),
                                    sizeInBytes);
                        }
                );

        static {
            PARSER.declareString(constructorArg(), Fields.IV);
            PARSER.declareString(constructorArg(), Fields.AUTHENTICATION_TAG);
            PARSER.declareInt(constructorArg(), Fields.SIZE_IN_BYTES);
        }

        static PacketInfo fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final byte[] iv;
        private final byte[] authenticationTag;
        private final int sizeInBytes;

        PacketInfo(byte[] iv, byte[] authenticationTag, int sizeInBytes) {
            this.iv = iv;
            this.authenticationTag = authenticationTag;
            this.sizeInBytes = sizeInBytes;
        }

        byte[] getIv() {
            return iv;
        }

        byte[] getAuthenticationTag() {
            return authenticationTag;
        }

        int getSizeInBytes() {
            return sizeInBytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PacketInfo that = (PacketInfo) o;
            return sizeInBytes == that.sizeInBytes &&
                    Arrays.equals(iv, that.iv) &&
                    Arrays.equals(authenticationTag, that.authenticationTag);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(sizeInBytes);
            result = 31 * result + Arrays.hashCode(iv);
            result = 31 * result + Arrays.hashCode(authenticationTag);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.IV.getPreferredName(), Base64.getEncoder().encodeToString(iv));
            builder.field(Fields.AUTHENTICATION_TAG.getPreferredName(), Base64.getEncoder().encodeToString(authenticationTag));
            builder.field(Fields.SIZE_IN_BYTES.getPreferredName(), sizeInBytes);
            builder.endObject();
            return null;
        }

        private interface Fields {
            ParseField IV = new ParseField("iv");
            ParseField AUTHENTICATION_TAG = new ParseField("authenticationTag");
            ParseField SIZE_IN_BYTES = new ParseField("sizeInBytes");
        }
    }

}
