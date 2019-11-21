package org.elasticsearch.repositories.encrypted;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class BlobEncryptionMetadata {

    private final int maxPacketSizeInBytes;
    private final int authTagSizeInBytes;
    private final int ivSizeInBytes;
    private final byte[] dataEncryptionKeyMaterial;
    private List<PacketInfo> packetsInfoList;

    public BlobEncryptionMetadata(int maxPacketSizeInBytes, int ivSizeInBytes, int authTagSizeInBytes,
                                  byte[] dataEncryptionKeyMaterial, List<PacketInfo> packetsInfoList) {
        this.maxPacketSizeInBytes = maxPacketSizeInBytes;
        this.ivSizeInBytes = ivSizeInBytes;
        this.authTagSizeInBytes = authTagSizeInBytes;
        this.dataEncryptionKeyMaterial = dataEncryptionKeyMaterial;
        // consistency check of the packet infos
        for (PacketInfo packetInfo : packetsInfoList) {
            if (packetInfo.getSizeInBytes() > maxPacketSizeInBytes) {
                throw new IllegalArgumentException();
            }
            if (packetInfo.getIv().length != ivSizeInBytes) {
                throw new IllegalArgumentException();
            }
            if (packetInfo.getAuthTag().length != authTagSizeInBytes) {
                throw new IllegalArgumentException();
            }
        }
        this.packetsInfoList = Collections.unmodifiableList(packetsInfoList);
    }

    public BlobEncryptionMetadata(InputStream inputStream) throws IOException {
        this.maxPacketSizeInBytes = readInt(inputStream);
        this.authTagSizeInBytes = readInt(inputStream);
        this.ivSizeInBytes = readInt(inputStream);

        int dataEncryptionKeySizeInBytes = readInt(inputStream);
        this.dataEncryptionKeyMaterial = readExactlyNBytes(inputStream, dataEncryptionKeySizeInBytes);

        int packetsInfoListSize = readInt(inputStream);
        List<PacketInfo> packetsInfo = new ArrayList<>(packetsInfoListSize);
        for (int i = 0; i < packetsInfoListSize; i++) {
            PacketInfo packetInfo = new PacketInfo(inputStream, ivSizeInBytes, authTagSizeInBytes);
            // consistency check of the packet infos
            if (packetInfo.getSizeInBytes() > this.maxPacketSizeInBytes) {
                throw new IllegalArgumentException();
            }
            if (packetInfo.getIv().length != this.ivSizeInBytes) {
                throw new IllegalArgumentException();
            }
            if (packetInfo.getAuthTag().length != this.authTagSizeInBytes) {
                throw new IllegalArgumentException();
            }
            packetsInfo.add(packetInfo);
        }
        this.packetsInfoList = Collections.unmodifiableList(packetsInfo);
    }

    public byte[] getDataEncryptionKeyMaterial() {
        return dataEncryptionKeyMaterial;
    }

    public int getMaxPacketSizeInBytes() {
        return maxPacketSizeInBytes;
    }

    public int getAuthTagSizeInBytes() {
        return authTagSizeInBytes;
    }

    public int getIvSizeInBytes() {
        return ivSizeInBytes;
    }

    public List<PacketInfo> getPacketsInfoList() {
        return packetsInfoList;
    }

    public void write(OutputStream out) throws IOException {
        writeInt(out, maxPacketSizeInBytes);
        writeInt(out, authTagSizeInBytes);
        writeInt(out, ivSizeInBytes);

        writeInt(out, dataEncryptionKeyMaterial.length);
        out.write(dataEncryptionKeyMaterial);

        writeInt(out, packetsInfoList.size());
        for (PacketInfo packetInfo : packetsInfoList) {
            packetInfo.write(out);
        }
    }

    private static int readInt(InputStream inputStream) throws IOException {
        return ((inputStream.read() & 0xFF) << 24) |
               ((inputStream.read() & 0xFF) << 16) |
               ((inputStream.read() & 0xFF) << 8 ) |
               ((inputStream.read() & 0xFF) << 0 );
    }

    private static void writeInt(OutputStream out, int val) throws IOException {
        out.write(val >>> 24);
        out.write(val >>> 16);
        out.write(val >>> 8);
        out.write(val);
    }

    private static byte[] readExactlyNBytes(InputStream inputStream, int nBytes) throws IOException {
        byte[] ans = new byte[nBytes];
        if (nBytes != inputStream.readNBytes(ans, 0, nBytes)) {
            throw new IOException("Fewer than [" + nBytes + "] read");
        }
        return ans;
    }

    static class PacketInfo {

        private final byte[] iv;
        private final byte[] authTag;
        private final int sizeInBytes;

        PacketInfo(byte[] iv, byte[] authTag, int sizeInBytes) {
            this.iv = iv;
            this.authTag = authTag;
            this.sizeInBytes = sizeInBytes;
        }

        PacketInfo(InputStream inputStream, int ivSizeInBytes, int authTagSizeInBytes) throws IOException {
            this.iv = readExactlyNBytes(inputStream, ivSizeInBytes);
            this.authTag = readExactlyNBytes(inputStream, authTagSizeInBytes);
            this.sizeInBytes = readInt(inputStream);
        }

        byte[] getIv() {
            return iv;
        }

        byte[] getAuthTag() {
            return authTag;
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
                    Arrays.equals(authTag, that.authTag);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(sizeInBytes);
            result = 31 * result + Arrays.hashCode(iv);
            result = 31 * result + Arrays.hashCode(authTag);
            return result;
        }

        public void write(OutputStream out) throws IOException {
            out.write(iv);
            out.write(authTag);
            writeInt(out, sizeInBytes);
        }

    }

}
