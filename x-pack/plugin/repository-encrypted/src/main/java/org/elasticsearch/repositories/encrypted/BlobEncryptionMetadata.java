package org.elasticsearch.repositories.encrypted;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class BlobEncryptionMetadata {

    private static final int FIELDS_BUFFER_SIZE = 5 * Integer.SIZE / Byte.SIZE;

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
        ByteBuffer fieldsByteBuffer = ByteBuffer.allocate(FIELDS_BUFFER_SIZE).order(ByteOrder.BIG_ENDIAN);
        if (FIELDS_BUFFER_SIZE != inputStream.readNBytes(fieldsByteBuffer.array(), 0, FIELDS_BUFFER_SIZE)) {
            throw new IllegalArgumentException();
        }
        this.maxPacketSizeInBytes = fieldsByteBuffer.getInt();
        this.authTagSizeInBytes = fieldsByteBuffer.getInt();
        this.ivSizeInBytes = fieldsByteBuffer.getInt();
        int dataEncryptionKeySizeInBytes = fieldsByteBuffer.getInt();
        int packetsInfoListSize = fieldsByteBuffer.getInt();
        this.dataEncryptionKeyMaterial = new byte[dataEncryptionKeySizeInBytes];
        if (dataEncryptionKeySizeInBytes != inputStream.readNBytes(dataEncryptionKeyMaterial, 0, dataEncryptionKeySizeInBytes)) {
            throw new IllegalArgumentException();
        }
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

    public InputStream toInputStream() {

        final ByteBuffer fieldsByteBuffer = ByteBuffer.allocate(FIELDS_BUFFER_SIZE).order(ByteOrder.BIG_ENDIAN);
        fieldsByteBuffer.putInt(maxPacketSizeInBytes);
        fieldsByteBuffer.putInt(authTagSizeInBytes);
        fieldsByteBuffer.putInt(ivSizeInBytes);
        fieldsByteBuffer.putInt(dataEncryptionKeyMaterial.length);
        fieldsByteBuffer.putInt(packetsInfoList.size());

        return new SequenceInputStream(new ByteArrayInputStream(fieldsByteBuffer.array()),
                new SequenceInputStream(new ByteArrayInputStream(dataEncryptionKeyMaterial),
                        new SequenceInputStream(new Enumeration<InputStream>() {

                            private final Iterator<PacketInfo> packetInfoIterator = packetsInfoList.iterator();

                            @Override
                            public boolean hasMoreElements() {
                                return packetInfoIterator.hasNext();
                            }

                            @Override
                            public InputStream nextElement() {
                                return packetInfoIterator.next().toInputStream();
                            }
                        })));
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
            this.iv = new byte[ivSizeInBytes];
            if (ivSizeInBytes != inputStream.readNBytes(iv, 0, ivSizeInBytes)) {
                throw new IllegalArgumentException();
            }
            this.authTag = new byte[authTagSizeInBytes];
            if (authTagSizeInBytes != inputStream.readNBytes(authTag, 0, authTagSizeInBytes)) {
                throw new IllegalArgumentException();
            }
            this.sizeInBytes = ((inputStream.read() & 0xFF) << 24) |
                    ((inputStream.read() & 0xFF) << 16) |
                    ((inputStream.read() & 0xFF) << 8 ) |
                    ((inputStream.read() & 0xFF) << 0 );
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

        InputStream toInputStream() {
            return new InputStream() {

                private int idx = 0;

                @Override
                public int read() throws IOException {
                    if (idx < iv.length) {
                        return iv[idx++];
                    } else if (idx < iv.length + authTag.length) {
                        return authTag[idx++ - iv.length];
                    } else if (idx < iv.length + authTag.length + 4) {
                        idx++;
                        if (idx == iv.length + authTag.length + 1) {
                            return (byte) (sizeInBytes >>> 24);
                        } else if (idx == iv.length + authTag.length + 2) {
                            return (byte) (sizeInBytes >>> 16);
                        } else if (idx == iv.length + authTag.length + 3) {
                            return (byte) (sizeInBytes >>> 8);
                        } else {
                            return (byte) sizeInBytes;
                        }
                    } else {
                        return -1;
                    }
                }
            };
        }

    }

}
