package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// not thread-safe
public class GCMPacketsEncryptorInputStream extends FilterInputStream {

    private final Logger logger =  LogManager.getLogger(getClass());
    private final int maxPacketSizeInBytes;
    private final byte[] packetTrailByteBuffer;
    private final SecretKey secretKey;
    private final IvRandomUniqueGenerator ivGenerator;
    private final List<BlobEncryptionMetadata.PacketInfo> packetInfoList;

    private int bytesRemainingInPacket;
    private byte[] packetIv;
    private Cipher packetCipher;
    private boolean closed;
    private int markPacketIndex;

    protected GCMPacketsEncryptorInputStream(InputStream in, SecretKey secretKey, int maxPacketSizeInBytes) throws IOException {
        super(in);
        this.maxPacketSizeInBytes = maxPacketSizeInBytes;
        this.packetTrailByteBuffer = new byte[EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES];
        this.secretKey = secretKey;
        this.ivGenerator = new IvRandomUniqueGenerator();
        this.packetInfoList = new ArrayList<>();
        this.bytesRemainingInPacket = maxPacketSizeInBytes;
        this.packetIv = ivGenerator.newUniqueIv();
        this.packetCipher = getPacketEncryptionCipher(packetIv);
        this.closed = false;
        this.markPacketIndex = -1;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        ensureOpen();
        int maxReadSize = getReadSize(len);
        int readSize = in.readNBytes(b, off, maxReadSize);
        assert readSize >= 0 : "readNBytes does not return -1 on end-of-stream";
        if (readSize == 0) {
            if (maxReadSize == 0) {
                // 0 bytes were requested
                return 0;
            }
            // end of filtered input stream
            assert maxReadSize > 0;
            assert in.read() == -1 : "readNBytes returned no bytes but it's not the end-of-stream";
            return -1;
        }
        bytesRemainingInPacket -= readSize;
        final int encryptedSize;
        try {
            // in-place encryption
            encryptedSize = packetCipher.update(b, off, readSize, b, off);
        } catch (ShortBufferException e) {
            throw new IllegalStateException(e);
        }
        if (bytesRemainingInPacket == 0 || readSize % EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES != 0) {
            // finalize packet
            final byte[] authenticationTag;
            if (encryptedSize == readSize) {
                try {
                    authenticationTag = packetCipher.doFinal();
                } catch (IllegalBlockSizeException | BadPaddingException e) {
                    throw new IOException(e);
                }
            } else {
                if (readSize - encryptedSize >= EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES) {
                    throw new IllegalStateException();
                }
                int trailAndTagSize = 0;
                try {
                    trailAndTagSize = packetCipher.doFinal(packetTrailByteBuffer, 0);
                } catch (IllegalBlockSizeException | ShortBufferException | BadPaddingException e) {
                    throw new IOException(e);
                }
                if (encryptedSize + trailAndTagSize != readSize + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES) {
                    throw new IllegalStateException();
                }
                // copy the remaining packet trail bytes
                System.arraycopy(packetTrailByteBuffer, 0, b, off + encryptedSize, trailAndTagSize - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES);
                authenticationTag = Arrays.copyOfRange(packetTrailByteBuffer, trailAndTagSize - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES, trailAndTagSize);
            }
            if (authenticationTag.length != EncryptedRepository.GCM_TAG_SIZE_IN_BYTES) {
                throw new IllegalStateException();
            }
            finishPacket(authenticationTag);
            return readSize;
        } else {
            if (encryptedSize != readSize) {
                throw new IllegalStateException();
            }
            return readSize;
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int bytesRead = read(b, 0, 1);
        if (bytesRead == -1) {
            return -1;
        }
        if (bytesRead != 1) {
            throw new IllegalStateException();
        }
        return (int) b[0];
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        in.close();
        if (bytesRemainingInPacket < maxPacketSizeInBytes) {
            // finish last packet
            final byte[] authenticationTag;
            try {
                authenticationTag = packetCipher.doFinal();
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new IOException(e);
            }
            finishPacket(authenticationTag);
        }
    }

    @Override
    public void mark(int readLimit) {
        in.mark(readLimit);
        if (bytesRemainingInPacket < maxPacketSizeInBytes) {
            // finish in-progress packet
            try {
                byte[] authenticationTag = packetCipher.doFinal();
                finishPacket(authenticationTag);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new UncheckedIOException(new IOException(e));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        markPacketIndex = packetInfoList.size();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        in.reset();
        // discard packets after mark point
        packetInfoList.subList(markPacketIndex, packetInfoList.size()).clear();
        // reinstantiate packetCipher
        bytesRemainingInPacket = maxPacketSizeInBytes;
        packetIv = ivGenerator.newUniqueIv();
        packetCipher = getPacketEncryptionCipher(packetIv);
    }

    public List<BlobEncryptionMetadata.PacketInfo> getEncryptionPacketMetadata() {
        if (false == closed) {
            throw new IllegalStateException("Stream must be closed in order to completely assemble the metadata");
        }
        return Collections.unmodifiableList(packetInfoList);
    }

    void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private void finishPacket(byte[] authTag) throws IOException {
        packetInfoList.add(new BlobEncryptionMetadata.PacketInfo(packetIv, authTag, maxPacketSizeInBytes - bytesRemainingInPacket));
        bytesRemainingInPacket = maxPacketSizeInBytes;
        packetIv = ivGenerator.newUniqueIv();
        packetCipher = getPacketEncryptionCipher(packetIv);
    }

    private Cipher getPacketEncryptionCipher(byte[] packetIv) throws IOException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(EncryptedRepository.GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, packetIv);
        try {
            Cipher packetCipher = Cipher.getInstance(EncryptedRepository.GCM_ENCRYPTION_SCHEME);
            packetCipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmParameterSpec);
            return packetCipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IOException(e);
        }
    }

    /**
     * Tries to return a read size value such that it is smaller or equal to the requested {@code len}, does not exceed the remaining
     * space in the current packet and, very important, is a multiple of {@link EncryptedRepository#AES_BLOCK_SIZE_IN_BYTES}. If the
     * requested {@code len} or the remaining space in the current packet are smaller than
     * {@link EncryptedRepository#AES_BLOCK_SIZE_IN_BYTES}, then their minimum is returned.
     *
     * @param len the requested read size
     */
    private int getReadSize(int len) {
        if (bytesRemainingInPacket <= 0) {
            throw new IllegalStateException();
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        int maxReadSize = Math.min(len, bytesRemainingInPacket);
        int readSize = (maxReadSize / EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES) * EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES;
        if (readSize != 0) {
            return readSize;
        }
        assert maxReadSize < EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES;
        if (maxReadSize == len) {
            logger.warn("Reading [" + len + "] bytes, which is less than [" + EncryptedRepository.AES_BLOCK_SIZE_IN_BYTES + "], is terribly inefficient.");
        }
        return maxReadSize;
    }

    static class IvRandomUniqueGenerator {

        private final Map<Long, Set<Integer>> generatedIvs;
        private final SecureRandom secureRandom;

        IvRandomUniqueGenerator() {
            generatedIvs = new HashMap<>();
            secureRandom = new SecureRandom();
        }

        byte[] newUniqueIv() {
            return newUniqueIv(5);
        }

        private byte[] newUniqueIv(int retryCount) {
            if (retryCount <= 0) {
                throw new IllegalStateException("Secure random returns many similar values");
            }
            long part1 = secureRandom.nextLong();
            Set<Integer> part2Set = generatedIvs.computeIfAbsent(part1, k -> new HashSet<>());
            int part2 = secureRandom.nextInt();
            if (false == part2Set.add(part2)) {
                return newUniqueIv(retryCount - 1);
            }
            ByteBuffer uniqueIv = ByteBuffer.allocate(EncryptedRepository.GCM_IV_SIZE_IN_BYTES);
            uniqueIv.putLong(part1);
            uniqueIv.putInt(part2);
            return uniqueIv.array();
        }
    }
}
