package org.elasticsearch.repositories.encrypted;

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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class GCMPacketsDecryptorInputStream extends FilterInputStream {

    private final int maxPacketSizeInBytes;
    private final int authenticationTagSizeInBytes;
    private final SecretKey secretKey;
    private final List<EncryptedBlobMetadata.PacketInfo> packetInfoList;
    private final byte[] packetBuffer;

    private int packetIndex;
    private int bufferStartOffset;
    private int bufferEndOffset;
    private boolean closed;

    protected GCMPacketsDecryptorInputStream(InputStream in, SecretKey secretKey, int maxPacketSizeInBytes,
                                             int authenticationTagSizeInBytes, List<EncryptedBlobMetadata.PacketInfo> packetInfoList) {
        super(in);
        this.secretKey = secretKey;
        this.maxPacketSizeInBytes = maxPacketSizeInBytes;
        this.authenticationTagSizeInBytes = authenticationTagSizeInBytes;
        this.packetInfoList = packetInfoList;
        this.packetBuffer = new byte[maxPacketSizeInBytes + authenticationTagSizeInBytes];
        this.packetIndex = 0;
        this.bufferStartOffset = 0;
        this.bufferEndOffset = 0;
        this.closed = false;
    }

    @Override
    public int read() throws IOException {
        if (bufferStartOffset >= bufferEndOffset) {
            bufferEndOffset = readAndDecryptNextPacket();
            if (bufferEndOffset == -1) {
                return -1;
            }
            bufferStartOffset = 0;
        }
        return packetBuffer[bufferStartOffset++];
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (bufferStartOffset >= bufferEndOffset) {
            bufferEndOffset = readAndDecryptNextPacket();
            if (bufferEndOffset == -1) {
                return -1;
            }
            bufferStartOffset = 0;
        }
        int readSize = Math.min(len, bufferEndOffset - bufferStartOffset);
        System.arraycopy(packetBuffer, bufferStartOffset, b, off, readSize);
        bufferStartOffset += readSize;
        return readSize;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n == 0L) {
            return 0L;
        }
        if (bufferStartOffset >= bufferEndOffset) {
            bufferEndOffset = readAndDecryptNextPacket();
            if (bufferEndOffset == -1) {
                return 0;
            }
            bufferStartOffset = 0;
        }
        int skipSize = Math.toIntExact(Math.min(n, bufferEndOffset - bufferStartOffset));
        bufferStartOffset += skipSize;
        return skipSize;
    }

    @Override
    public int available() throws IOException {
        return Math.max(0, bufferEndOffset - bufferStartOffset);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        in.close();
    }

    void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private int readAndDecryptNextPacket() throws IOException {
        if (packetIndex == packetInfoList.size()) {
            return -1;
        }
        EncryptedBlobMetadata.PacketInfo currentPacketInfo = packetInfoList.get(packetIndex++);
        int packetSize = currentPacketInfo.getSizeInBytes();
        if (packetSize > maxPacketSizeInBytes) {
            throw new IllegalArgumentException();
        }
        ensureOpen();
        int bytesRead = in.readNBytes(packetBuffer, 0, packetSize);
        if (bytesRead != packetSize) {
            throw new IllegalArgumentException();
        }
        if (currentPacketInfo.getAuthenticationTag().length != authenticationTagSizeInBytes) {
            throw new IllegalArgumentException();
        }
        System.arraycopy(currentPacketInfo.getAuthenticationTag(), 0, packetBuffer, packetSize, authenticationTagSizeInBytes);
        Cipher packetCipher = getPacketDecryptionCipher(currentPacketInfo.getIv());
        final int bytesDecrypted;
        try {
            // in-place decryption
            bytesDecrypted = packetCipher.doFinal(packetBuffer, 0, packetSize + authenticationTagSizeInBytes, packetBuffer);
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException(e);
        }
        if (bytesDecrypted != packetSize) {
            throw new IllegalStateException();
        }
        return packetSize;
    }

    private Cipher getPacketDecryptionCipher(byte[] packetIv) throws IOException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(EncryptedRepository.GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, packetIv);
        try {
            Cipher packetCipher = Cipher.getInstance(EncryptedRepository.GCM_ENCRYPTION_SCHEME);
            packetCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);
            return packetCipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IOException(e);
        }
    }

}
