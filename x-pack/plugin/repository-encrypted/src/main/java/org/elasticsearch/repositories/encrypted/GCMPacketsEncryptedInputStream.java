package org.elasticsearch.repositories.encrypted;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.security.SecureRandom;

import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * This is NOT thread-safe.
 */
public class GCMPacketsEncryptedInputStream extends FilterInputStream {

    private static final int GCM_TAG_SIZE_IN_BYTES = 16;
    private static final int GCM_IV_SIZE_IN_BYTES = 12;
    private static final String GCM_ENCRYPTION_MODE = "AES/GCM/NoPadding";

    private static final int PACKET_SIZE_IN_BYTES = 4096;
    private static final int READ_BUFFER_SIZE_IN_BYTES = 512;

    private boolean done = false;
    private boolean closed = false;
    private final Provider cipherSecurityProvider;
    private final SecretKey encryptionKey;
    private Cipher packetCipher;

    private long runningPacketIndex;
    private final ByteBuffer runningPacketIV = ByteBuffer.allocate(GCM_IV_SIZE_IN_BYTES);
    // how much to read from the underlying stream before finishing the current packet and starting the next one
    private int roomLeftInPacket = PACKET_SIZE_IN_BYTES;

    private byte[] plaintextBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES];
    private byte[] ciphertextBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES];
    private int ciphertextStartOffset = 0;
    private int ciphertextEndOffset = 0;
    private int readButNotEncrypted = 0;

    public GCMPacketsEncryptedInputStream(InputStream in, Provider provider, SecretKey secretKey) throws GeneralSecurityException {
        this(in, provider, secretKey, 0, new SecureRandom().nextInt());
    }

    private GCMPacketsEncryptedInputStream(InputStream in, Provider provider, SecretKey secretKey, long packetIndex, int nonce)
            throws GeneralSecurityException {
        super(in);
        cipherSecurityProvider = provider;
        encryptionKey = secretKey;
        runningPacketIndex = packetIndex;
        // the first 8 bytes of the IV for packet encryption are the index of the packet
        runningPacketIV.putLong(packetIndex);
        // the last 4 bytes of the IV for packet encryption are all equal (randomly generated)
        runningPacketIV.putInt(nonce);
        initCipher();
    }

    private void initCipher() throws GeneralSecurityException {
        Cipher cipher;
        if (cipherSecurityProvider != null) {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_MODE, cipherSecurityProvider);
        } else {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_MODE);
        }
        runningPacketIV.putLong(0, runningPacketIndex++);
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, runningPacketIV.array());
        cipher.init(ENCRYPT_MODE, encryptionKey, gcmParameterSpec);
        packetCipher = cipher;
    }

    private int readAndEncrypt() throws IOException, GeneralSecurityException {
        if (ciphertextStartOffset > ciphertextEndOffset) {
            throw new IllegalStateException();
        }
        if (ciphertextStartOffset < ciphertextEndOffset) {
            // do not read anything more, there is still ciphertext to be consumed
            return ciphertextEndOffset - ciphertextStartOffset;
        }
        if (done) {
            return -1;
        }
        int bytesToRead = Math.min(plaintextBuffer.length - readButNotEncrypted, roomLeftInPacket);
        if (bytesToRead <= 0) {
            throw new IllegalStateException();
        }
        int bytesRead = in.read(plaintextBuffer, 0, bytesToRead);
        assert bytesRead != 0 : "read must return at least one byte";
        assert ciphertextEndOffset - ciphertextStartOffset == 0 : "there exists ciphertext still to be consumed, but it shouldn't";
        final int ciphertextLen;
        if (bytesRead == -1) {
            // end of the underlying stream to be encrypted
            done = true;
            try {
                ciphertextLen = packetCipher.doFinal(ciphertextBuffer, 0);
            } catch (ShortBufferException e) {
                throw new IllegalStateException();
            }
            // there should be no internally buffered (by the cipher) data remaining after doFinal
            readButNotEncrypted -= ciphertextLen;
            readButNotEncrypted += GCM_TAG_SIZE_IN_BYTES;
            if (readButNotEncrypted != 0) {
                throw new IllegalStateException();
            }
        } else {
            roomLeftInPacket -= bytesRead;
            if (roomLeftInPacket < 0) {
                throw new IllegalStateException();
            }
            if (roomLeftInPacket == 0) {
                // this is the last encryption for this packet
                try {
                    ciphertextLen = packetCipher.doFinal(plaintextBuffer, 0, bytesRead, ciphertextBuffer, 0);
                } catch (ShortBufferException e) {
                    throw new IllegalStateException(e);
                }
                // there should be no internally buffered (by the cipher) data remaining after doFinal
                readButNotEncrypted += (bytesRead - ciphertextLen);
                readButNotEncrypted += GCM_TAG_SIZE_IN_BYTES;
                if (readButNotEncrypted != 0) {
                    throw new IllegalArgumentException();
                }
                // reset the packet size for the next packet
                roomLeftInPacket = PACKET_SIZE_IN_BYTES;
                // reinit cipher for the next packet
                initCipher();
            } else {
                // this is a partial encryption inside the packet
                try {
                    ciphertextLen = packetCipher.update(plaintextBuffer, 0, bytesRead, ciphertextBuffer, 0);
                } catch (ShortBufferException e) {
                    throw new IllegalStateException(e);
                }
                // the cipher might encrypt only part of the plaintext and cache the rest
                readButNotEncrypted += (bytesRead - ciphertextLen);
            }
        }
        ciphertextStartOffset = 0;
        ciphertextEndOffset = ciphertextLen;
        return ciphertextLen;
    }

    @Override
    public int read() throws IOException {
        while (ciphertextStartOffset >= ciphertextEndOffset) {
            int cipherBytesAvailable = 0;
            try {
                cipherBytesAvailable = readAndEncrypt();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            if (cipherBytesAvailable == -1) {
                return -1;
            }
        }
        return ciphertextBuffer[ciphertextStartOffset++];
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int cipherBytesAvailable = 0;
        while (ciphertextStartOffset >= ciphertextEndOffset) {
            try {
                cipherBytesAvailable = readAndEncrypt();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            if (cipherBytesAvailable == -1) {
                return -1;
            }
        }
        if (len <= 0) {
            return 0;
        }
        assert cipherBytesAvailable == (ciphertextEndOffset - ciphertextStartOffset);
        int cipherBytesRead = Math.min(len, cipherBytesAvailable);
        if (b != null) {
            System.arraycopy(ciphertextBuffer, ciphertextStartOffset, b, off, cipherBytesRead);
        }
        ciphertextStartOffset += cipherBytesRead;
        return cipherBytesRead;
    }

    @Override
    public long skip(long n) throws IOException {
        int cipherBytesAvailable = ciphertextEndOffset - ciphertextStartOffset;
        long cipherBytesSkipped = Math.min(cipherBytesAvailable, n);
        if (n < 0) {
            return 0;
        }
        ciphertextStartOffset = Math.addExact(ciphertextStartOffset, Math.toIntExact(cipherBytesSkipped));
        return cipherBytesSkipped;
    }

    @Override
    public int available() throws IOException {
        return (ciphertextEndOffset - ciphertextStartOffset);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        ciphertextStartOffset = 0;
        ciphertextEndOffset = 0;
        in.close();
        // Throw away the unprocessed data and throw no crypto exceptions.
        // Normally the GCM cipher is fully readed before closing, so any authentication
        // exceptions would occur while reading.
        if (false == done) {
            done = true;
            try {
                packetCipher.doFinal();
            }
            catch (BadPaddingException | IllegalBlockSizeException ex) {
                // Catch exceptions as the rest of the stream is unused.
            }
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}
