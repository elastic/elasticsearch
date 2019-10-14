package org.elasticsearch.repositories.encrypted;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
    private ByteArrayInputStream ciphertextReadBuffer = new ByteArrayInputStream(new byte[0]);
    private int bytesBufferedInsideTheCipher = 0;

    private ByteArrayOutputStream markWriteBuffer = null;
    private ByteArrayInputStream markReadBuffer = new ByteArrayInputStream(new byte[0]);
    private boolean markTriggered = false;
    private long markPacketIndex;
    private int markReadLimit;

    public GCMPacketsEncryptedInputStream(InputStream in, Provider provider, SecretKey secretKey) {
        this(in, provider, secretKey, 0, new SecureRandom().nextInt());
    }

    private GCMPacketsEncryptedInputStream(InputStream in, Provider provider, SecretKey secretKey, long packetIndex, int nonce) {
        super(in);
        cipherSecurityProvider = provider;
        encryptionKey = secretKey;
        runningPacketIndex = packetIndex;
        // the first 8 bytes of the IV for packet encryption are the index of the packet
        runningPacketIV.putLong(packetIndex);
        // the last 4 bytes of the IV for packet encryption are all equal (randomly generated)
        runningPacketIV.putInt(nonce);
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
        // do not read anything more, there is still ciphertext to be consumed
        if (ciphertextReadBuffer.available() > 0) {
            ciphertextReadBuffer.available();
        }
        // the underlying input stream is exhausted
        if (done) {
            return -1;
        }
        if (roomLeftInPacket == PACKET_SIZE_IN_BYTES) {
            readAtTheStartOfPacket();
        }
        int bytesToRead = Math.min(plaintextBuffer.length - bytesBufferedInsideTheCipher, roomLeftInPacket);
        if (bytesToRead <= 0) {
            throw new IllegalStateException();
        }
        int bytesRead = in.read(plaintextBuffer, 0, bytesToRead);
        assert bytesRead != 0 : "read must return at least one byte";
        assert ciphertextReadBuffer.available() == 0 : "there exists ciphertext still to be consumed, but it shouldn't";
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
            bytesBufferedInsideTheCipher -= ciphertextLen;
            bytesBufferedInsideTheCipher += GCM_TAG_SIZE_IN_BYTES;
            if (bytesBufferedInsideTheCipher != 0) {
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
                bytesBufferedInsideTheCipher += (bytesRead - ciphertextLen);
                bytesBufferedInsideTheCipher += GCM_TAG_SIZE_IN_BYTES;
                if (bytesBufferedInsideTheCipher != 0) {
                    throw new IllegalArgumentException();
                }
                // reset the packet size for the next packet
                roomLeftInPacket = PACKET_SIZE_IN_BYTES;
            } else {
                // this is a partial encryption inside the packet
                try {
                    ciphertextLen = packetCipher.update(plaintextBuffer, 0, bytesRead, ciphertextBuffer, 0);
                } catch (ShortBufferException e) {
                    throw new IllegalStateException(e);
                }
                // the cipher might encrypt only part of the plaintext and cache the rest
                bytesBufferedInsideTheCipher += (bytesRead - ciphertextLen);
            }
        }
        ciphertextReadBuffer = new ByteArrayInputStream(ciphertextBuffer, 0, ciphertextLen);
        return ciphertextLen;
    }

    @Override
    public int read() throws IOException {
        // first try read from the buffered bytes after the mark inside the packet
        if (markReadBuffer.available() > 0) {
            return markReadBuffer.read();
        }
        while (ciphertextReadBuffer.available() <= 0) {
            try {
                if (readAndEncrypt() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        int cipherByte = ciphertextReadBuffer.read();
        if (markTriggered && cipherByte != -1) {
            markWriteBuffer.write(cipherByte);
        }
        return cipherByte;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int bytesReadFromMarkBuffer = markReadBuffer.read(b, off, len);
        if (bytesReadFromMarkBuffer != -1) {
            return bytesReadFromMarkBuffer;
        }
        while (ciphertextReadBuffer.available() <= 0) {
            try {
                if( readAndEncrypt() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        int bytesReadFromCipherBuffer = ciphertextReadBuffer.read(b, off, len);
        if (markTriggered && bytesReadFromCipherBuffer != -1) {
            markWriteBuffer.write(b, off, bytesReadFromCipherBuffer);
        }
        return bytesReadFromCipherBuffer;
    }

    @Override
    public long skip(long n) throws IOException {
        if (markReadBuffer.available() > 0) {
            return markReadBuffer.skip(n);
        }
        if (markTriggered) {
            ciphertextReadBuffer.mark(PACKET_SIZE_IN_BYTES);
            int skipAheadBytes = Math.toIntExact(ciphertextReadBuffer.skip(n));
            byte[] temp = new byte[skipAheadBytes];
            ciphertextReadBuffer.read(temp);
            markWriteBuffer.write(temp);
            return skipAheadBytes;
        } else {
            return ciphertextReadBuffer.skip(n);
        }
    }

    @Override
    public int available() throws IOException {
        return markReadBuffer.available() + ciphertextReadBuffer.available();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        ciphertextReadBuffer = new ByteArrayInputStream(new byte[0]);
        in.close();
        // Throw away the unprocessed data and throw no crypto exceptions.
        // Normally the GCM cipher is fully readed before closing, so any authentication
        // exceptions would occur while reading.
        if (false == done) {
            done = true;
            try {
                packetCipher.doFinal();
            } catch (BadPaddingException | IllegalBlockSizeException ex) {
                // Catch exceptions as the rest of the stream is unused.
            }
        }
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void mark(int readLimit) {
        markTriggered = true;
        markWriteBuffer = new ByteArrayOutputStream();
        markPacketIndex = runningPacketIndex;
        markReadLimit = readLimit;
    }

    @Override
    public void reset() throws IOException {
        if (markWriteBuffer == null) {
            throw new IOException();
        }
        if (false == markTriggered) {
            in.reset();
            ciphertextReadBuffer = new ByteArrayInputStream(new byte[0]);
        }
        markReadBuffer = new ByteArrayInputStream(markWriteBuffer.toByteArray());
        runningPacketIndex = markPacketIndex;
    }

    private void readAtTheStartOfPacket() throws GeneralSecurityException {
        // reinit cipher for the next packet
        initCipher();
        if (markTriggered) {
            markTriggered = false;
            in.mark(markReadLimit);
        }
    }
}
