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

import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * This is NOT thread-safe.
 */
public class GCMPacketsCipherInputStream extends FilterInputStream {

    private static final int GCM_TAG_SIZE_IN_BYTES = 16;
    private static final int GCM_IV_SIZE_IN_BYTES = 12;
    private static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";

    private static final int PACKET_SIZE_IN_BYTES = 4096;
    private static final int READ_BUFFER_SIZE_IN_BYTES = 512;

    private boolean done = false;
    private boolean closed = false;
    private final Provider provider;
    private final SecretKey secretKey;
    private final int mode;
    private Cipher packetCipher;

    private long packetIndex;
    private final ByteBuffer packetIV = ByteBuffer.allocate(GCM_IV_SIZE_IN_BYTES);
    // how much to read from the underlying stream before finishing the current packet and starting the next one
    private int stillToReadInPacket;
    private int packetSizeInBytes;

    private byte[] inputByteBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES];
    private byte[] processedByteBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES];
    private ByteArrayInputStream processedInputStream = new ByteArrayInputStream(new byte[0]);
    private int bytesBufferedInsideTheCipher = 0;

    private ByteArrayOutputStream markWriteBuffer = null;
    private ByteArrayInputStream markReadBuffer = new ByteArrayInputStream(new byte[0]);
    private boolean markTriggered = false;
    private long markPacketIndex;
    private int markReadLimit;

    public static GCMPacketsCipherInputStream getGCMPacketsEncryptor(InputStream in, Provider provider, SecretKey secretKey) {
        return new GCMPacketsCipherInputStream(in, provider, secretKey, ENCRYPT_MODE, 0, new SecureRandom().nextInt());
    }

    public static GCMPacketsCipherInputStream getGCMPacketsDecryptor(InputStream in, Provider provider, SecretKey secretKey) {
        return new GCMPacketsCipherInputStream(in, provider, secretKey, DECRYPT_MODE, 0, new SecureRandom().nextInt());
    }

    private GCMPacketsCipherInputStream(InputStream in, Provider provider, SecretKey secretKey, int mode, long packetIndex, int nonce) {
        super(in);
        this.provider = provider;
        this.secretKey = secretKey;
        this.mode = mode;
        this.packetIndex = packetIndex;
        // the first 8 bytes of the IV for packet encryption are the index of the packet
        packetIV.putLong(packetIndex);
        // the last 4 bytes of the IV for packet encryption are all equal (randomly generated)
        packetIV.putInt(nonce);
        if (mode == ENCRYPT_MODE) {
            packetSizeInBytes = PACKET_SIZE_IN_BYTES;
        } else {
            packetSizeInBytes = PACKET_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES;
        }
    }

    private void initCipher() throws GeneralSecurityException {
        Cipher cipher;
        if (provider != null) {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_SCHEME, provider);
        } else {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_SCHEME);
        }
        packetIV.putLong(0, packetIndex++);
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, packetIV.array());
        cipher.init(mode, secretKey, gcmParameterSpec);
        packetCipher = cipher;
    }

    private int readAndProcess() throws IOException, GeneralSecurityException {
        // do not read anything more, there are still processed bytes to be consumed
        if (processedInputStream.available() > 0) {
            processedInputStream.available();
        }
        // the underlying input stream is exhausted
        if (done) {
            return -1;
        }
        if (stillToReadInPacket == packetSizeInBytes) {
            readAtTheStartOfPacket();
        }
        int bytesToRead = Math.min(inputByteBuffer.length - bytesBufferedInsideTheCipher, stillToReadInPacket);
        if (bytesToRead <= 0) {
            throw new IllegalStateException();
        }
        int bytesRead = in.read(inputByteBuffer, 0, bytesToRead);
        assert bytesRead != 0 : "read must return at least one byte";
        assert processedInputStream.available() == 0 : "there exists processed still to be consumed, but it shouldn't";
        final int bytesProcessed;
        if (bytesRead == -1) {
            // end of the underlying stream to be encrypted
            done = true;
            try {
                bytesProcessed = packetCipher.doFinal(processedByteBuffer, 0);
            } catch (ShortBufferException e) {
                throw new IllegalStateException();
            }
            // there should be no internally buffered (by the cipher) data remaining after doFinal
            bytesBufferedInsideTheCipher -= bytesProcessed;
            if (mode == ENCRYPT_MODE) {
                bytesBufferedInsideTheCipher += GCM_TAG_SIZE_IN_BYTES;
            } else {
                bytesBufferedInsideTheCipher -= GCM_TAG_SIZE_IN_BYTES;
            }
            if (bytesBufferedInsideTheCipher != 0) {
                throw new IllegalStateException();
            }
        } else {
            stillToReadInPacket -= bytesRead;
            if (stillToReadInPacket < 0) {
                throw new IllegalStateException();
            }
            if (stillToReadInPacket == 0) {
                // this is the last encryption for this packet
                try {
                    bytesProcessed = packetCipher.doFinal(inputByteBuffer, 0, bytesRead, processedByteBuffer, 0);
                } catch (ShortBufferException e) {
                    throw new IllegalStateException(e);
                }
                // there should be no internally buffered (by the cipher) data remaining after doFinal
                bytesBufferedInsideTheCipher += (bytesRead - bytesProcessed);
                if (mode == ENCRYPT_MODE) {
                    bytesBufferedInsideTheCipher += GCM_TAG_SIZE_IN_BYTES;
                } else {
                    bytesBufferedInsideTheCipher -= GCM_TAG_SIZE_IN_BYTES;
                }
                if (bytesBufferedInsideTheCipher != 0) {
                    throw new IllegalArgumentException();
                }
                // reset the packet size for the next packet
                stillToReadInPacket = packetSizeInBytes;
            } else {
                // this is a partial encryption inside the packet
                try {
                    bytesProcessed = packetCipher.update(inputByteBuffer, 0, bytesRead, processedByteBuffer, 0);
                } catch (ShortBufferException e) {
                    throw new IllegalStateException(e);
                }
                // the cipher might encrypt only part of the plaintext and cache the rest
                bytesBufferedInsideTheCipher += (bytesRead - bytesProcessed);
            }
        }
        processedInputStream = new ByteArrayInputStream(processedByteBuffer, 0, bytesProcessed);
        return bytesProcessed;
    }

    @Override
    public int read() throws IOException {
        // first try read from the buffered bytes after the mark inside the packet
        if (markReadBuffer.available() > 0) {
            return markReadBuffer.read();
        }
        while (processedInputStream.available() <= 0) {
            try {
                if (readAndProcess() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        int cipherByte = processedInputStream.read();
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
        while (processedInputStream.available() <= 0) {
            try {
                if( readAndProcess() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        int bytesReadFromCipherBuffer = processedInputStream.read(b, off, len);
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
            processedInputStream.mark(packetSizeInBytes);
            int skipAheadBytes = Math.toIntExact(processedInputStream.skip(n));
            byte[] temp = new byte[skipAheadBytes];
            processedInputStream.read(temp);
            markWriteBuffer.write(temp);
            return skipAheadBytes;
        } else {
            return processedInputStream.skip(n);
        }
    }

    @Override
    public int available() throws IOException {
        return markReadBuffer.available() + processedInputStream.available();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        processedInputStream = new ByteArrayInputStream(new byte[0]);
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
        markPacketIndex = packetIndex;
        markReadLimit = readLimit;
    }

    @Override
    public void reset() throws IOException {
        if (markWriteBuffer == null) {
            throw new IOException();
        }
        if (false == markTriggered) {
            in.reset();
            processedInputStream = new ByteArrayInputStream(new byte[0]);
        }
        markReadBuffer = new ByteArrayInputStream(markWriteBuffer.toByteArray());
        packetIndex = markPacketIndex;
    }

    private void readAtTheStartOfPacket() throws GeneralSecurityException {
        // re init cipher for this following packet
        initCipher();
        if (markTriggered) {
            markTriggered = false;
            // mark the underlying stream at the start of the packet
            in.mark(markReadLimit);
        }
    }
}
