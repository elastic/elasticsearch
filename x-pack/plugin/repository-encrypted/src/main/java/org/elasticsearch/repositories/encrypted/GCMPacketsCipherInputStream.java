package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

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

import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * This is obviously NOT thread-safe.
 */
public class GCMPacketsCipherInputStream extends FilterInputStream {

    static class GCMPacketsWithMarkCipherInputStream extends GCMPacketsCipherInputStream {

        private GCMPacketsWithMarkCipherInputStream(InputStream in, SecretKey secretKey, int mode, long packetIndex, int nonce,
                                                    Provider provider) {
            super(in, secretKey, mode, packetIndex, nonce, provider);
        }

        private ByteArrayOutputStream markWriteBuffer = null;
        private ByteArrayInputStream markReadBuffer = new ByteArrayInputStream(new byte[0]);
        private boolean markTriggeredForCurrentPacket = false;
        private long markPacketIndex;
        private int markReadLimit;

        @Override
        public int read() throws IOException {
            ensureOpen();
            // in case this is a reseted stream that has buffered part of the ciphertext
            if (markReadBuffer.available() > 0) {
                return markReadBuffer.read();
            }
            int cipherByte = super.read();
            // if buffering of the ciphertext is required
            if (markTriggeredForCurrentPacket && cipherByte != -1) {
                markWriteBuffer.write(cipherByte);
            }
            return cipherByte;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            ensureOpen();
            // in case this is a reseted stream that has buffered part of the ciphertext
            int bytesReadFromMarkBuffer = markReadBuffer.read(b, off, len);
            if (bytesReadFromMarkBuffer != -1) {
                return bytesReadFromMarkBuffer;
            }
            int cipherBytesCount = super.read(b, off, len);
            // if buffering of the ciphertext is required
            if (markTriggeredForCurrentPacket && cipherBytesCount > 0) {
                markWriteBuffer.write(b, off, cipherBytesCount );
            }
            return cipherBytesCount;
        }

        @Override
        public long skip(long n) throws IOException {
            ensureOpen();
            if (markReadBuffer.available() > 0) {
                return markReadBuffer.skip(n);
            }
            int bytesAvailable = super.available();
            bytesAvailable = Math.min(bytesAvailable, Math.toIntExact(n));
            if (markTriggeredForCurrentPacket) {
                byte[] temp = new byte[bytesAvailable];
                int bytesRead = super.read(temp);
                markWriteBuffer.write(temp);
                return bytesRead;
            } else {
                return super.skip(n);
            }
        }

        @Override
        public int available() throws IOException {
            ensureOpen();
            return markReadBuffer.available() + super.available();
        }

        @Override
        public boolean markSupported() {
            return in.markSupported();
        }

        @Override
        public void mark(int readLimit) {
            markTriggeredForCurrentPacket = true;
            markWriteBuffer = new ByteArrayOutputStream();
            if (markReadBuffer.available() > 0) {
                markReadBuffer.mark(Integer.MAX_VALUE);
                try {
                    markReadBuffer.transferTo(markWriteBuffer);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                markReadBuffer.reset();
            }
            markPacketIndex = getPacketIndex();
            markReadLimit = readLimit;
        }

        @Override
        public void reset() throws IOException {
            if (markWriteBuffer == null) {
                throw new IOException("mark not called");
            }
            if (markPacketIndex > getPacketIndex()) {
                throw new IllegalStateException();
            }
            // mark triggered before the packet boundary has been read over
            if (false == markTriggeredForCurrentPacket) {
                // reset underlying input stream to packet boundary
                in.reset();
                // set packet index for the next packet and clear any transitory state of any inside of packet processing
                setPacketIndex(markPacketIndex);
            }
            if (markPacketIndex != getPacketIndex()) {
                throw new IllegalStateException();
            }
            // make any cached ciphertext available to read
            markReadBuffer = new ByteArrayInputStream(markWriteBuffer.toByteArray());
        }

        @Override
        void readAtTheStartOfPacketHandler() {
            if (markTriggeredForCurrentPacket) {
                markTriggeredForCurrentPacket = false;
                // mark the underlying stream at the start of the packet
                in.mark(markReadLimit);
            }
        }
    }

    private static final int GCM_TAG_SIZE_IN_BYTES = 16;
    private static final int GCM_IV_SIZE_IN_BYTES = 12;
    private static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";

    static final int PACKET_SIZE_IN_BYTES = 4096;
    static final int READ_BUFFER_SIZE_IN_BYTES = 512;

    private boolean done = false;
    private boolean closed = false;
    private final SecretKey secretKey;
    private final int mode;
    private final Provider provider;

    private Cipher packetCipher;
    private long packetIndex;
    private final ByteBuffer packetIV = ByteBuffer.allocate(GCM_IV_SIZE_IN_BYTES);
    // how much to read from the underlying stream before finishing the current packet and starting the next one
    private int stillToReadInPacket;
    private final int packetSizeInBytes;

    private final byte[] inputByteBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES];
    private final byte[] processedByteBuffer = new byte[READ_BUFFER_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES];
    private InputStream processedInputStream = InputStream.nullInputStream();
    private int bytesBufferedInsideTheCipher = 0;

    static GCMPacketsCipherInputStream getEncryptor(InputStream in, SecretKey secretKey, int nonce, Provider provider) {
        return new GCMPacketsWithMarkCipherInputStream(in, secretKey, ENCRYPT_MODE, 0, nonce, provider);
    }

    static GCMPacketsCipherInputStream getDecryptor(InputStream in, SecretKey secretKey, int nonce, Provider provider) {
        return new GCMPacketsWithMarkCipherInputStream(in, secretKey, DECRYPT_MODE, 0, nonce, provider);
    }

    public static GCMPacketsCipherInputStream getEncryptor(InputStream in, SecretKey secretKey, int nonce) {
        return getEncryptor(in, secretKey, nonce, new BouncyCastleFipsProvider());
    }

    public static GCMPacketsCipherInputStream getDecryptor(InputStream in, SecretKey secretKey, int nonce) {
        return getDecryptor(in, secretKey, nonce, new BouncyCastleFipsProvider());
    }

    public static int getEncryptionSizeFromPlainSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException();
        }
        return (size / PACKET_SIZE_IN_BYTES) * (PACKET_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES)
                + (size % PACKET_SIZE_IN_BYTES) + GCM_TAG_SIZE_IN_BYTES;
    }

    public static int getDecryptionSizeFromCipherSize(int size) {
        if (size < GCM_TAG_SIZE_IN_BYTES) {
            throw new IllegalArgumentException();
        }
        return (size / (PACKET_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES)) * PACKET_SIZE_IN_BYTES
                + (size % (PACKET_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES)) - GCM_TAG_SIZE_IN_BYTES;
    }

    private GCMPacketsCipherInputStream(InputStream in, SecretKey secretKey, int mode, long packetIndex, int nonce, Provider provider) {
        super(in);
        this.secretKey = secretKey;
        this.mode = mode;
        this.packetIndex = packetIndex;
        this.provider = provider;
        // the first 8 bytes of the IV for packet encryption are the index of the packet
        packetIV.putLong(packetIndex);
        // the last 4 bytes of the IV for packet encryption are all equal (randomly generated)
        packetIV.putInt(nonce);
        if (mode == ENCRYPT_MODE) {
            packetSizeInBytes = PACKET_SIZE_IN_BYTES;
        } else {
            packetSizeInBytes = PACKET_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES;
        }
        stillToReadInPacket = packetSizeInBytes;
    }

    private void reinitPacketCipher() throws GeneralSecurityException {
        Cipher cipher;
        if (provider != null) {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_SCHEME, provider);
        } else {
            cipher = Cipher.getInstance(GCM_ENCRYPTION_SCHEME);
        }
        // construct IV and increment packet index
        packetIV.putLong(0, packetIndex++);
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, packetIV.array());
        cipher.init(mode, secretKey, gcmParameterSpec);
        packetCipher = cipher;
        // the new cipher has no bytes buffered inside
        bytesBufferedInsideTheCipher = 0;
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
        // starting to read a new packet
        if (stillToReadInPacket == packetSizeInBytes) {
            // reinit cipher for this following packet
            reinitPacketCipher();
            // call handler to notify subclasses that the processing of a new packet has started
            readAtTheStartOfPacketHandler();
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
        // the "if" here is just an "optimization"
        if (bytesProcessed != 0) {
            processedInputStream = new ByteArrayInputStream(processedByteBuffer, 0, bytesProcessed);
        }
        return bytesProcessed;
    }

    @Override
    public int read() throws IOException {
        while (processedInputStream.available() <= 0) {
            try {
                if (readAndProcess() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        return processedInputStream.read();
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        while (processedInputStream.available() <= 0) {
            try {
                if (readAndProcess() == -1) {
                    return -1;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }
        return processedInputStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return processedInputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return processedInputStream.available();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        processedInputStream = InputStream.nullInputStream();
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
        return false;
    }

    @Override
    public void mark(int readLimit) {
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    /**
     * Sets the packet index and clears the transitory state from processing of the previous packet
     */
    void setPacketIndex(long packetIndex) {
        processedInputStream = InputStream.nullInputStream();
        stillToReadInPacket = packetSizeInBytes;
        done = false;
        this.packetIndex = packetIndex;
    }

    long getPacketIndex() {
        return packetIndex;
    }

    void readAtTheStartOfPacketHandler() {
    }

    void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }
}
