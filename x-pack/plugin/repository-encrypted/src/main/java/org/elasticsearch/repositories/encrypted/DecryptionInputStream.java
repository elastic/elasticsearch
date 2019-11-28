package org.elasticsearch.repositories.encrypted;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;
import java.util.Objects;

public class DecryptionInputStream extends ChainInputStream {

    private final InputStream source;
    private final SecretKey secretKey;
    private final int packetLength;
    private final byte[] packet;
    private final byte[] iv;
    private boolean hasNext;

    public DecryptionInputStream(InputStream source, SecretKey secretKey, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        this.packetLength = packetLength;
        this.packet = new byte[packetLength + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES];
        this.iv = new byte[EncryptedRepository.GCM_IV_SIZE_IN_BYTES];
        this.hasNext = true;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark/reset not supported");
    }

    @Override
    boolean hasNext(InputStream currentStream) {
        return hasNext;
    }

    private int decrypt(PrefixInputStream packetInputStream) throws IOException {
        if (packetInputStream.read(iv) != iv.length) {
            throw new IOException("Error while reading the heading IV of the packet");
        }
        int packetLength = packetInputStream.read(packet);
        if (packetLength < EncryptedRepository.GCM_TAG_SIZE_IN_BYTES) {
            throw new IOException("Error while reading the packet");
        }
        Cipher packetCipher = getPacketDecryptionCipher(iv);
        try {
            // in-place decryption
            return packetCipher.doFinal(packet, 0, packetLength, packet);
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException(e);
        }
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

    @Override
    InputStream next(InputStream currentStream) throws IOException {
        if (currentStream != null && currentStream.read() != -1) {
            throw new IllegalStateException("Stream for previous packet has not been fully processed");
        }
        if (false == hasNext(currentStream)) {
            throw new NoSuchElementException();
        }
        PrefixInputStream packetInputStream = new PrefixInputStream(source,
                packetLength + EncryptedRepository.GCM_IV_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES,
                false);
        int currentPacketLength = decrypt(packetInputStream);
        if (currentPacketLength != packetLength) {
            hasNext = false;
        }
        return new ByteArrayInputStream(packet, 0, currentPacketLength);
    }

    public static long getDecryptionSize(long size, int packetLength) {
        long encryptedPacketLength = packetLength + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES + EncryptedRepository.GCM_IV_SIZE_IN_BYTES;
        long completePackets = size / encryptedPacketLength;
        long decryptedSize = completePackets * packetLength;
        if (size % encryptedPacketLength != 0) {
            decryptedSize += (size % encryptedPacketLength) - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        }
        return decryptedSize;
    }
}
