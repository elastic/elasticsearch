package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.hash.MessageDigests;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.SequenceInputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public final class EncryptionInputStream extends ChainInputStream {

    private final InputStream source;
    private final SecretKey secretKey;
    private final int packetLength;
    private final int encryptedPacketLength;

    private IvGenerator currentIvGenerator;
    private IvGenerator markIvGenerator;
    private int markSourceOnNextPacket;

    public EncryptionInputStream(InputStream source, SecretKey secretKey, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        this.packetLength = packetLength;
        this.encryptedPacketLength = packetLength + EncryptedRepository.GCM_IV_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        this.currentIvGenerator = new IvGenerator();
        this.markIvGenerator = null;
        this.markSourceOnNextPacket = -1;
    }

    @Override
    public boolean markSupported() {
        return source.markSupported();
    }

    @Override
    public void mark(int readlimit) {
        if (markSupported()) {
            if (readlimit <= 0) {
                throw new IllegalArgumentException("Mark readlimit must be a positive integer");
            }
            super.mark(encryptedPacketLength);
            markIvGenerator = new IvGenerator(this.currentIvGenerator);
            markSourceOnNextPacket = readlimit;
        }
    }

    @Override
    public void reset() throws IOException {
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        if (markIvGenerator == null) {
            throw new IOException("Mark no set");
        }
        super.reset();
        currentIvGenerator = new IvGenerator(markIvGenerator);
        if (markSourceOnNextPacket == -1) {
            source.reset();
        }
    }

    @Override
    boolean hasNext(InputStream currentStream) {
        if (currentStream != null && currentStream instanceof CountingInputStream == false) {
            throw new IllegalStateException();
        }
        if (((CountingInputStream) currentStream).getCount() > encryptedPacketLength) {
            throw new IllegalStateException();
        }
        return currentStream == null || ((CountingInputStream) currentStream).getCount() == encryptedPacketLength;
    }

    @Override
    InputStream next(InputStream currentStream) throws IOException {
        if (currentStream != null && currentStream.read() != -1) {
            throw new IllegalStateException("Stream for previous packet has not been fully processed");
        }
        if (false == hasNext(currentStream)) {
            throw new NoSuchElementException();
        }
        if (markSourceOnNextPacket != -1) {
            markSourceOnNextPacket = -1;
            source.mark(markSourceOnNextPacket);
        }
        InputStream encryptionInputStream = new PrefixInputStream(source, packetLength, false);
        byte[] packetIv = currentIvGenerator.newRandomUniqueIv();
        Cipher packetCipher = getPacketEncryptionCipher(packetIv);
        encryptionInputStream = new CipherInputStream(encryptionInputStream, packetCipher);
        encryptionInputStream = new SequenceInputStream(new ByteArrayInputStream(packetIv), encryptionInputStream);
        encryptionInputStream = new BufferOnMarkInputStream(encryptionInputStream, packetLength);
        return new CountingInputStream(encryptionInputStream, false);
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

    public static long getEncryptionSize(long size, int packetLength) {
        return size + (size / packetLength + 1) * (EncryptedRepository.GCM_TAG_SIZE_IN_BYTES + EncryptedRepository.GCM_IV_SIZE_IN_BYTES);
    }

    static class IvGenerator {

        private final SecureRandom secureRandom;
        private final Set<String> previousIvs;

        IvGenerator() {
            this.secureRandom = new SecureRandom();
            this.previousIvs = new HashSet<>();
        }

        IvGenerator(IvGenerator other) {
            this(other.secureRandom, other.previousIvs);
        }

        IvGenerator(SecureRandom secureRandom, Set<String> previousIvs) {
            try {
                this.secureRandom = cloneRandom(secureRandom);
            } catch (Exception e) {
                throw new Error(e);
            }
            this.previousIvs = new HashSet<>(previousIvs);
        }

        byte[] newRandomUniqueIv() {
            byte[] iv = new byte[EncryptedRepository.GCM_TAG_SIZE_IN_BYTES];
            do {
                secureRandom.nextBytes(iv);
            } while (false == previousIvs.add(MessageDigests.toHexString(iv)));
            return iv;
        }

        private static SecureRandom cloneRandom(SecureRandom src) throws Exception {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bo);
            oos.writeObject(src);
            oos.close();
            ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(bo.toByteArray()));
            return (SecureRandom)(ois.readObject());
        }
    }

}
