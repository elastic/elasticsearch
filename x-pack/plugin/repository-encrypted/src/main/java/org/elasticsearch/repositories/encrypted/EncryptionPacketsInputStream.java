/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * An {@code EncryptionPacketsInputStream} wraps another input stream and encrypts its contents.
 * The method of encryption is AES/GCM/NoPadding, which is a type of authenticated encryption.
 * The encryption works packet wise, i.e. the stream is segmented into fixed-size byte packets
 * which are separately encrypted using a unique {@link Cipher}. As an exception, only the last
 * packet will have a different size, possibly zero. Note that the encrypted packets are
 * larger compared to the plaintext packets, because they contain a 16 byte length trailing
 * authentication tag. The resulting encrypted and authenticated packets are assembled back into
 * the resulting stream.
 * <p>
 * The packets are encrypted using the same {@link SecretKey} but using a different initialization
 * vector. The IV is 12 bytes wide and it's comprised of an integer {@code nonce}, the same for
 * every packet in a stream, but which MUST not otherwise be repeated for the same {@code SecretKey}
 * across other streams, and a monotonically increasing long counter. When assembling the resulting
 * stream, the IV is prepended to the corresponding packet's ciphertext.
 * <p>
 * The packet size is preferably a large multiple of the AES block size (128 bytes), but any positive
 * integer value smaller than {@link EncryptedRepository#MAX_PACKET_LENGTH_IN_BYTES} is valid.
 * <p>
 * This input stream supports the {@code mark} and {@code reset} operations, but only if the wrapped
 * stream supports them as well. A {@code mark} call will trigger the memory buffering of the current
 * packet and will also trigger a {@code mark} call on the wrapped input stream on the next
 * packet boundary. Upon a {@code reset} call, the buffered packet will be replayed and new packets
 * will be generated starting from the marked packet boundary on the wrapped stream.
 * <p>
 * The {@code close} call will close the encryption input stream and any subsequent {@code read},
 * {@code skip}, {@code available} and {@code reset} calls will throw {@code IOException}s.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 *
 * @see DecryptionPacketsInputStream
 */
public final class EncryptionPacketsInputStream extends ChainingInputStream {

    protected final InputStream source; // protected for tests
    private final SecretKey secretKey;
    private final int packetLength;
    private final ByteBuffer packetIv;
    private final int encryptedPacketLength;

    protected long counter; // protected for tests
    protected Long markCounter; // protected for tests
    protected int markSourceOnNextPacket; // protected for tests

    public static long getEncryptionSize(long size, int packetLength) {
        return size + (size / packetLength + 1) * (EncryptedRepository.GCM_TAG_SIZE_IN_BYTES + EncryptedRepository.GCM_IV_SIZE_IN_BYTES);
    }

    public EncryptionPacketsInputStream(InputStream source, SecretKey secretKey, int nonce, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        if (packetLength <= 0 || packetLength >= EncryptedRepository.MAX_PACKET_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Invalid packet length [" + packetLength + "]");
        }
        this.packetLength = packetLength;
        this.packetIv = ByteBuffer.allocate(EncryptedRepository.GCM_IV_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        this.packetIv.putInt(0, nonce);
        this.encryptedPacketLength = packetLength + EncryptedRepository.GCM_IV_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        this.counter = EncryptedRepository.PACKET_START_COUNTER;
        this.markCounter = null;
        this.markSourceOnNextPacket = -1;
    }

    @Override
    InputStream nextComponent(InputStream currentComponentIn) throws IOException {
        // the last packet input stream is the only one shorter than encryptedPacketLength
        if (currentComponentIn != null && ((CountingInputStream) currentComponentIn).getCount() < encryptedPacketLength) {
            // there are no more packets
            return null;
        }
        // mark source input stream at packet boundary
        if (markSourceOnNextPacket != -1) {
            source.mark(markSourceOnNextPacket);
            markSourceOnNextPacket = -1;
        }
        // create the new packet
        InputStream encryptionInputStream = new PrefixInputStream(source, packetLength, false);
        packetIv.putLong(4, counter++);
        if (counter == EncryptedRepository.PACKET_START_COUNTER) {
            // counter wrap around
            throw new IOException("Maximum packet count limit exceeded");
        }
        Cipher packetCipher = getPacketEncryptionCipher(secretKey, packetIv.array());
        encryptionInputStream = new CipherInputStream(encryptionInputStream, packetCipher);
        encryptionInputStream = new SequenceInputStream(new ByteArrayInputStream(packetIv.array()), encryptionInputStream);
        encryptionInputStream = new BufferOnMarkInputStream(encryptionInputStream, encryptedPacketLength);
        return new CountingInputStream(encryptionInputStream, false);
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
            // handles the packet-wise part of the marking operation
            super.mark(encryptedPacketLength);
            // saves the counter used to generate packet IVs
            markCounter = counter;
            // stores the flag used to mark the source input stream at packet boundary
            markSourceOnNextPacket = readlimit;
        }
    }

    @Override
    public void reset() throws IOException {
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        if (markCounter == null) {
            throw new IOException("Mark no set");
        }
        super.reset();
        counter = markCounter;
        if (markSourceOnNextPacket == -1) {
            source.reset();
        }
    }

    private static Cipher getPacketEncryptionCipher(SecretKey secretKey, byte[] packetIv) throws IOException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(EncryptedRepository.GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, packetIv);
        try {
            Cipher packetCipher = Cipher.getInstance(EncryptedRepository.GCM_ENCRYPTION_SCHEME);
            packetCipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmParameterSpec);
            return packetCipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IOException(e);
        }
    }

}
