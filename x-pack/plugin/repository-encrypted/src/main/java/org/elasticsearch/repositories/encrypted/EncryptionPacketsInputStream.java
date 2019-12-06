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
 * The method of encryption is AES/GCM/NoPadding, which is a variant of authenticated encryption.
 * The encryption works packet wise, i.e. the bytes are encrypted separately, using an unique
 * {@code Cipher}. All the packets are encrypted using the same {@code SecretKey} but using a
 * different Initialization Vector. The IV is comprised of an integer the same for all packets,
 * a {@code nonce} that must not repeat for the same {@code secretKey}, and a monotonically
 * increasing long counter. The packet size is preferably a large multiple of the AES block size,
 * but this is not a requirement.
 * <p>
 * This input stream supports the {@code mark} and {@code reset} operations only if the wrapped
 * stream also supports them. A {@code mark} call will trigger the memory buffering of the current
 * packet and will also trigger a {@code mark} call on the wrapped input stream on the next
 * packet boundary.
 *
 * @see DecryptionPacketsInputStream
 */
public final class EncryptionPacketsInputStream extends ChainingInputStream {

    private static final int MAX_PACKET_LENGTH_IN_BYTES = 1 << 30;

    private final InputStream source;
    private final SecretKey secretKey;
    private final int packetLength;
    private final ByteBuffer packetIv;
    private final int encryptedPacketLength;

    private long counter;
    private Long markCounter;
    private int markSourceOnNextPacket;

    public static long getEncryptionSize(long size, int packetLength) {
        return size + (size / packetLength + 1) * (EncryptedRepository.GCM_TAG_SIZE_IN_BYTES + EncryptedRepository.GCM_IV_SIZE_IN_BYTES);
    }

    public EncryptionPacketsInputStream(InputStream source, SecretKey secretKey, int nonce, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        if (packetLength <= 0 || packetLength >= MAX_PACKET_LENGTH_IN_BYTES) {
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
    InputStream nextElement(InputStream currentElementIn) throws IOException {
        // the last packet input stream is the only one shorter than encryptedPacketLength
        if (currentElementIn != null && ((CountingInputStream) currentElementIn).getCount() < encryptedPacketLength) {
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
