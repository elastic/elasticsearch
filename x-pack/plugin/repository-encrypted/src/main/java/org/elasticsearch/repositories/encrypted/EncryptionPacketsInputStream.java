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
import java.util.NoSuchElementException;
import java.util.Objects;

public final class EncryptionPacketsInputStream extends ChainPacketsInputStream {

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
    boolean hasNextPacket(InputStream currentPacketIn) {
        if (currentPacketIn != null && currentPacketIn instanceof CountingInputStream == false) {
            throw new IllegalStateException();
        }
        if (currentPacketIn != null && ((CountingInputStream) currentPacketIn).getCount() > encryptedPacketLength) {
            throw new IllegalStateException();
        }
        return currentPacketIn == null || ((CountingInputStream) currentPacketIn).getCount() == encryptedPacketLength;
    }

    @Override
    InputStream nextPacket(InputStream currentPacketIn) throws IOException {
        if (currentPacketIn != null && currentPacketIn.read() != -1) {
            throw new IllegalStateException("Stream for previous packet has not been fully processed");
        }
        if (false == hasNextPacket(currentPacketIn)) {
            throw new NoSuchElementException();
        }
        if (markSourceOnNextPacket != -1) {
            source.mark(markSourceOnNextPacket);
            markSourceOnNextPacket = -1;
        }
        InputStream encryptionInputStream = new PrefixInputStream(source, packetLength, false);
        packetIv.putLong(4, counter++);
        if (counter == EncryptedRepository.PACKET_START_COUNTER) {
            // counter wrap around
            throw new Error("Maximum packet count limit exceeded");
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
            super.mark(encryptedPacketLength);
            markCounter = counter;
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
