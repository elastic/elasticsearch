/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class DecryptionPacketsInputStream extends ChainPacketsInputStream {

    private final InputStream source;
    private final SecretKey secretKey;
    private final int nonce;
    private final int packetLength;
    private final byte[] packet;
    private final byte[] iv;

    private boolean hasNext;
    private long counter;

    public static long getDecryptionSize(long size, int packetLength) {
        long encryptedPacketLength = packetLength + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES + EncryptedRepository.GCM_IV_SIZE_IN_BYTES;
        long completePackets = size / encryptedPacketLength;
        long decryptedSize = completePackets * packetLength;
        if (size % encryptedPacketLength != 0) {
            decryptedSize += (size % encryptedPacketLength) - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES
                    - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        }
        return decryptedSize;
    }

    public DecryptionPacketsInputStream(InputStream source, SecretKey secretKey, int nonce, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        this.nonce = nonce;
        this.packetLength = packetLength;
        this.packet = new byte[packetLength + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES];
        this.iv = new byte[EncryptedRepository.GCM_IV_SIZE_IN_BYTES];
        this.hasNext = true;
        this.counter = EncryptedRepository.PACKET_START_COUNTER;
    }

    @Override
    boolean hasNextPacket(InputStream currentPacketIn) {
        return hasNext;
    }

    @Override
    InputStream nextPacket(InputStream currentPacketIn) throws IOException {
        if (currentPacketIn != null && currentPacketIn.read() != -1) {
            throw new IllegalStateException("Stream for previous packet has not been fully processed");
        }
        if (false == hasNextPacket(currentPacketIn)) {
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

    private int decrypt(PrefixInputStream packetInputStream) throws IOException {
        if (packetInputStream.read(iv) != iv.length) {
            throw new IOException("Error while reading the heading IV of the packet");
        }
        ByteBuffer ivBuffer = ByteBuffer.wrap(iv).order(ByteOrder.LITTLE_ENDIAN);
        if (ivBuffer.getInt(0) != nonce || ivBuffer.getLong(4) != counter++) {
            throw new IOException("Invalid packet IV");
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
}
