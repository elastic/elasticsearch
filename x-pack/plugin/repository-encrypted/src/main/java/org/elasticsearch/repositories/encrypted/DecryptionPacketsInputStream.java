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
import java.util.Objects;

/**
 * A {@code DecryptionPacketsInputStream} wraps an encrypted input stream and decrypts
 * its contents. This is designed (and tested) to decrypt only the encryption format that
 * {@link EncryptionPacketsInputStream} generates. No decrypted bytes are returned before
 * they are authenticated.
 * <p>
 * The same parameters, namely {@code secretKey}, {@code nonce} and {@code packetLength},
 * that have been used during encryption must also be used for decryption, otherwise
 * decryption will fail.
 * <p>
 * This implementation buffers the encrypted packet in memory. The maximum packet size it can
 * accommodate is {@link EncryptedRepository#MAX_PACKET_LENGTH_IN_BYTES}.
 * <p>
 * This implementation does not support {@code mark} and {@code reset}.
 * <p>
 * The {@code close} call will close the decryption input stream and any subsequent {@code read},
 * {@code skip}, {@code available} and {@code reset} calls will throw {@code IOException}s.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 *
 * @see EncryptionPacketsInputStream
 */
public final class DecryptionPacketsInputStream extends ChainingInputStream {

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
            decryptedSize += (size % encryptedPacketLength) - EncryptedRepository.GCM_IV_SIZE_IN_BYTES
                    - EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        }
        return decryptedSize;
    }

    public DecryptionPacketsInputStream(InputStream source, SecretKey secretKey, int nonce, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        this.nonce = nonce;
        if (packetLength <= 0 || packetLength >= EncryptedRepository.MAX_PACKET_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Invalid packet length [" + packetLength + "]");
        }
        this.packetLength = packetLength;
        this.packet = new byte[packetLength + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES];
        this.iv = new byte[EncryptedRepository.GCM_IV_SIZE_IN_BYTES];
        this.hasNext = true;
        this.counter = EncryptedRepository.PACKET_START_COUNTER;
    }

    @Override
    InputStream nextComponent(InputStream currentComponentIn) throws IOException {
        if (currentComponentIn != null && currentComponentIn.read() != -1) {
            throw new IllegalStateException("Stream for previous packet has not been fully processed");
        }
        if (false == hasNext) {
            return null;
        }
        PrefixInputStream packetInputStream = new PrefixInputStream(source,
                packetLength + EncryptedRepository.GCM_IV_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES,
                false);
        int currentPacketLength = decrypt(packetInputStream);
        // only the last packet is shorter, so this must be the last packet
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
