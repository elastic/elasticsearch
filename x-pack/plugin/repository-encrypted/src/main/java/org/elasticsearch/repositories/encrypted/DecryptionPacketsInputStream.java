/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.GCM_IV_LENGTH_IN_BYTES;
import static org.elasticsearch.repositories.encrypted.EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;

/**
 * A {@code DecryptionPacketsInputStream} wraps an encrypted input stream and decrypts
 * its contents. This is designed (and tested) to decrypt only the encryption format that
 * {@link EncryptionPacketsInputStream} generates. No decrypted bytes are returned before
 * they are authenticated.
 * <p>
 * The same parameters, namely {@code secretKey} and {@code packetLength},
 * which have been used during encryption, must also be used for decryption,
 * otherwise decryption will fail.
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
    private final int packetLength;
    private final byte[] packetBuffer;

    private boolean hasNext;
    private long counter;

    /**
     * Computes and returns the length of the plaintext given the {@code ciphertextLength} and the {@code packetLength}
     * used during encryption.
     * Each ciphertext packet is prepended by the Initilization Vector and has the Authentication Tag appended.
     * Decryption is 1:1, and the ciphertext is not padded, but stripping away the IV and the AT amounts to a shorter
     * plaintext compared to the ciphertext.
     *
     * @see EncryptionPacketsInputStream#getEncryptionLength(long, int)
     */
    public static long getDecryptionLength(long ciphertextLength, int packetLength) {
        long encryptedPacketLength = packetLength + GCM_TAG_LENGTH_IN_BYTES + GCM_IV_LENGTH_IN_BYTES;
        long completePackets = ciphertextLength / encryptedPacketLength;
        long decryptedSize = completePackets * packetLength;
        if (ciphertextLength % encryptedPacketLength != 0) {
            decryptedSize += (ciphertextLength % encryptedPacketLength) - GCM_IV_LENGTH_IN_BYTES - GCM_TAG_LENGTH_IN_BYTES;
        }
        return decryptedSize;
    }

    public DecryptionPacketsInputStream(InputStream source, SecretKey secretKey, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        if (packetLength <= 0 || packetLength >= EncryptedRepository.MAX_PACKET_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Invalid packet length [" + packetLength + "]");
        }
        this.packetLength = packetLength;
        this.packetBuffer = new byte[packetLength + GCM_TAG_LENGTH_IN_BYTES];
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
        PrefixInputStream packetInputStream = new PrefixInputStream(
            source,
            packetLength + GCM_IV_LENGTH_IN_BYTES + GCM_TAG_LENGTH_IN_BYTES,
            false
        );
        int currentPacketLength = decrypt(packetInputStream);
        // only the last packet is shorter, so this must be the last packet
        if (currentPacketLength != packetLength) {
            hasNext = false;
        }
        return new ByteArrayInputStream(packetBuffer, 0, currentPacketLength);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {}

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark/reset not supported");
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(super::close, source);
    }

    private int decrypt(PrefixInputStream packetInputStream) throws IOException {
        // read only the IV prefix into the packet buffer
        int ivLength = packetInputStream.readNBytes(packetBuffer, 0, GCM_IV_LENGTH_IN_BYTES);
        if (ivLength != GCM_IV_LENGTH_IN_BYTES) {
            throw new IOException("Packet heading IV error. Unexpected length [" + ivLength + "].");
        }
        // extract the counter from the packet IV and validate it (that the packet is in order)
        // skips the first 4 bytes in the packet IV, which contain the encryption nonce, which cannot be explicitly validated
        // because the nonce is not passed in during decryption, but it is implicitly because it is part of the IV,
        // when GCM validates the packet authn tag
        long packetIvCounter = ByteUtils.readLongLE(packetBuffer, Integer.BYTES);
        if (packetIvCounter != counter) {
            throw new IOException("Packet counter mismatch. Expecting [" + counter + "], but got [" + packetIvCounter + "].");
        }
        // counter increment for the subsequent packet
        counter++;
        // counter wrap around
        if (counter == EncryptedRepository.PACKET_START_COUNTER) {
            throw new IOException("Maximum packet count limit exceeded");
        }
        // cipher used to decrypt only the current packetInputStream
        Cipher packetCipher = getPacketDecryptionCipher(packetBuffer);
        // read the rest of the packet, reusing the packetBuffer
        int packetLen = packetInputStream.readNBytes(packetBuffer, 0, packetBuffer.length);
        if (packetLen < GCM_TAG_LENGTH_IN_BYTES) {
            throw new IOException("Encrypted packet is too short");
        }
        try {
            // in-place decryption of the whole packet and return decrypted length
            return packetCipher.doFinal(packetBuffer, 0, packetLen, packetBuffer);
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException("Exception during packet decryption", e);
        }
    }

    private Cipher getPacketDecryptionCipher(byte[] packet) throws IOException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_IN_BYTES * Byte.SIZE, packet, 0, GCM_IV_LENGTH_IN_BYTES);
        try {
            Cipher packetCipher = Cipher.getInstance(EncryptedRepository.DATA_ENCRYPTION_SCHEME);
            packetCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);
            return packetCipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IOException("Exception during packet cipher initialisation", e);
        }
    }
}
