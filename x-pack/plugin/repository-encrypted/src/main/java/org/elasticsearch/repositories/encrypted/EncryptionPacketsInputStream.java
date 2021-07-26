/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.core.internal.io.IOUtils;

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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

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
 * vector. The IV of each packet is 12 bytes wide and is comprised of a 4-byte integer {@code nonce},
 * the same for every packet in the stream, and a monotonically increasing 8-byte integer counter.
 * The caller must assure that the same {@code nonce} is not reused for other encrypted streams
 * using the same {@code secretKey}. The counter from the IV identifies the position of the packet
 * in the encrypted stream, so that packets cannot be reordered without breaking the decryption.
 * When assembling the encrypted stream, the IV is prepended to the corresponding packet's ciphertext.
 * <p>
 * The packet length is preferably a large multiple (typically 128) of the AES block size (128 bytes),
 * but any positive integer value smaller than {@link EncryptedRepository#MAX_PACKET_LENGTH_IN_BYTES}
 * is valid. A larger packet length incurs smaller relative size overhead because the 12 byte wide IV
 * and the 16 byte wide authentication tag are constant no matter the packet length. A larger packet
 * length also exposes more opportunities for the JIT compilation of the AES encryption loop. But
 * {@code mark} will buffer up to packet length bytes, and, more importantly, <b>decryption</b> might
 * need to allocate a memory buffer the size of the packet in order to assure that no un-authenticated
 * decrypted ciphertext is returned. The decryption procedure is the primary factor that limits the
 * packet length.
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

    private final SecretKey secretKey;
    private final int packetLength;
    private final ByteBuffer packetIv;
    private final int encryptedPacketLength;

    final InputStream source; // package-protected for tests
    long counter; // package-protected for tests
    Long markCounter; // package-protected for tests
    int markSourceOnNextPacket; // package-protected for tests

    /**
     * Computes and returns the length of the ciphertext given the {@code plaintextLength} and the {@code packetLength}
     * used during encryption.
     * The plaintext is segmented into packets of equal {@code packetLength} length, with the exception of the last
     * packet which is shorter and can have a length of {@code 0}. Encryption is packet-wise and is 1:1, with no padding.
     * But each encrypted packet is prepended by the Initilization Vector and appended the Authentication Tag, including
     * the last packet, so when pieced together will amount to a longer resulting ciphertext.
     *
     * @see DecryptionPacketsInputStream#getDecryptionLength(long, int)
     */
    public static long getEncryptionLength(long plaintextLength, int packetLength) {
        return plaintextLength + (plaintextLength / packetLength + 1) * (EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES
            + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES);
    }

    public EncryptionPacketsInputStream(InputStream source, SecretKey secretKey, int nonce, int packetLength) {
        this.source = Objects.requireNonNull(source);
        this.secretKey = Objects.requireNonNull(secretKey);
        if (packetLength <= 0 || packetLength >= EncryptedRepository.MAX_PACKET_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Invalid packet length [" + packetLength + "]");
        }
        this.packetLength = packetLength;
        this.packetIv = ByteBuffer.allocate(EncryptedRepository.GCM_IV_LENGTH_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        // nonce takes the first 4 bytes of the IV
        this.packetIv.putInt(0, nonce);
        this.encryptedPacketLength = packetLength + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES
            + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
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
        // If the enclosing stream has a mark set,
        // then apply it to the source input stream when we reach a packet boundary
        if (markSourceOnNextPacket != -1) {
            source.mark(markSourceOnNextPacket);
            markSourceOnNextPacket = -1;
        }
        // create the new packet
        InputStream encryptionInputStream = new PrefixInputStream(source, packetLength, false);
        // the counter takes up the last 8 bytes of the packet IV (12 byte wide)
        // the first 4 bytes are used by the nonce (which is the same for every packet IV)
        packetIv.putLong(Integer.BYTES, counter++);
        // counter wrap around
        if (counter == EncryptedRepository.PACKET_START_COUNTER) {
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

    @Override
    public void close() throws IOException {
        IOUtils.close(super::close, source);
    }

    private static Cipher getPacketEncryptionCipher(SecretKey secretKey, byte[] packetIv) throws IOException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES * Byte.SIZE, packetIv);
        try {
            Cipher packetCipher = Cipher.getInstance(EncryptedRepository.DATA_ENCRYPTION_SCHEME);
            packetCipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmParameterSpec);
            return packetCipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IOException(e);
        }
    }

}
