/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * Holds the necessary, and sufficient, metadata required to decrypt the associated encrypted blob.
 * The data encryption key (DEK {@link #dataEncryptionKey}) is the most important part of the metadata;
 * it must be kept secret (i.e. MUST be stored encrypted).
 * The metadata does not hold an explicit link to the associated encrypted blob. It's the responsibility of the creator
 * ({@link EncryptedRepository}) to maintain this association.
 */
public final class BlobEncryptionMetadata {

    // this is part of the Initialization Vectors of the encrypted data blobs
    // although the IVs of the encrypted data blobs are stored in plain in the ciphertext,
    // storing it in the metadata as well, is a simpler way to verify the association without
    // attempting the decryption (without using this software even, because the {@link #nonce} is the
    // first 4-byte integer (little endian) of both the metadata and the associated encrypted blob)
    private final int nonce;
    // the packet length from {@link EncryptionPacketsInputStream}
    private final int packetLengthInBytes;
    // the key used to encrypt and decrypt the associated blob
    private final SecretKey dataEncryptionKey;

    public BlobEncryptionMetadata(int nonce, int packetLengthInBytes, SecretKey dataEncryptionKey) {
        this.nonce = nonce;
        this.packetLengthInBytes = packetLengthInBytes;
        this.dataEncryptionKey = Objects.requireNonNull(dataEncryptionKey);
    }

    public int getNonce() {
        return nonce;
    }

    public int getPacketLengthInBytes() {
        return packetLengthInBytes;
    }

    public SecretKey getDataEncryptionKey() {
        return dataEncryptionKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlobEncryptionMetadata metadata = (BlobEncryptionMetadata) o;
        return nonce == metadata.nonce &&
                packetLengthInBytes == metadata.packetLengthInBytes &&
                Objects.equals(dataEncryptionKey, metadata.dataEncryptionKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nonce, packetLengthInBytes);
        result = 31 * result + Objects.hashCode(dataEncryptionKey);
        return result;
    }

    static byte[] serializeMetadata(BlobEncryptionMetadata metadata, CheckedBiFunction<byte[], byte[], byte[], Exception> encryptor)
            throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0, metadata.getNonce());
        byteBuffer.putInt(Integer.BYTES, metadata.getPacketLengthInBytes());
        byte[] authenticatedData = byteBuffer.array();
        final byte[] encryptedData;
        try {
            encryptedData = encryptor.apply(metadata.getDataEncryptionKey().getEncoded(), authenticatedData);
        } catch (Exception e) {
            throw new IOException("Failure to encrypt metadata", e);
        }
        byte[] result = new byte[authenticatedData.length + encryptedData.length];
        System.arraycopy(authenticatedData, 0, result, 0, authenticatedData.length);
        System.arraycopy(encryptedData, 0, result, authenticatedData.length, encryptedData.length);
        return result;
    }

    static BlobEncryptionMetadata deserializeMetadata(byte[] metadata, CheckedBiFunction<byte[], byte[], byte[], Exception> decryptor)
            throws IOException {
        byte[] authenticatedData = Arrays.copyOf(metadata, 2 * Integer.BYTES);
        ByteBuffer byteBuffer = ByteBuffer.wrap(authenticatedData).order(ByteOrder.LITTLE_ENDIAN);
        int nonce = byteBuffer.get(0);
        int packetLengthInBytes = byteBuffer.get(Integer.BYTES);
        byte[] encryptedData = Arrays.copyOfRange(metadata, 2 * Integer.BYTES, metadata.length);
        final byte[] decryptedData;
        try {
            decryptedData = decryptor.apply(encryptedData, authenticatedData);
        } catch (Exception e) {
            throw new IOException("Failure to decrypt metadata", e);
        }
        SecretKey dataDecryptionKey = new SecretKeySpec(decryptedData, 0, decryptedData.length, "AES");
        return new BlobEncryptionMetadata(nonce, packetLengthInBytes, dataDecryptionKey);
    }
}
