/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

/**
 * Provides symmetric encrypt/decrypt operations.
 *
 * <p>Callers never handle raw key material. The returned {@link EncryptedData} is self-describing, carrying the key ID alongside the
 * encrypted payload so that features can store both.
 *
 * <p>Features that store encrypted data must register a {@link KeyRotationHandler} via
 * {@link #registerKeyRotationHandler(KeyRotationHandler)}
 * so that their data can be re-encrypted under a new key when the primary encryption key rotates.
 */
public interface EncryptionService {

    /**
     * Encrypts the given bytes using the current active key.
     *
     * @param bytes the data to encrypt
     * @return the encrypted payload and the key ID used
     */
    EncryptedData encrypt(byte[] bytes);

    /**
     * Decrypts data previously produced by {@link #encrypt(byte[])}.
     *
     * @param encryptedData the output of a prior {@link #encrypt(byte[])} call
     * @return the original unencrypted bytes
     */
    byte[] decrypt(EncryptedData encryptedData);

    /**
     * Registers a {@link KeyRotationHandler} to be invoked when the encryption key is rotated.
     *
     * @param handler the callback to invoke when the encryption key is rotated
     */
    void registerKeyRotationHandler(KeyRotationHandler handler);
}
