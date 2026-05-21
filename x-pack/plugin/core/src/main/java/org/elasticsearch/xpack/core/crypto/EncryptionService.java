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
 * <p>Callers never handle raw key material. The returned {@link EncryptedData} is self-describing,
 * carrying the key ID alongside the encrypted payload so that features can store both.
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
}
