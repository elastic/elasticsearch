/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

/**
 * Provides symmetric encrypt/decrypt operations
 *
 * <p>Callers never handle raw key material. The encrypted data is self-describing and embeds any metadata needed to decrypt.
 */
public interface EncryptionService {

    /**
     * Encrypts the given bytes
     *
     * @param bytes the data to encrypt
     * @return the encrypted data and metadata
     */
    byte[] encrypt(byte[] bytes);

    /**
     * Decrypts bytes previously produced by {@link #encrypt(byte[])}.
     *
     * @param bytes the output of a prior {@link #encrypt(byte[])} call
     * @return the original unencrypted bytes
     */
    byte[] decrypt(byte[] bytes);
}
