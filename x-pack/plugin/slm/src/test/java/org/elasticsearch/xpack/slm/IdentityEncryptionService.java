/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

/**
 * Identity crypto for unit tests: the payload is the plaintext, so tests can directly assert what was
 * "encrypted" and what a decryption yields without real key material.
 */
public final class IdentityEncryptionService implements EncryptionService {

    public static final IdentityEncryptionService INSTANCE = new IdentityEncryptionService();

    private IdentityEncryptionService() {}

    @Override
    public EncryptedData encrypt(byte[] bytes) {
        return new EncryptedData("test-key", bytes.clone());
    }

    @Override
    public byte[] decrypt(EncryptedData encryptedData) {
        return encryptedData.payload();
    }
}
