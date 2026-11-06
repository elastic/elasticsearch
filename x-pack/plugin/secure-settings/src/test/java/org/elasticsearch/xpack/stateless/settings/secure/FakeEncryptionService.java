/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;

/**
 * Stands in for the encryption plugin's AES-GCM service, which needs a project encryption key; callers here only depend on
 * decryption returning the plaintext or throwing while the service is unavailable.
 */
class FakeEncryptionService implements EncryptionService {

    volatile boolean available = true;

    @Override
    public EncryptedData encrypt(byte[] bytes) {
        return new EncryptedData("test-key", bytes.clone());
    }

    @Override
    public byte[] decrypt(EncryptedData encryptedData) {
        if (available == false) {
            throw new EncryptionServiceUnavailableException(EncryptionServiceState.DISABLED);
        }
        return encryptedData.payload().clone();
    }
}
