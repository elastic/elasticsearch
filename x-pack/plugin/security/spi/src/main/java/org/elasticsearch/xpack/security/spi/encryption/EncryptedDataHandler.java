/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.spi.encryption;

import org.elasticsearch.action.ActionListener;

/**
 * Implemented by features that own data encrypted under a primary encryption key. The rotation coordinator invokes each handler in
 * response to encryption-lifecycle events so the feature can keep its ciphertext consistent.
 *
 * <p>Handlers are contributed via the {@link EncryptedDataHandlerProvider} SPI.
 */
public interface EncryptedDataHandler {

    /**
     * Re-encrypt all data owned by this handler that is currently encrypted with any key other than {@code activeKeyId}.
     * Implementations decrypt with the current key and re-encrypt under the new active key. Implementations must be idempotent.
     *
     * @param activeKeyId the key ID that all owned data should be encrypted under after this call
     * @param listener    notified on completion or failure
     */
    void reEncrypt(String activeKeyId, ActionListener<Void> listener);
}
