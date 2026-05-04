/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.action.ActionListener;

/**
 * Implemented by features that store data encrypted via {@link EncryptionService}, to re-encrypt when the primary encryption key rotates.
 *
 * <p>Implementations are registered once via {@link EncryptionService#registerKeyRotationHandler(KeyRotationHandler)}.
 * The rotation coordinator invokes {@link #reEncrypt(String, ActionListener)} after a new active key is published.
 */
public interface KeyRotationHandler {

    /**
     * Unique identifier for this handler.
     */
    String name();

    /**
     * Re-encrypt all data owned by this handler that was encrypted with any key other than {@code activeKeyId}. Implementations decrypt
     * with the current key via {@link EncryptionService#decrypt(EncryptedData)} and re-encrypt via
     * {@link EncryptionService#encrypt(byte[])} which now uses the new active key. Implementations must be idempotent.
     *
     * @param activeKeyId the key ID that all owned data should be encrypted under after this call
     * @param listener    notified on completion or failure
     */
    void reEncrypt(String activeKeyId, ActionListener<Void> listener);
}
