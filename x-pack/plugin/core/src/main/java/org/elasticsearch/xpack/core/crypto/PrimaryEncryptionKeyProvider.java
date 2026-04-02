/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.core.Nullable;

import javax.crypto.SecretKey;

/**
 * Provides access to the primary encryption key.
 *
 * <p>The primary encryption key is generated once per cluster and distributed to all nodes
 * via cluster state. Keys are identified by a key ID derived from the key material.
 *
 * <p>Implementations are registered via the security plugin. Consumers should inject
 * this interface rather than depending on the security module directly.
 */
public interface PrimaryEncryptionKeyProvider {

    /**
     * Returns the active encryption key for new encrypt operations,
     * or {@code null} if not yet available.
     */
    @Nullable
    SecretKey getActiveKey();

    /**
     * Returns the key ID of the active encryption key,
     * or {@code null} if not yet available.
     */
    @Nullable
    String getActiveKeyId();

    /**
     * Returns the encryption key for the given key ID (for decryption),
     * or {@code null} if the key ID is not known.
     */
    @Nullable
    SecretKey getKey(String keyId);
}
