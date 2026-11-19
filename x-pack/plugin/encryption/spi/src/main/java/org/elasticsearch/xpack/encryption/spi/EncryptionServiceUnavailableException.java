/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

/**
 * Thrown when an encryption or decryption operation cannot proceed because the cluster-state
 * encryption service is in a degraded state that requires operator action, not a retry.
 *
 * <p>Unlike {@link EncryptionKeyNotYetAvailableException} (transient — cluster recovery or first
 * key installation), this exception indicates a permanent condition: the password is missing,
 * mismatched, or cannot decrypt the persisted key. The caller should surface the
 * {@link #serviceState()} and direct the operator to configure or reset the encryption settings.
 */
public class EncryptionServiceUnavailableException extends ElasticsearchException {

    private final EncryptionServiceState serviceState;

    public EncryptionServiceUnavailableException(EncryptionServiceState state) {
        super("cluster state encryption is unavailable (" + state.displayValue() + "). " + state.actionableHint());
        this.serviceState = state;
    }

    /** The specific degraded state that caused this exception. */
    public EncryptionServiceState serviceState() {
        return serviceState;
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
