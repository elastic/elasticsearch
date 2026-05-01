/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

/**
 * Thrown when an encryption or decryption operation cannot proceed because the required key
 * is not yet available. This is a transient condition during cluster recovery or primary
 * encryption key installation.
 */
public class EncryptionKeyNotYetAvailableException extends ElasticsearchException {

    public EncryptionKeyNotYetAvailableException(String msg, Object... args) {
        super(msg, args);
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
