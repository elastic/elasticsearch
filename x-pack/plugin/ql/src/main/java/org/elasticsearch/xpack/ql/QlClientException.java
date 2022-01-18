/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql;

/**
 * Exception thrown by performing client (or user) code.
 * Typically it means the given action or query is incorrect and needs fixing.
 */
public abstract class QlClientException extends QlException {

    protected QlClientException(String message, Object... args) {
        super(message, args);
    }

    protected QlClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected QlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    protected QlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected QlClientException(Throwable cause) {
        super(cause);
    }
}
