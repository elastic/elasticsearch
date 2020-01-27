/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql;

/**
 * Exception triggered inside server-side code.
 * Typically a validation error or worse, a bug.
 */
public abstract class QlServerException extends QlException {

    protected QlServerException(String message, Object... args) {
        super(message, args);
    }

    protected QlServerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected QlServerException(String message, Throwable cause) {
        super(message, cause);
    }

    protected QlServerException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected QlServerException(Throwable cause) {
        super(cause);
    }
}
