/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.ql.QlServerException;

/**
 * Indicates an unsupported operation like methods missing an implementation or code that should be unreachable or dead.
 * Throws an error during construction if assertions are enabled. This is important for integration tests because we want to crash the
 * server with an error in case we encounter unsupported operations, but do not want to crash in production.
 */
public class EsqlUnsupportedOperationException extends QlServerException {
    public EsqlUnsupportedOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        assert false : message;
    }

    public EsqlUnsupportedOperationException(String message, Throwable cause) {
        super(message, cause);
        assert false : message;
    }

    public EsqlUnsupportedOperationException(String message, Object... args) {
        super(message, args);
        assert false : message;
    }

    public EsqlUnsupportedOperationException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
        assert false : message;
    }

    public EsqlUnsupportedOperationException(String message) {
        super(message);
        assert false : message;
    }

    public EsqlUnsupportedOperationException(Throwable cause) {
        super(cause);
        assert false : "unsupported operation caused by: " + cause.getMessage();
    }
}
