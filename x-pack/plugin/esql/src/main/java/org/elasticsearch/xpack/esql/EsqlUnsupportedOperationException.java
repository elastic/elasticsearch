/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.ql.QlServerException;

public class EsqlUnsupportedOperationException extends QlServerException {
    public EsqlUnsupportedOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EsqlUnsupportedOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsqlUnsupportedOperationException(String message, Object... args) {
        super(message, args);
    }

    public EsqlUnsupportedOperationException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public EsqlUnsupportedOperationException(String message) {
        super(message);
    }

    public EsqlUnsupportedOperationException(Throwable cause) {
        super(cause);
    }
}
