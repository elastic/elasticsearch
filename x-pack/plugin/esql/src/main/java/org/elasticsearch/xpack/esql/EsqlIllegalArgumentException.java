/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class EsqlIllegalArgumentException extends QlIllegalArgumentException {
    public EsqlIllegalArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EsqlIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsqlIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public EsqlIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public EsqlIllegalArgumentException(String message) {
        super(message);
    }

    public EsqlIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
