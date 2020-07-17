/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql;

public class QlIllegalArgumentException extends QlServerException {
    public QlIllegalArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public QlIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public QlIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public QlIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public QlIllegalArgumentException(String message) {
        super(message);
    }

    public QlIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
