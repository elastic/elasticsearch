/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.xpack.ql.QlClientException;

public abstract class SqlClientException extends QlClientException {

    protected SqlClientException(String message, Object... args) {
        super(message, args);
    }

    protected SqlClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected SqlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    protected SqlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected SqlClientException(Throwable cause) {
        super(cause);
    }
}
