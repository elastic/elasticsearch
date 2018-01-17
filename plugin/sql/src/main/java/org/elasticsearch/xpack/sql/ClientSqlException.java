/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

public abstract class ClientSqlException extends SqlException {

    protected ClientSqlException(String message, Object... args) {
        super(message, args);
    }

    protected ClientSqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected ClientSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ClientSqlException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected ClientSqlException(Throwable cause) {
        super(cause);
    }
}
