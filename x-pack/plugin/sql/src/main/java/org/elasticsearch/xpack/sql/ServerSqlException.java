/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

public abstract class ServerSqlException extends SqlException {

    protected ServerSqlException(String message, Object... args) {
        super(message, args);
    }

    protected ServerSqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected ServerSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ServerSqlException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected ServerSqlException(Throwable cause) {
        super(cause);
    }
}
