/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.xpack.ql.QlServerException;

public abstract class SqlServerException extends QlServerException {

    protected SqlServerException(String message, Object... args) {
        super(message, args);
    }

    protected SqlServerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected SqlServerException(String message, Throwable cause) {
        super(message, cause);
    }

    protected SqlServerException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected SqlServerException(Throwable cause) {
        super(cause);
    }
}
