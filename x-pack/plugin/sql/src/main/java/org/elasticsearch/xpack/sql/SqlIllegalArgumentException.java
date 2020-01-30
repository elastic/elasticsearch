/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class SqlIllegalArgumentException extends QlIllegalArgumentException {
    public SqlIllegalArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SqlIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public SqlIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public SqlIllegalArgumentException(String message) {
        super(message);
    }

    public SqlIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
