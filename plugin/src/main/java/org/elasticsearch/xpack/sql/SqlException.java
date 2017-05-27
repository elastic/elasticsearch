/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import java.util.Locale;

import static java.lang.String.format;

public class SqlException extends RuntimeException {

    public SqlException() {
        super();
    }

    public SqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlException(String message, Object... args) {
        this(null, message, args);
    }

    public SqlException(Throwable cause, String message, Object... args) {
        super(format(Locale.ROOT, message, args), cause);
    }

    public SqlException(Throwable cause) {
        super(cause);
    }
}
