/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.ElasticsearchException;

public abstract class SqlException extends ElasticsearchException {
    public SqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlException(String message, Object... args) {
        super(message, args);
    }

    public SqlException(Throwable cause, String message, Object... args) {
        super(message, cause, args);
    }

    public SqlException(Throwable cause) {
        super(cause);
    }
}
