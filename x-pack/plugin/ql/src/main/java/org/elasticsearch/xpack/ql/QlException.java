/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql;

import org.elasticsearch.exception.ElasticsearchException;

/**
 * Base class for all QL exceptions. Useful as a catch-all.
 */
public abstract class QlException extends ElasticsearchException {
    public QlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public QlException(String message, Throwable cause) {
        super(message, cause);
    }

    public QlException(String message, Object... args) {
        super(message, args);
    }

    public QlException(Throwable cause, String message, Object... args) {
        super(message, cause, args);
    }

    public QlException(Throwable cause) {
        super(cause);
    }
}
