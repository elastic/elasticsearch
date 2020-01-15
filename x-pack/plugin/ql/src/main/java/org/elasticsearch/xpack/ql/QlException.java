/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql;

import org.elasticsearch.ElasticsearchException;

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
