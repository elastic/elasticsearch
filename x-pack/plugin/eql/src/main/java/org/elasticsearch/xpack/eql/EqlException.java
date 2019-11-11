/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.ElasticsearchException;

public abstract class EqlException extends ElasticsearchException {
    public EqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public EqlException(String message, Object... args) {
        super(message, args);
    }

    public EqlException(Throwable cause, String message, Object... args) {
        super(message, cause, args);
    }

    public EqlException(Throwable cause) {
        super(cause);
    }
}
