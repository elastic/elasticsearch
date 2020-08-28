/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class EqlIllegalArgumentException extends QlIllegalArgumentException {
    public EqlIllegalArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EqlIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public EqlIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public EqlIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public EqlIllegalArgumentException(String message) {
        super(message);
    }

    public EqlIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
