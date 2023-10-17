/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql;

/**
 * Exception thrown when unable to continue processing client request,
 * in cases such as invalid query or failure to apply requested processing to given data.
 */
public class QlClientIllegalArgumentException extends QlClientException {

    public QlClientIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public QlClientIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

}
