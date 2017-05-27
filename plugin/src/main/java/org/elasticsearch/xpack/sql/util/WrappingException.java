/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

public class WrappingException extends RuntimeException {

    public WrappingException(String message, Exception cause) {
        super(message, cause, false, false);
    }

    public WrappingException(Exception cause) {
        super(cause.getMessage(), cause, false, false);
    }

    public Exception wrapped() {
        return (Exception) getCause();
    }
}
