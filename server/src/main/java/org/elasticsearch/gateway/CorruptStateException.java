/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.ElasticsearchCorruptionException;

/**
 * This exception is thrown when Elasticsearch detects
 * an inconsistency in one of it's persistent states.
 */
public class CorruptStateException extends ElasticsearchCorruptionException {

    /**
     * Creates a new {@link CorruptStateException}
     * @param message the exception message.
     */
    public CorruptStateException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link CorruptStateException} with the given exceptions stacktrace.
     * This constructor copies the stacktrace as well as the message from the given {@link Throwable}
     * into this exception.
     *
     * @param ex the exception cause
     */
    public CorruptStateException(Throwable ex) {
        super(ex);
    }
}
