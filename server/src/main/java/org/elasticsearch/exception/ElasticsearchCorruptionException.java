/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.exception;

import java.io.IOException;

/**
 * This exception is thrown when Elasticsearch detects
 * an inconsistency in one of it's persistent files.
 */
public class ElasticsearchCorruptionException extends IOException {

    /**
     * Creates a new {@link ElasticsearchCorruptionException}
     * @param message the exception message.
     */
    public ElasticsearchCorruptionException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link ElasticsearchCorruptionException} with the given exceptions stacktrace.
     * This constructor copies the stacktrace as well as the message from the given
     * {@code Throwable} into this exception.
     *
     * @param ex the exception cause
     */
    public ElasticsearchCorruptionException(Throwable ex) {
        super(ex);
    }
}
