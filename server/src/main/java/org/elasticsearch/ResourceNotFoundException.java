/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Indicates a requested resource was not found in Elasticsearch.
 * <p>
 * This generic exception corresponds to the {@link RestStatus#NOT_FOUND} HTTP status code
 * and is used when an operation references a resource (index, document, snapshot, etc.) that
 * does not exist in the cluster.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * public Document getDocument(String id) {
 *     Document doc = repository.findById(id);
 *     if (doc == null) {
 *         throw new ResourceNotFoundException("Document with id [{}] not found", id);
 *     }
 *     return doc;
 * }
 * }</pre>
 */
public class ResourceNotFoundException extends ElasticsearchException {

    /**
     * Constructs a resource not found exception with a formatted message.
     *
     * @param msg the detail message, can include {} placeholders
     * @param args the arguments to format into the message
     */
    public ResourceNotFoundException(String msg, Object... args) {
        super(msg, args);
    }

    /**
     * Constructs a resource not found exception with a formatted message and cause.
     *
     * @param msg the detail message, can include {} placeholders
     * @param cause the underlying cause of this exception
     * @param args the arguments to format into the message
     */
    public ResourceNotFoundException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    /**
     * Constructs a resource not found exception from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public ResourceNotFoundException(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Returns the REST status code for this exception.
     *
     * @return {@link RestStatus#NOT_FOUND} indicating the resource was not found
     */
    @Override
    public final RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
