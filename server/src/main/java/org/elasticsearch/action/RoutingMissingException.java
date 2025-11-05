/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Exception thrown when a document operation requires routing but none was provided.
 * This occurs when an index has been configured to require explicit routing for document
 * operations, but a request is made without specifying the routing value.
 *
 * <p>Routing is a mechanism in Elasticsearch to control which shard a document is stored on.
 * When an index requires routing (typically for performance or co-location reasons), all
 * operations on documents in that index must include a routing parameter.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing when routing is missing
 * if (requiresRouting && request.routing() == null) {
 *     throw new RoutingMissingException(index, documentId);
 * }
 *
 * // Handling RoutingMissingException
 * try {
 *     indexDocument(request);
 * } catch (RoutingMissingException e) {
 *     logger.error("Routing required for index {} document {}: add routing parameter",
 *         e.getIndex().getName(), e.getId());
 *     // Return error to user instructing them to provide routing
 * }
 * }</pre>
 */
public final class RoutingMissingException extends ElasticsearchException {

    private final String id;

    /**
     * Constructs a new routing missing exception for the specified index and document ID.
     *
     * @param index the name of the index requiring routing
     * @param id the ID of the document for which routing is missing
     * @throws NullPointerException if index or id is null
     */
    public RoutingMissingException(String index, String id) {
        super("routing is required for [" + index + "]/[" + id + "]");
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(id, "id must not be null");
        setIndex(index);
        this.id = id;
    }

    /**
     * Returns the ID of the document for which routing is missing.
     *
     * @return the document ID
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the REST status code for this exception, which is {@link RestStatus#BAD_REQUEST}
     * since this represents a client error (missing required parameter).
     *
     * @return {@link RestStatus#BAD_REQUEST}
     */
    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    /**
     * Constructs a new routing missing exception by reading from a stream input.
     * This constructor is used for deserialization. It handles backward compatibility
     * with versions before 8.0.0 where type information was included.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public RoutingMissingException(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readString();
        }
        id = in.readString();
    }

    /**
     * Writes this exception to the specified stream output for serialization.
     * This method handles backward compatibility with versions before 8.0.0
     * where type information was included.
     *
     * @param out the stream output to write to
     * @param nestedExceptionsWriter the writer for nested exceptions
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
    }
}
