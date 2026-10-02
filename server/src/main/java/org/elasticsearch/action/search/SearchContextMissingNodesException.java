/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

/**
 * Exception thrown when a search context references nodes that have left the cluster.
 * <p>
 * This exception indicates that a previously created search context is no longer valid
 * because one or more of the nodes it depends on have left the cluster. This typically
 * occurs when a client tries to fetch additional pages of search results after some nodes
 * have become unavailable. The exception provides details about the missing nodes and
 * the context that is no longer valid.
 * </p>
 * <p>
 * The exception is returned with HTTP status 404 (NOT_FOUND) when serialized to REST responses.
 * </p>
 */
public class SearchContextMissingNodesException extends ElasticsearchException {

    /**
     * The type of search context that became invalid.
     */
    public enum ContextType {
        SCROLL,
        PIT;

        private final String value;

        ContextType() {
            this.value = name().toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return value;
        }

        public static ContextType fromString(String value) {
            return ContextType.valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * The transport version at which this exception type was introduced.
     */
    public static final TransportVersion SEARCH_CONTEXT_MISSING_NODES_EXCEPTION_VERSION = TransportVersion.fromName(
        "search_context_missing_nodes_exception"
    );

    private final Set<String> missingNodeIds;
    private final ContextType contextType;

    /**
     * Constructs a new SearchContextMissingNodesException.
     *
     * @param contextType the type of search context
     * @param missingNodeIds the set of node IDs that have left the cluster
     */
    public SearchContextMissingNodesException(ContextType contextType, Set<String> missingNodeIds) {
        super(buildMessage(contextType, missingNodeIds));
        this.contextType = contextType;
        this.missingNodeIds = Set.copyOf(missingNodeIds);
    }

    /**
     * Constructs a new SearchContextMissingNodesException from a StreamInput.
     * Used for deserialization during transport communication between nodes.
     *
     * @param in the input stream to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public SearchContextMissingNodesException(StreamInput in) throws IOException {
        super(in);
        this.contextType = ContextType.fromString(in.readString());
        this.missingNodeIds = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    /**
     * Serializes this exception to a StreamOutput.
     * Overrides the protected two-arg overload so that custom fields are written
     * both on the direct {@code Writeable.writeTo} path and on the
     * {@code ElasticsearchException.writeException} nesting path.
     */
    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeString(contextType.toString());
        out.writeStringCollection(missingNodeIds);
    }

    /**
     * Returns the HTTP status code for this exception.
     *
     * @return {@link RestStatus#NOT_FOUND} to indicate the search context is not found
     */
    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    /**
     * Serializes the exception metadata to XContent format.
     * Adds context type and list of missing node IDs to the JSON output.
     *
     * @param builder the XContent builder to write to
     * @param params additional parameters for serialization
     * @throws IOException if an I/O error occurs while writing
     */
    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("context_type", contextType.toString());
        builder.startArray("missing_nodes");
        for (String nodeId : missingNodeIds) {
            builder.value(nodeId);
        }
        builder.endArray();
    }

    /**
     * Returns the set of node IDs that have left the cluster.
     *
     * @return an immutable set of node IDs
     */
    public Set<String> getMissingNodeIds() {
        return missingNodeIds;
    }

    /**
     * Returns the type of search context that is no longer valid.
     *
     * @return the context type
     */
    public ContextType getContextType() {
        return contextType;
    }

    private static String buildMessage(ContextType contextType, Set<String> missingNodeIds) {
        return "Search context of type [" + contextType + "] references nodes that have left the cluster: " + missingNodeIds;
    }
}
