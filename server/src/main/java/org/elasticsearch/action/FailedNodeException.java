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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Exception indicating that an operation failed on a specific node in the cluster.
 * This exception wraps node-specific failures that occur during distributed operations,
 * allowing the failure to be associated with the node where it occurred.
 *
 * <p>When an action is executed across multiple nodes (e.g., gathering node statistics,
 * performing cluster-wide operations), individual nodes may fail. This exception captures
 * both the failure details and the identity of the failed node.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing a FailedNodeException
 * try {
 *     performNodeOperation();
 * } catch (Exception e) {
 *     throw new FailedNodeException(
 *         nodeId,
 *         "Operation failed on node: " + nodeId,
 *         e
 *     );
 * }
 *
 * // Handling FailedNodeExceptions
 * try {
 *     executeMultiNodeOperation();
 * } catch (FailedNodeException e) {
 *     logger.error("Operation failed on node {}: {}",
 *         e.nodeId(), e.getMessage());
 * }
 * }</pre>
 */
public class FailedNodeException extends ElasticsearchException {

    private final String nodeId;

    /**
     * Constructs a new failed node exception with the specified node ID, message, and cause.
     *
     * @param nodeId the ID of the node where the failure occurred
     * @param msg the detail message explaining the failure
     * @param cause the underlying cause of the failure, or {@code null} if none
     */
    public FailedNodeException(String nodeId, String msg, Throwable cause) {
        super(msg, cause);
        this.nodeId = nodeId;
    }

    /**
     * Returns the ID of the node where the failure occurred.
     *
     * @return the node ID
     */
    public String nodeId() {
        return this.nodeId;
    }

    /**
     * Constructs a new failed node exception by reading from a stream input.
     * This constructor is used for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public FailedNodeException(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readOptionalString();
    }

    /**
     * Writes this exception to the specified stream output for serialization.
     *
     * @param out the stream output to write to
     * @param nestedExceptionsWriter the writer for nested exceptions
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(nodeId);
    }

    /**
     * Writes exception metadata to the XContent builder, including the node ID.
     *
     * @param builder the XContent builder to write to
     * @param params the serialization parameters
     * @throws IOException if an I/O error occurs while writing
     */
    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", nodeId);
    }
}
