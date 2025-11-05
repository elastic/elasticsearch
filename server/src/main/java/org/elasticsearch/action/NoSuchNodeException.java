/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown when an operation attempts to target a node that does not exist in the cluster.
 * This typically occurs when a node ID is specified that is either invalid or refers to a node that
 * has been removed from the cluster.
 *
 * <p>This exception extends {@link FailedNodeException} and represents a specific case where the
 * failure is due to the node not existing rather than the node failing during an operation.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing when a node is not found
 * if (clusterState.nodes().get(nodeId) == null) {
 *     throw new NoSuchNodeException(nodeId);
 * }
 *
 * // Handling NoSuchNodeException
 * try {
 *     performOperationOnNode(nodeId);
 * } catch (NoSuchNodeException e) {
 *     logger.warn("Node {} does not exist in the cluster", e.nodeId());
 *     // Handle missing node, perhaps retry with a different node
 * }
 * }</pre>
 */
public class NoSuchNodeException extends FailedNodeException {

    /**
     * Constructs a new no such node exception for the specified node ID.
     *
     * @param nodeId the ID of the node that does not exist
     */
    public NoSuchNodeException(String nodeId) {
        super(nodeId, "No such node [" + nodeId + "]", null);
    }

    /**
     * Constructs a new no such node exception by reading from a stream input.
     * This constructor is used for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public NoSuchNodeException(StreamInput in) throws IOException {
        super(in);
    }
}
