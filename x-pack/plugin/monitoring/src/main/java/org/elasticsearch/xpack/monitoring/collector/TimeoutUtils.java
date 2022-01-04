/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.collector;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.HashSet;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for identifying timeouts in responses to collection requests, since we prefer to fail the whole collection attempt if any of
 * the involved nodes times out.
 */
public final class TimeoutUtils {
    private TimeoutUtils() {}

    /**
     * @throws ElasticsearchTimeoutException iff the {@code response} contains any node-level timeout. The exception message identifies the
     *                                       nodes that timed out and mentions {@code collectionTimeout}.
     */
    public static <T extends BaseNodeResponse> void ensureNoTimeouts(TimeValue collectionTimeout, BaseNodesResponse<T> response) {
        HashSet<String> timedOutNodeIds = null;
        for (FailedNodeException failedNodeException : response.failures()) {
            if (isTimeoutFailure(failedNodeException)) {
                if (timedOutNodeIds == null) {
                    timedOutNodeIds = new HashSet<>();
                }
                timedOutNodeIds.add(failedNodeException.nodeId());
            }
        }
        ensureNoTimeouts(collectionTimeout, timedOutNodeIds);
    }

    /**
     * @throws ElasticsearchTimeoutException iff the {@code response} contains any node-level timeout. The exception message identifies the
     *                                       nodes that timed out and mentions {@code collectionTimeout}.
     */
    public static void ensureNoTimeouts(TimeValue collectionTimeout, BaseTasksResponse response) {
        HashSet<String> timedOutNodeIds = null;
        for (ElasticsearchException nodeFailure : response.getNodeFailures()) {
            if (nodeFailure instanceof FailedNodeException failedNodeException) {
                if (isTimeoutFailure(failedNodeException)) {
                    if (timedOutNodeIds == null) {
                        timedOutNodeIds = new HashSet<>();
                    }
                    timedOutNodeIds.add(failedNodeException.nodeId());
                }
            }
        }
        ensureNoTimeouts(collectionTimeout, timedOutNodeIds);
    }

    /**
     * @throws ElasticsearchTimeoutException iff the {@code response} contains any node-level timeout. The exception message identifies the
     *                                       nodes that timed out and mentions {@code collectionTimeout}.
     */
    public static void ensureNoTimeouts(TimeValue collectionTimeout, BroadcastResponse response) {
        HashSet<String> timedOutNodeIds = null;
        for (DefaultShardOperationFailedException shardFailure : response.getShardFailures()) {
            final Throwable shardFailureCause = shardFailure.getCause();
            if (shardFailureCause instanceof FailedNodeException failedNodeException) {
                if (isTimeoutFailure(failedNodeException)) {
                    if (timedOutNodeIds == null) {
                        timedOutNodeIds = new HashSet<>();
                    }
                    timedOutNodeIds.add(failedNodeException.nodeId());
                }
            }
        }
        ensureNoTimeouts(collectionTimeout, timedOutNodeIds);
    }

    private static boolean isTimeoutFailure(FailedNodeException failedNodeException) {
        final Throwable cause = failedNodeException.getCause();
        return cause instanceof ElasticsearchTimeoutException
            || cause instanceof TimeoutException
            || cause instanceof ReceiveTimeoutTransportException;
    }

    private static void ensureNoTimeouts(TimeValue collectionTimeout, HashSet<String> timedOutNodeIds) {
        if (timedOutNodeIds != null) {
            throw new ElasticsearchTimeoutException(
                (timedOutNodeIds.size() == 1 ? "node " : "nodes ") + timedOutNodeIds + " did not respond within [" + collectionTimeout + "]"
            );
        }
    }

}
