/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.HashSet;
import java.util.concurrent.TimeoutException;

public final class TimeoutUtils {
    private TimeoutUtils() {
    }

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

    public static void ensureNoTimeouts(TimeValue collectionTimeout, BroadcastResponse broadcastResponse) {
        HashSet<String> timedOutNodeIds = null;
        for (DefaultShardOperationFailedException shardFailure : broadcastResponse.getShardFailures()) {
            final Throwable shardFailureCause = shardFailure.getCause();
            if (shardFailureCause instanceof FailedNodeException) {
                FailedNodeException failedNodeException = (FailedNodeException) shardFailureCause;
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
            throw new ElasticsearchTimeoutException((timedOutNodeIds.size() == 1 ? "node " : "nodes ") + timedOutNodeIds +
                    " did not respond within [" + collectionTimeout + "]");
        }
    }

}
