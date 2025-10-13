/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * An extension interface to {@link ClusterStateUpdateTask} that allows the caller to be notified after the master has
 * computed, published, accepted, committed, and applied the cluster state update AND only after the rest of the nodes
 * (or a specified subset) have also accepted and applied the cluster state update.
 */
public abstract class AckedClusterStateUpdateTask extends ClusterStateUpdateTask implements ClusterStateAckListener {

    private final ActionListener<AcknowledgedResponse> listener;
    private final TimeValue ackTimeout;

    protected AckedClusterStateUpdateTask(AcknowledgedRequest<?> request, ActionListener<? extends AcknowledgedResponse> listener) {
        this(Priority.NORMAL, request.masterNodeTimeout(), request.ackTimeout(), listener);
    }

    protected AckedClusterStateUpdateTask(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<? extends AcknowledgedResponse> listener
    ) {
        this(Priority.NORMAL, masterNodeTimeout, ackTimeout, listener);
    }

    protected AckedClusterStateUpdateTask(
        Priority priority,
        AcknowledgedRequest<?> request,
        ActionListener<? extends AcknowledgedResponse> listener
    ) {
        this(priority, request.masterNodeTimeout(), request.ackTimeout(), listener);
    }

    @SuppressWarnings("unchecked")
    protected AckedClusterStateUpdateTask(
        Priority priority,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<? extends AcknowledgedResponse> listener
    ) {
        super(priority, masterNodeTimeout);
        this.listener = (ActionListener<AcknowledgedResponse>) listener;
        this.ackTimeout = Objects.requireNonNull(ackTimeout);
    }

    /**
     * Called to determine which nodes the acknowledgement is expected from
     *
     * @param discoveryNode a node
     * @return true if the node is expected to send ack back, false otherwise
     */
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return true;
    }

    @Override
    public void onAllNodesAcked() {
        listener.onResponse(newResponse(true));
    }

    @Override
    public void onAckFailure(Exception e) {
        listener.onResponse(newResponse(false));
    }

    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return AcknowledgedResponse.of(acknowledged);
    }

    public void onAckTimeout() {
        listener.onResponse(newResponse(false));
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    public final TimeValue ackTimeout() {
        return ackTimeout;
    }
}
