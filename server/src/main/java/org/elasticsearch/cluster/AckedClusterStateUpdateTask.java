/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;

/**
 * An extension interface to {@link ClusterStateUpdateTask} that allows to be notified when
 * all the nodes have acknowledged a cluster state update request
 */
public abstract class AckedClusterStateUpdateTask extends ClusterStateUpdateTask implements AckedClusterStateTaskListener {

    private final ActionListener<AcknowledgedResponse> listener;
    private final AckedRequest request;

    protected AckedClusterStateUpdateTask(AckedRequest request, ActionListener<? extends AcknowledgedResponse> listener) {
        this(Priority.NORMAL, request, listener);
    }

    @SuppressWarnings("unchecked")
    protected AckedClusterStateUpdateTask(Priority priority, AckedRequest request,
                                          ActionListener<? extends AcknowledgedResponse> listener) {
        super(priority, request.masterNodeTimeout());
        this.listener = (ActionListener<AcknowledgedResponse>) listener;
        this.request = request;
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

    /**
     * Called once all the nodes have acknowledged the cluster state update request. Must be
     * very lightweight execution, since it gets executed on the cluster service thread.
     *
     * @param e optional error that might have been thrown
     */
    public void onAllNodesAcked(@Nullable Exception e) {
        listener.onResponse(newResponse(e == null));
    }

    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return AcknowledgedResponse.of(acknowledged);
    }

    /**
     * Called once the acknowledgement timeout defined by
     * {@link AckedClusterStateUpdateTask#ackTimeout()} has expired
     */
    public void onAckTimeout() {
        listener.onResponse(newResponse(false));
    }

    @Override
    public void onFailure(String source, Exception e) {
        listener.onFailure(e);
    }

    /**
     * Acknowledgement timeout, maximum time interval to wait for acknowledgements
     */
    public final TimeValue ackTimeout() {
        return request.ackTimeout();
    }
}
