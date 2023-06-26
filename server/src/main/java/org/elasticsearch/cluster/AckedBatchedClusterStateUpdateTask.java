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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;

/**
 * This class models a cluster state update task that notifies an AcknowledgedResponse listener when
 * all the nodes have acknowledged the cluster state update request. It works with batched cluster state updates.
 */
public class AckedBatchedClusterStateUpdateTask implements ClusterStateTaskListener, ClusterStateAckListener {

    private final ActionListener<AcknowledgedResponse> listener;
    private final TimeValue ackTimeout;

    public AckedBatchedClusterStateUpdateTask(TimeValue ackTimeout, ActionListener<AcknowledgedResponse> listener) {
        this.ackTimeout = ackTimeout;
        this.listener = listener;
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return true;
    }

    @Override
    public void onAllNodesAcked() {
        listener.onResponse(AcknowledgedResponse.TRUE);
    }

    @Override
    public void onAckFailure(Exception e) {
        listener.onResponse(AcknowledgedResponse.FALSE);
    }

    @Override
    public void onAckTimeout() {
        listener.onResponse(AcknowledgedResponse.FALSE);
    }

    @Override
    public TimeValue ackTimeout() {
        return ackTimeout;
    }
}
