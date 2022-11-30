/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Cluster state update request that allows to close one or more indices
 */
public class DeleteIndexClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<DeleteIndexClusterStateUpdateRequest>
    implements
        ClusterStateAckListener,
        ClusterStateTaskListener {

    private final ActionListener<AcknowledgedResponse> listener;

    public DeleteIndexClusterStateUpdateRequest(ActionListener<AcknowledgedResponse> listener) {
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
}
