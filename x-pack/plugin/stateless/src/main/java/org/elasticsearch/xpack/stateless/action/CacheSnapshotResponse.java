/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Response for the cache snapshot action. If the single targeted node responded successfully,
 * {@link #snapshotId()} returns its snapshot ID; otherwise fails with the node error.
 */
public class CacheSnapshotResponse extends BaseNodesResponse<CacheSnapshotNodeResponse> {

    public CacheSnapshotResponse(ClusterName clusterName, List<CacheSnapshotNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public CacheSnapshotResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<CacheSnapshotNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readCollectionAsList(CacheSnapshotNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<CacheSnapshotNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    /**
     * Returns the snapshot ID from the (single) responding node, or null if the node failed.
     */
    public String snapshotId() {
        List<CacheSnapshotNodeResponse> nodes = getNodes();
        return nodes.isEmpty() ? null : nodes.get(0).snapshotId();
    }
}
