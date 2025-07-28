/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public final class RepositoriesMeteringResponse extends BaseNodesResponse<RepositoriesNodeMeteringResponse> implements ToXContentFragment {

    public RepositoriesMeteringResponse(
        ClusterName clusterName,
        List<RepositoriesNodeMeteringResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<RepositoriesNodeMeteringResponse> readNodesFrom(StreamInput in) {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<RepositoriesNodeMeteringResponse> nodes) {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (RepositoriesNodeMeteringResponse nodeStats : getNodes()) {
            nodeStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
