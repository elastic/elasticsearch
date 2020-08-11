/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public final class RepositoriesMetricsResponse extends BaseNodesResponse<RepositoriesNodeMetricsResponse> implements ToXContentFragment {

    public RepositoriesMetricsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public RepositoriesMetricsResponse(
        ClusterName clusterName,
        List<RepositoriesNodeMetricsResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<RepositoriesNodeMetricsResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(RepositoriesNodeMetricsResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<RepositoriesNodeMetricsResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (RepositoriesNodeMetricsResponse nodeStats : getNodes()) {
            nodeStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
