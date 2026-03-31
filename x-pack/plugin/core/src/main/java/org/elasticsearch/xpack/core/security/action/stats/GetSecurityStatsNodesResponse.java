/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class GetSecurityStatsNodesResponse extends BaseNodesResponse<GetSecurityStatsNodeResponse> implements ToXContentObject {

    public GetSecurityStatsNodesResponse(
        final ClusterName clusterName,
        final List<GetSecurityStatsNodeResponse> nodes,
        final List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<GetSecurityStatsNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readCollectionAsList(GetSecurityStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<GetSecurityStatsNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (GetSecurityStatsNodeResponse response : getNodes()) {
            builder.startObject(response.getNode().getId());
            response.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
