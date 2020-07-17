/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryStats;

import java.io.IOException;
import java.util.List;

public class RepositoryStatsResponse extends BaseNodesResponse<RepositoryStatsNodeResponse> implements ToXContentObject {

    private final RepositoryStats globalStats;

    public RepositoryStatsResponse(ClusterName clusterName, List<RepositoryStatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        globalStats = computeGlobalStats(getNodes());
    }

    public RepositoryStatsResponse(StreamInput in) throws IOException {
        super(in);
        globalStats = computeGlobalStats(getNodes());
    }

    private static RepositoryStats computeGlobalStats(List<RepositoryStatsNodeResponse> nodes) {
        if (nodes.isEmpty()) {
            return RepositoryStats.EMPTY_STATS;
        } else {
            return nodes.stream().map(RepositoryStatsNodeResponse::getRepositoryStats).reduce(RepositoryStats::merge).get();
        }
    }

    @Override
    protected List<RepositoryStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(RepositoryStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<RepositoryStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("_all", globalStats.requestCounts);
        builder.startArray("nodes");
        for (RepositoryStatsNodeResponse node : getNodes()) {
            node.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();

        return builder;
    }
}
