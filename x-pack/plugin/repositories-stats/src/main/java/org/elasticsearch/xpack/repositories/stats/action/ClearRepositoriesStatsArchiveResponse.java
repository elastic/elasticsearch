/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ClearRepositoriesStatsArchiveResponse extends BaseNodesResponse<ClearRepositoriesStatsArchiveNodeResponse>
    implements
        ToXContentObject {

    public ClearRepositoriesStatsArchiveResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ClearRepositoriesStatsArchiveResponse(
        ClusterName clusterName,
        List<ClearRepositoriesStatsArchiveNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ClearRepositoriesStatsArchiveNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClearRepositoriesStatsArchiveNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearRepositoriesStatsArchiveNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (failures().isEmpty() == false) {
            builder.startArray("failures");
            for (FailedNodeException failure : failures()) {
                builder.startObject();
                failure.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }
}
