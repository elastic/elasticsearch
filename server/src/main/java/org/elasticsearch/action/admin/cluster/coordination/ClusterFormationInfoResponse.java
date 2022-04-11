/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ClusterFormationInfoResponse extends BaseNodesResponse<ClusterFormationInfoNodeResponse> implements ToXContentFragment {

    public ClusterFormationInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ClusterFormationInfoResponse(ClusterName clusterName,
                                        List<ClusterFormationInfoNodeResponse> nodes,
                                        List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ClusterFormationInfoNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClusterFormationInfoNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClusterFormationInfoNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (ClusterFormationInfoNodeResponse nodeResponse : getNodes()) {
            nodeResponse.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
