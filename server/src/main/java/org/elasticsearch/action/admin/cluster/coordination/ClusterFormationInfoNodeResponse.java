/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents the information a node can offer with regards to cluster formation.
 * It currently holds an optional warning message that the {@link org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper}
 * is sometimes emitting.
 */
public class ClusterFormationInfoNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private static final String WARNING_MESSAGE_FIELD_NAME = "warning";

    @Nullable
    private final String warningMessage;

    public ClusterFormationInfoNodeResponse(StreamInput in) throws IOException {
        super(in);
        warningMessage = in.readOptionalString();
    }

    public ClusterFormationInfoNodeResponse(
        DiscoveryNode node,
        @Nullable String warningMessage
    ) {
        super(node);
        this.warningMessage = warningMessage;
    }

    @Nullable
    public String warningMessage() {
        return warningMessage;
    }

    public static ClusterFormationInfoNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ClusterFormationInfoNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(warningMessage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (warningMessage != null) {
            builder.startObject(getNode().getId());
            builder.field(WARNING_MESSAGE_FIELD_NAME, warningMessage);
            builder.endObject();
        }
        return builder;
    }
}
