/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * What one node did: how many of its sampled queries got their ground truth and how many failed.
 */
public final class QuerySamplingNodeGroundTruthResponse extends BaseNodeResponse implements ToXContentFragment {

    private final int computed;
    private final int failed;

    public QuerySamplingNodeGroundTruthResponse(DiscoveryNode node, int computed, int failed) {
        super(node);
        this.computed = computed;
        this.failed = failed;
    }

    public QuerySamplingNodeGroundTruthResponse(StreamInput in) throws IOException {
        super(in);
        this.computed = in.readVInt();
        this.failed = in.readVInt();
    }

    public int computed() {
        return computed;
    }

    public int failed() {
        return failed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(computed);
        out.writeVInt(failed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getNode().getId());
        builder.field("name", getNode().getName());
        builder.field("computed", computed);
        builder.field("failed", failed);
        builder.endObject();
        return builder;
    }
}
