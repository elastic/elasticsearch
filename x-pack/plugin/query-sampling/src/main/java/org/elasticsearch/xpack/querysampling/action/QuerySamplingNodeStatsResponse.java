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
import org.elasticsearch.xpack.querysampling.QuerySamplingStats;

import java.io.IOException;

public final class QuerySamplingNodeStatsResponse extends BaseNodeResponse implements ToXContentFragment {

    private final QuerySamplingStats stats;

    public QuerySamplingNodeStatsResponse(DiscoveryNode node, QuerySamplingStats stats) {
        super(node);
        this.stats = stats;
    }

    public QuerySamplingNodeStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = new QuerySamplingStats(in);
    }

    public QuerySamplingStats getStats() {
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        stats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getNode().getId());
        builder.field("name", getNode().getName());
        stats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
