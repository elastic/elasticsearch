/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats.http;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class HttpStatsContent extends BaseNodeResponse implements ChunkedToXContent {

    private final long timestamp;

    private final HttpStats stats;

    public HttpStatsContent(DiscoveryNode node, long timestamp, HttpStats stats) {
        super(node);
        this.timestamp = timestamp;
        this.stats = stats;
    }

    protected HttpStatsContent(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        stats = in.readOptionalWriteable(HttpStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        out.writeOptionalWriteable(stats);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Objects.requireNonNullElse(stats, ChunkedToXContent.EMPTY).toXContentChunked(params);
    }

    public HttpStats getStats() {
        return stats;
    }
}
