/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats.http;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesXContentResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HttpStatsNodesResponse extends BaseNodesXContentResponse<HttpStatsContent> {

    public HttpStatsNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public HttpStatsNodesResponse(ClusterName clusterName, List<HttpStatsContent> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<HttpStatsContent> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(HttpStatsContent::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<HttpStatsContent> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    protected Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams) {
        return Iterators.concat(
            getNodes().stream()
                .map(HttpStatsContent::getStats)
                .reduce(new HttpStats(List.of(), 0, 0), HttpStats::merge)
                .toXContentChunked(outerParams)
        );
    }
}
