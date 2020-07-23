/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryStats;

import java.io.IOException;

public class RepositoryStatsNodeResponse extends BaseNodeResponse implements ToXContentObject {

    private final RepositoryStats repositoryStats;

    public RepositoryStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        repositoryStats = new RepositoryStats(in);
    }

    public RepositoryStatsNodeResponse(DiscoveryNode node, RepositoryStats repositoryStats) {
        super(node);
        this.repositoryStats = repositoryStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        repositoryStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (repositoryStats.requestCounts.isEmpty() == false) {
            builder.field("stats", repositoryStats.requestCounts);
        }
        builder.endObject();
        return builder;
    }

    public RepositoryStats getRepositoryStats() {
        return repositoryStats;
    }

}
