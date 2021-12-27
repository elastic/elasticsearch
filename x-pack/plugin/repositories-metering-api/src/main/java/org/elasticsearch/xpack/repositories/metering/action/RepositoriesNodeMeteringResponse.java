/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public final class RepositoriesNodeMeteringResponse extends BaseNodeResponse implements ToXContentFragment {

    final List<RepositoryStatsSnapshot> repositoryStatsSnapshots;

    public RepositoriesNodeMeteringResponse(DiscoveryNode node, List<RepositoryStatsSnapshot> repositoryStatsSnapshots) {
        super(node);
        this.repositoryStatsSnapshots = repositoryStatsSnapshots;
    }

    public RepositoriesNodeMeteringResponse(StreamInput in) throws IOException {
        super(in);
        this.repositoryStatsSnapshots = in.readList(RepositoryStatsSnapshot::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray(getNode().getId());
        for (RepositoryStatsSnapshot repositoryStats : repositoryStatsSnapshots) {
            repositoryStats.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(repositoryStatsSnapshots);
    }
}
