/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class ClusterStateRenderer extends AbstractRenderer<ClusterStateMarvelDoc> {

    private static final String[] FILTERS = {
            "cluster_state.version",
            "cluster_state.master_node",
            "cluster_state.status",
            "cluster_state.nodes",
            "cluster_state.shards",
    };

    public ClusterStateRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(ClusterStateMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATE);

        ClusterStateMarvelDoc.Payload payload = marvelDoc.payload();
        if (payload != null) {
            ClusterState clusterState = payload.getClusterState();
            if (clusterState != null) {
                builder.field(Fields.STATUS, payload.getStatus().name().toLowerCase(Locale.ROOT));

                clusterState.toXContent(builder, params);

                RoutingTable routingTable = clusterState.routingTable();
                if (routingTable != null) {
                    List<ShardRouting> shards = routingTable.allShards();
                    if (shards != null) {

                        builder.startArray(Fields.SHARDS);
                        for (ShardRouting shard : shards) {
                            shard.toXContent(builder, params);
                        }
                        builder.endArray();
                    }
                }
            }
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_STATE = new XContentBuilderString("cluster_state");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
    }
}
