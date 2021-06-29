/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.eql;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class EqlStatsResponseToXContent implements ToXContent {

    private final EqlStatsResponse response;

    public EqlStatsResponseToXContent(EqlStatsResponse response) {
        this.response = response;
    }

    public EqlStatsResponse unwrap() {
        return this.response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        NodesResponseHeader header = response.getHeader();
        if (header != null) {
            builder.startObject("_nodes");
            builder.field("total", header.getTotal());
            builder.field("successful", header.getSuccessful());
            builder.field("failed", header.getFailed());

            if (header.getFailures().isEmpty() == false) {
                builder.startArray("failures");
                for (ElasticsearchException failure : header.getFailures()) {
                    builder.startObject();
                    failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                }
                builder.endArray();
            }

            builder.endObject();
        }

        builder.field("cluster_name", response.getClusterName());

        List<EqlStatsResponse.Node> nodes = response.getNodes();
        if (nodes != null) {
            builder.startArray("stats");
            for (EqlStatsResponse.Node node : nodes) {
                builder.startObject();
                if (node.getStats() != null) {
                    builder.field("stats", node.getStats());
                }
                builder.endObject();
            }
            builder.endArray();
        }

        return builder;
    }
}
