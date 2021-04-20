/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class NodesResponseHeaderTestUtils {

    public static void toXContent(NodesResponseHeader header, String clusterName, XContentBuilder builder) throws IOException {
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
        builder.field("cluster_name", clusterName);
    }

}
