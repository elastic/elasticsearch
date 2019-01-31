/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
