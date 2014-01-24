/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.admin.cluster.node.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;


/**
 *
 */
public class RestNodesStatsAction extends BaseRestHandler {

    @Inject
    public RestNodesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_nodes/stats", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats", this);

        controller.registerHandler(GET, "/_nodes/stats/{metric}", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats/{metric}", this);

        controller.registerHandler(GET, "/_nodes/stats/{metric}/{indexMetric}", this);
        controller.registerHandler(GET, "/_nodes/stats/{metric}/{indexMetric}/{fields}", this);

        controller.registerHandler(GET, "/_nodes/{nodeId}/stats/{metric}/{indexMetric}", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats/{metric}/{indexMetric}/{fields}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.splitStringByCommaToSet(request.param("metric", "_all"));

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.listenerThreaded(false);

        if (metrics.size() == 1 && metrics.contains("_all")) {
            nodesStatsRequest.all();
            nodesStatsRequest.indices(CommonStatsFlags.ALL);
        } else {
            nodesStatsRequest.clear();
            nodesStatsRequest.os(metrics.contains("os"));
            nodesStatsRequest.jvm(metrics.contains("jvm"));
            nodesStatsRequest.threadPool(metrics.contains("thread_pool"));
            nodesStatsRequest.network(metrics.contains("network"));
            nodesStatsRequest.fs(metrics.contains("fs"));
            nodesStatsRequest.transport(metrics.contains("transport"));
            nodesStatsRequest.http(metrics.contains("http"));
            nodesStatsRequest.indices(metrics.contains("indices"));
            nodesStatsRequest.process(metrics.contains("process"));
            nodesStatsRequest.breaker(metrics.contains("breaker"));

            // check for index specific metrics
            if (metrics.contains("indices")) {
                Set<String> indexMetrics = Strings.splitStringByCommaToSet(request.param("indexMetric", "_all"));
                if (indexMetrics.size() == 1 && indexMetrics.contains("_all")) {
                    nodesStatsRequest.indices(CommonStatsFlags.ALL);
                } else {
                    CommonStatsFlags flags = new CommonStatsFlags();
                    for (Flag flag : CommonStatsFlags.Flag.values()) {
                        flags.set(flag, indexMetrics.contains(flag.getRestName()));
                    }
                    nodesStatsRequest.indices(flags);
                }
            }
        }

        if (nodesStatsRequest.indices().isSet(Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            nodesStatsRequest.indices().fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            nodesStatsRequest.indices().completionDataFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Search) && (request.hasParam("groups"))) {
            nodesStatsRequest.indices().groups(request.paramAsStringArray("groups", null));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Indexing) && (request.hasParam("types"))) {
            nodesStatsRequest.indices().types(request.paramAsStringArray("types", null));
        }

        client.admin().cluster().nodesStats(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
