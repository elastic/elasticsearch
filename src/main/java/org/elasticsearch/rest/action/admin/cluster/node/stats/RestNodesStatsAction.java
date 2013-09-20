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


/**
 *
 */
public class RestNodesStatsAction extends BaseRestHandler {

    @Inject
    public RestNodesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}/stats", this);

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats", this);

        RestIndicesHandler indicesHandler = new RestIndicesHandler(new CommonStatsFlags().all());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/indices", indicesHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/indices", indicesHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/indices/stats", indicesHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/indices/stats", indicesHandler);
        for (Flag flag : CommonStatsFlags.Flag.values()) {
            indicesHandler = new RestIndicesHandler(new CommonStatsFlags().clear().set(flag, true));
            controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/indices/" + flag.getRestName(), indicesHandler);
            controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/indices/" + flag.getRestName(), indicesHandler);
            controller.registerHandler(RestRequest.Method.GET, "/_nodes/indices/" + flag.getRestName() + "/stats", indicesHandler);
            controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/indices/" + flag.getRestName() + "/stats", indicesHandler);
            if (flag == Flag.FieldData || flag == Flag.Completion) {
                // add field specific endpoints
                controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/indices/" + flag.getRestName() + "/{fields}", indicesHandler);
                controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/indices/" + flag.getRestName() + "/{fields}", indicesHandler);
                controller.registerHandler(RestRequest.Method.GET, "/_nodes/indices/" + flag.getRestName() + "/{fields}/stats", indicesHandler);
                controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/indices/" + flag.getRestName() + "/{fields}/stats", indicesHandler);
            }
        }

        RestOsHandler osHandler = new RestOsHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/os", osHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/os", osHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/os/stats", osHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/os/stats", osHandler);

        RestProcessHandler processHandler = new RestProcessHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/process", processHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/process", processHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/process/stats", processHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/process/stats", processHandler);

        RestJvmHandler jvmHandler = new RestJvmHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/jvm", jvmHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/jvm", jvmHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/jvm/stats", jvmHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/jvm/stats", jvmHandler);

        RestThreadPoolHandler threadPoolHandler = new RestThreadPoolHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/thread_pool", threadPoolHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/thread_pool", threadPoolHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/thread_pool/stats", threadPoolHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/thread_pool/stats", threadPoolHandler);

        RestNetworkHandler networkHandler = new RestNetworkHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/network", networkHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/network", networkHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/network/stats", networkHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/network/stats", networkHandler);

        RestFsHandler fsHandler = new RestFsHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/fs", fsHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/fs", fsHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/fs/stats", fsHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/fs/stats", fsHandler);

        RestTransportHandler transportHandler = new RestTransportHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/transport", transportHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/transport", transportHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/transport/stats", transportHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/transport/stats", transportHandler);

        RestHttpHandler httpHandler = new RestHttpHandler();
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/stats/http", httpHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/stats/http", httpHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/http/stats", httpHandler);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/http/stats", httpHandler);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        boolean clear = request.paramAsBoolean("clear", false);
        if (clear) {
            nodesStatsRequest.clear();
        }
        boolean all = request.paramAsBoolean("all", false);
        if (all) {
            nodesStatsRequest.all();
        }
        if (request.hasParam("indices")) {
            nodesStatsRequest.indices(request.paramAsBoolean("indices", false));
        }
        nodesStatsRequest.os(request.paramAsBoolean("os", nodesStatsRequest.os()));
        nodesStatsRequest.process(request.paramAsBoolean("process", nodesStatsRequest.process()));
        nodesStatsRequest.jvm(request.paramAsBoolean("jvm", nodesStatsRequest.jvm()));
        nodesStatsRequest.threadPool(request.paramAsBoolean("thread_pool", nodesStatsRequest.threadPool()));
        nodesStatsRequest.network(request.paramAsBoolean("network", nodesStatsRequest.network()));
        nodesStatsRequest.fs(request.paramAsBoolean("fs", nodesStatsRequest.fs()));
        nodesStatsRequest.transport(request.paramAsBoolean("transport", nodesStatsRequest.transport()));
        nodesStatsRequest.http(request.paramAsBoolean("http", nodesStatsRequest.http()));
        executeNodeStats(request, channel, nodesStatsRequest);
    }

    void executeNodeStats(final RestRequest request, final RestChannel channel, final NodesStatsRequest nodesStatsRequest) {
        nodesStatsRequest.listenerThreaded(false);
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

    class RestIndicesHandler implements RestHandler {

        private final CommonStatsFlags flags;

        RestIndicesHandler(CommonStatsFlags flags) {
            this.flags = flags;
        }

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            CommonStatsFlags flags = this.flags;
            if (flags.isSet(Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
                flags = flags.clone().fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
            } else if (flags.isSet(Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
                flags = flags.clone().completionDataFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
            }
            nodesStatsRequest.clear().indices(flags);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestOsHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().os(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestProcessHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().process(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestJvmHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().jvm(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestThreadPoolHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().threadPool(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestNetworkHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().network(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestFsHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().fs(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestTransportHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().transport(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }

    class RestHttpHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
            nodesStatsRequest.clear().http(true);
            executeNodeStats(request, channel, nodesStatsRequest);
        }
    }
}
