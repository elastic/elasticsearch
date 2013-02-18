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

package org.elasticsearch.rest.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

/**
 *
 */
public class RestNodesInfoAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestNodesInfoAction(Settings settings, Client client, RestController controller,
                               SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}", this);

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/settings", new RestSettingsHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/settings", new RestSettingsHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/os", new RestOsHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/os", new RestOsHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/process", new RestProcessHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/process", new RestProcessHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/jvm", new RestJvmHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/jvm", new RestJvmHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/thread_pool", new RestThreadPoolHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/thread_pool", new RestThreadPoolHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/network", new RestNetworkHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/network", new RestNetworkHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/transport", new RestTransportHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/transport", new RestTransportHandler());

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/http", new RestHttpHandler());
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/http", new RestHttpHandler());

        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = RestActions.splitNodes(request.param("nodeId"));
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodesIds);

        boolean clear = request.paramAsBoolean("clear", false);
        if (clear) {
            nodesInfoRequest.clear();
        }
        boolean all = request.paramAsBoolean("all", false);
        if (all) {
            nodesInfoRequest.all();
        }
        nodesInfoRequest.setSettings(request.paramAsBoolean("settings", nodesInfoRequest.isSettings()));
        nodesInfoRequest.setOs(request.paramAsBoolean("os", nodesInfoRequest.isOs()));
        nodesInfoRequest.setProcess(request.paramAsBoolean("process", nodesInfoRequest.isProcess()));
        nodesInfoRequest.setJvm(request.paramAsBoolean("jvm", nodesInfoRequest.isJvm()));
        nodesInfoRequest.setThreadPool(request.paramAsBoolean("thread_pool", nodesInfoRequest.isThreadPool()));
        nodesInfoRequest.setNetwork(request.paramAsBoolean("network", nodesInfoRequest.isNetwork()));
        nodesInfoRequest.setTransport(request.paramAsBoolean("transport", nodesInfoRequest.isTransport()));
        nodesInfoRequest.setHttp(request.paramAsBoolean("http", nodesInfoRequest.isHttp()));

        executeNodeRequest(request, channel, nodesInfoRequest);
    }

    void executeNodeRequest(final RestRequest request, final RestChannel channel, NodesInfoRequest nodesInfoRequest) {
        nodesInfoRequest.setListenerThreaded(false);
        client.admin().cluster().nodesInfo(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
            @Override
            public void onResponse(NodesInfoResponse response) {
                try {
                    response.settingsFilter(settingsFilter);
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
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

    class RestSettingsHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setSettings(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestOsHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setOs(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestProcessHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setProcess(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestJvmHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setJvm(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestThreadPoolHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setThreadPool(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestNetworkHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setNetwork(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestTransportHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setTransport(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }

    class RestHttpHandler implements RestHandler {
        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(RestActions.splitNodes(request.param("nodeId")));
            nodesInfoRequest.clear().setHttp(true);
            executeNodeRequest(request, channel, nodesInfoRequest);
        }
    }
}
