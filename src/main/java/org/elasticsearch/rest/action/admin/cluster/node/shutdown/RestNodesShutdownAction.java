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

package org.elasticsearch.rest.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

/**
 *
 */
public class RestNodesShutdownAction extends BaseRestHandler {

    @Inject
    public RestNodesShutdownAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_shutdown", this);
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/nodes/_shutdown", this);
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/nodes/{nodeId}/_shutdown", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesShutdownRequest nodesShutdownRequest = new NodesShutdownRequest(nodesIds);
        nodesShutdownRequest.listenerThreaded(false);
        nodesShutdownRequest.delay(request.paramAsTime("delay", nodesShutdownRequest.delay()));
        nodesShutdownRequest.exit(request.paramAsBoolean("exit", nodesShutdownRequest.exit()));
        client.admin().cluster().nodesShutdown(nodesShutdownRequest, new RestBuilderListener<NodesShutdownResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesShutdownResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("cluster_name", response.getClusterName().value());

                builder.startObject("nodes");
                for (DiscoveryNode node : response.getNodes()) {
                    builder.startObject(node.id(), XContentBuilder.FieldCaseConversion.NONE);
                    builder.field("name", node.name(), XContentBuilder.FieldCaseConversion.NONE);
                    builder.endObject();
                }
                builder.endObject();

                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
