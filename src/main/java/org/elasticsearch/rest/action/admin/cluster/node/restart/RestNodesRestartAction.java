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

package org.elasticsearch.rest.action.admin.cluster.node.restart;

import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

/**
 *
 */
public class RestNodesRestartAction extends BaseRestHandler {

    @Inject
    public RestNodesRestartAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_cluster/nodes/_restart", this);
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/nodes/{nodeId}/_restart", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesRestartRequest nodesRestartRequest = new NodesRestartRequest(nodesIds);
        nodesRestartRequest.listenerThreaded(false);
        nodesRestartRequest.delay(request.paramAsTime("delay", nodesRestartRequest.delay()));
        client.admin().cluster().nodesRestart(nodesRestartRequest, new RestBuilderListener<NodesRestartResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesRestartResponse result, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("cluster_name", result.getClusterName().value());

                builder.startObject("nodes");
                for (NodesRestartResponse.NodeRestartResponse nodeInfo : result) {
                    builder.startObject(nodeInfo.getNode().id());
                    builder.field("name", nodeInfo.getNode().name());
                    builder.endObject();
                }
                builder.endObject();

                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
