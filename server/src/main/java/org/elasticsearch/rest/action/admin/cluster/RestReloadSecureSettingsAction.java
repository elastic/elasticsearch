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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestReloadSecureSettingsAction extends BaseRestHandler {

    public RestReloadSecureSettingsAction(RestController controller) {
        controller.registerHandler(POST, "/_nodes/reload_secure_settings", this);
        controller.registerHandler(POST, "/_nodes/{nodeId}/reload_secure_settings", this);
    }

    @Override
    public String getName() {
        return "nodes_reload_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final NodesReloadSecureSettingsRequestBuilder nodesRequestBuilder = client.admin()
                .cluster()
                .prepareReloadSecureSettings()
                .setTimeout(request.param("timeout"))
                .setNodesIds(nodesIds);
        return channel -> nodesRequestBuilder
                .execute(new RestBuilderListener<NodesReloadSecureSettingsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesReloadSecureSettingsResponse response, XContentBuilder builder)
                            throws Exception {
                        builder.startObject();
                        {
                            RestActions.buildNodesHeader(builder, channel.request(), response);
                            builder.field("cluster_name", response.getClusterName().value());
                            response.toXContent(builder, channel.request());
                        }
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
