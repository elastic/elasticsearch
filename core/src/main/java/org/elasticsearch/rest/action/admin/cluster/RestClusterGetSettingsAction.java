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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

public class RestClusterGetSettingsAction extends BaseRestHandler {

    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public RestClusterGetSettingsAction(Settings settings, RestController controller, ClusterSettings clusterSettings,
            SettingsFilter settingsFilter) {
        super(settings);
        this.clusterSettings = clusterSettings;
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/settings", this);
        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false);
        final boolean renderDefaults = request.paramAsBoolean("include_defaults", false);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, renderResponse(response.getState(), renderDefaults, builder, request));
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private XContentBuilder renderResponse(ClusterState state, boolean renderDefaults, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
        builder.startObject();

        builder.startObject("persistent");
        state.metaData().persistentSettings().toXContent(builder, params);
        builder.endObject();

        builder.startObject("transient");
        state.metaData().transientSettings().toXContent(builder, params);
        builder.endObject();

        if (renderDefaults) {
            builder.startObject("defaults");
            settingsFilter.filter(clusterSettings.diff(state.metaData().settings(), this.settings)).toXContent(builder, params);
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }
}
