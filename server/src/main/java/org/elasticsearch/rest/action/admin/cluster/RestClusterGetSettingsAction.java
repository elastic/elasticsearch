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

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.Set;

public class RestClusterGetSettingsAction extends BaseRestHandler {

    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public RestClusterGetSettingsAction(Settings settings, RestController controller, ClusterSettings clusterSettings,
            SettingsFilter settingsFilter) {
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/settings", this);
        this.settingsFilter = settingsFilter;
    }

    @Override
    public String getName() {
        return "cluster_get_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false);
        final boolean renderDefaults = request.paramAsBoolean("include_defaults", false);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, renderResponse(response.getState(), renderDefaults, builder, request));
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private XContentBuilder renderResponse(ClusterState state, boolean renderDefaults, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
        return response(state, renderDefaults, settingsFilter, clusterSettings, settings).toXContent(builder, params);
    }

    static ClusterGetSettingsResponse response(
            final ClusterState state,
            final boolean renderDefaults,
            final SettingsFilter settingsFilter,
            final ClusterSettings clusterSettings,
            final Settings settings) {
        return new ClusterGetSettingsResponse(
                settingsFilter.filter(state.metaData().persistentSettings()),
                settingsFilter.filter(state.metaData().transientSettings()),
                renderDefaults ? settingsFilter.filter(clusterSettings.diff(state.metaData().settings(), settings)) : Settings.EMPTY);
    }

}
