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

package org.elasticsearch.rest.action.admin.cluster.state;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.EnumSet;


/**
 *
 */
public class RestClusterStateAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestClusterStateAction(Settings settings, RestController controller, Client client, SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/state", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/state/{metric}", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/state/{metric}/{indices}", this);

        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        clusterStateRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterStateRequest.indicesOptions()));
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        final String[] indices = Strings.splitStringByCommaToArray(request.param("indices", "_all"));
        boolean isAllIndicesOnly = indices.length == 1 && "_all".equals(indices[0]);
        if (!isAllIndicesOnly) {
            clusterStateRequest.indices(indices);
        }

        if (request.hasParam("metric")) {
            EnumSet<ClusterState.Metric> metrics = ClusterState.Metric.parseString(request.param("metric"), true);
            // do not ask for what we do not need.
            clusterStateRequest.nodes(metrics.contains(ClusterState.Metric.NODES) || metrics.contains(ClusterState.Metric.MASTER_NODE));
            //there is no distinction in Java api between routing_table and routing_nodes, it's the same info set over the wire, one single flag to ask for it
            clusterStateRequest.routingTable(metrics.contains(ClusterState.Metric.ROUTING_TABLE) || metrics.contains(ClusterState.Metric.ROUTING_NODES));
            clusterStateRequest.metaData(metrics.contains(ClusterState.Metric.METADATA));
            clusterStateRequest.blocks(metrics.contains(ClusterState.Metric.BLOCKS));
            clusterStateRequest.customs(metrics.contains(ClusterState.Metric.CUSTOMS));
        }
        settingsFilter.addFilterSettingParams(request);

        client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.CLUSTER_NAME, response.getClusterName().value());
                response.getState().toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    static final class Fields {
        static final String CLUSTER_NAME = "cluster_name";
    }
}
