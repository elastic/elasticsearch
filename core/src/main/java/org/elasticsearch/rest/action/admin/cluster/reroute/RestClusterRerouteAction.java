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

package org.elasticsearch.rest.action.admin.cluster.reroute;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;
import java.util.EnumSet;

/**
 */
public class RestClusterRerouteAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    private static String DEFAULT_METRICS = Strings.arrayToCommaDelimitedString(EnumSet.complementOf(EnumSet.of(ClusterState.Metric.METADATA)).toArray());

    @Inject
    public RestClusterRerouteAction(Settings settings, RestController controller, Client client, SettingsFilter settingsFilter) {
        super(settings, controller, client);
        this.settingsFilter = settingsFilter;
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/reroute", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        final ClusterRerouteRequest clusterRerouteRequest = Requests.clusterRerouteRequest();
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.explain(request.paramAsBoolean("explain", clusterRerouteRequest.explain()));
        clusterRerouteRequest.timeout(request.paramAsTime("timeout", clusterRerouteRequest.timeout()));
        clusterRerouteRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterRerouteRequest.masterNodeTimeout()));
        if (request.hasContent()) {
            clusterRerouteRequest.source(request.content());
        }

        client.admin().cluster().reroute(clusterRerouteRequest, new AcknowledgedRestListener<ClusterRerouteResponse>(channel) {
            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterRerouteResponse response) throws IOException {
                builder.startObject("state");
                // by default, return everything but metadata
                if (request.param("metric") == null) {
                    request.params().put("metric", DEFAULT_METRICS);
                }
                settingsFilter.addFilterSettingParams(request);
                response.getState().toXContent(builder, request);
                builder.endObject();
                if (clusterRerouteRequest.explain()) {
                    assert response.getExplanations() != null;
                    response.getExplanations().toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
            }
        });
    }
}
