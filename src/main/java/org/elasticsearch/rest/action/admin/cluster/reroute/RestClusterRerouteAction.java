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

package org.elasticsearch.rest.action.admin.cluster.reroute;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

/**
 */
public class RestClusterRerouteAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestClusterRerouteAction(Settings settings, Client client, RestController controller,
                                    SettingsFilter settingsFilter) {
        super(settings, client);
        this.settingsFilter = settingsFilter;
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/reroute", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final ClusterRerouteRequest clusterRerouteRequest = Requests.clusterRerouteRequest();
        clusterRerouteRequest.listenerThreaded(false);
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.timeout(request.paramAsTime("timeout", clusterRerouteRequest.timeout()));
        clusterRerouteRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterRerouteRequest.masterNodeTimeout()));
        if (request.hasContent()) {
            try {
                clusterRerouteRequest.source(request.content());
            } catch (Exception e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                }
                return;
            }
        }

        client.admin().cluster().reroute(clusterRerouteRequest, new AcknowledgedRestResponseActionListener<ClusterRerouteResponse>(request, channel, logger) {
            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterRerouteResponse response) throws IOException {
                builder.startObject("state");
                // by default, filter metadata
                if (request.param("filter_metadata") == null) {
                    request.params().put("filter_metadata", "true");
                }
                response.getState().settingsFilter(settingsFilter).toXContent(builder, request);
                builder.endObject();
            }

            @Override
            public void onFailure(Throwable e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to handle cluster reroute", e);
                }
                super.onFailure(e);
            }
        });
    }
}
