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

package org.elasticsearch.rest.action.admin.cluster.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.rest.RestStatus.PRECONDITION_FAILED;

/**
 *
 */
public class RestClusterHealthAction extends BaseRestHandler {

    @Inject
    public RestClusterHealthAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.GET, "/_cluster/health", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/health/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterHealthRequest clusterHealthRequest = clusterHealthRequest(Strings.splitStringByCommaToArray(request.param("index")));
        clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
        clusterHealthRequest.listenerThreaded(false);
        try {
            clusterHealthRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterHealthRequest.masterNodeTimeout()));
            clusterHealthRequest.timeout(request.paramAsTime("timeout", clusterHealthRequest.timeout()));
            String waitForStatus = request.param("wait_for_status");
            if (waitForStatus != null) {
                clusterHealthRequest.waitForStatus(ClusterHealthStatus.valueOf(waitForStatus.toUpperCase(Locale.ROOT)));
            }
            clusterHealthRequest.waitForRelocatingShards(request.paramAsInt("wait_for_relocating_shards", clusterHealthRequest.waitForRelocatingShards()));
            clusterHealthRequest.waitForActiveShards(request.paramAsInt("wait_for_active_shards", clusterHealthRequest.waitForActiveShards()));
            clusterHealthRequest.waitForNodes(request.param("wait_for_nodes", clusterHealthRequest.waitForNodes()));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new BytesRestResponse(PRECONDITION_FAILED, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.admin().cluster().health(clusterHealthRequest, new ActionListener<ClusterHealthResponse>() {
            @Override
            public void onResponse(ClusterHealthResponse response) {
                try {
                    RestStatus status = RestStatus.OK;
                    // not sure..., we handle the health API, so we are not unavailable
                    // in any case, "/" should be used for
                    //if (response.status() == ClusterHealthStatus.RED) {
                    //    status = RestStatus.SERVICE_UNAVAILABLE;
                    //}
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();

                    channel.sendResponse(new BytesRestResponse(status, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new BytesRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
