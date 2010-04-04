/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.rest.action.admin.cluster.health;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.rest.RestResponse.Status.*;

/**
 * @author kimchy (shay.banon)
 */
public class RestClusterHealthAction extends BaseRestHandler {

    @Inject public RestClusterHealthAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.GET, "/_cluster/health", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/health/{index}", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterHealthRequest clusterHealthRequest = clusterHealth(RestActions.splitIndices(request.param("index")));
        int level = 0;
        try {
            clusterHealthRequest.timeout(request.paramAsTime("timeout", clusterHealthRequest.timeout()));
            String waitForStatus = request.param("wait_for_status");
            if (waitForStatus != null) {
                clusterHealthRequest.waitForStatus(ClusterHealthStatus.valueOf(waitForStatus.toUpperCase()));
            }
            clusterHealthRequest.waitForRelocatingShards(request.paramAsInt("wait_for_relocating_shards", clusterHealthRequest.waitForRelocatingShards()));
            String sLevel = request.param("level");
            if (sLevel != null) {
                if ("cluster".equals("sLevel")) {
                    level = 0;
                } else if ("indices".equals(sLevel)) {
                    level = 1;
                } else if ("shards".equals(sLevel)) {
                    level = 2;
                }
            }
        } catch (Exception e) {
            try {
                JsonBuilder builder = RestJsonBuilder.restJsonBuilder(request);
                channel.sendResponse(new JsonRestResponse(request, PRECONDITION_FAILED, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        final int fLevel = level;
        client.admin().cluster().health(clusterHealthRequest, new ActionListener<ClusterHealthResponse>() {
            @Override public void onResponse(ClusterHealthResponse response) {
                try {
                    JsonBuilder builder = RestJsonBuilder.restJsonBuilder(request);
                    builder.startObject();

                    builder.field("status", response.status().name().toLowerCase());
                    builder.field("timed_out", response.timedOut());
                    builder.field("active_primary_shards", response.activePrimaryShards());
                    builder.field("active_shards", response.activeShards());
                    builder.field("relocating_shards", response.relocatingShards());

                    if (!response.validationFailures().isEmpty()) {
                        builder.startArray("validation_failures");
                        for (String validationFailure : response.validationFailures()) {
                            builder.value(validationFailure);
                        }
                        // if we don't print index level information, still print the index validation failures
                        // so we know why the status is red
                        if (fLevel == 0) {
                            for (ClusterIndexHealth indexHealth : response) {
                                builder.startObject(indexHealth.index());

                                if (!indexHealth.validationFailures().isEmpty()) {
                                    builder.startArray("validation_failures");
                                    for (String validationFailure : indexHealth.validationFailures()) {
                                        builder.value(validationFailure);
                                    }
                                    builder.endArray();
                                }

                                builder.endObject();
                            }
                        }
                        builder.endArray();
                    }

                    if (fLevel > 0) {
                        builder.startObject("indices");
                        for (ClusterIndexHealth indexHealth : response) {
                            builder.startObject(indexHealth.index());

                            builder.field("status", indexHealth.status().name().toLowerCase());
                            builder.field("number_of_shards", indexHealth.numberOfShards());
                            builder.field("number_of_replicas", indexHealth.numberOfReplicas());
                            builder.field("active_primary_shards", indexHealth.activePrimaryShards());
                            builder.field("active_shards", indexHealth.activeShards());
                            builder.field("relocating_shards", indexHealth.relocatingShards());

                            if (!indexHealth.validationFailures().isEmpty()) {
                                builder.startArray("validation_failures");
                                for (String validationFailure : indexHealth.validationFailures()) {
                                    builder.value(validationFailure);
                                }
                                builder.endArray();
                            }

                            if (fLevel > 1) {
                                builder.startObject("shards");

                                for (ClusterShardHealth shardHealth : indexHealth) {
                                    builder.startObject(Integer.toString(shardHealth.id()));

                                    builder.field("status", shardHealth.status().name().toLowerCase());
                                    builder.field("primary_active", shardHealth.primaryActive());
                                    builder.field("active_shards", shardHealth.activeShards());
                                    builder.field("relocating_shards", shardHealth.relocatingShards());

                                    builder.endObject();
                                }

                                builder.endObject();
                            }

                            builder.endObject();
                        }
                        builder.endObject();
                    }

                    builder.endObject();

                    channel.sendResponse(new JsonRestResponse(request, RestResponse.Status.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
