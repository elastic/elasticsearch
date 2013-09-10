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

package org.elasticsearch.rest.action.admin.cluster.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
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
        int level = 0;
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
            String sLevel = request.param("level");
            if (sLevel != null) {
                if ("cluster".equals(sLevel)) {
                    level = 0;
                } else if ("indices".equals(sLevel)) {
                    level = 1;
                } else if ("shards".equals(sLevel)) {
                    level = 2;
                }
            }
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, PRECONDITION_FAILED, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        final int fLevel = level;
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

                    builder.field(Fields.CLUSTER_NAME, response.getClusterName());
                    builder.field(Fields.STATUS, response.getStatus().name().toLowerCase(Locale.ROOT));
                    builder.field(Fields.TIMED_OUT, response.isTimedOut());
                    builder.field(Fields.NUMBER_OF_NODES, response.getNumberOfNodes());
                    builder.field(Fields.NUMBER_OF_DATA_NODES, response.getNumberOfDataNodes());
                    builder.field(Fields.ACTIVE_PRIMARY_SHARDS, response.getActivePrimaryShards());
                    builder.field(Fields.ACTIVE_SHARDS, response.getActiveShards());
                    builder.field(Fields.RELOCATING_SHARDS, response.getRelocatingShards());
                    builder.field(Fields.INITIALIZING_SHARDS, response.getInitializingShards());
                    builder.field(Fields.UNASSIGNED_SHARDS, response.getUnassignedShards());

                    if (!response.getValidationFailures().isEmpty()) {
                        builder.startArray(Fields.VALIDATION_FAILURES);
                        for (String validationFailure : response.getValidationFailures()) {
                            builder.value(validationFailure);
                        }
                        // if we don't print index level information, still print the index validation failures
                        // so we know why the status is red
                        if (fLevel == 0) {
                            for (ClusterIndexHealth indexHealth : response) {
                                builder.startObject(indexHealth.getIndex());

                                if (!indexHealth.getValidationFailures().isEmpty()) {
                                    builder.startArray(Fields.VALIDATION_FAILURES);
                                    for (String validationFailure : indexHealth.getValidationFailures()) {
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
                        builder.startObject(Fields.INDICES);
                        for (ClusterIndexHealth indexHealth : response) {
                            builder.startObject(indexHealth.getIndex(), XContentBuilder.FieldCaseConversion.NONE);

                            builder.field(Fields.STATUS, indexHealth.getStatus().name().toLowerCase(Locale.ROOT));
                            builder.field(Fields.NUMBER_OF_SHARDS, indexHealth.getNumberOfShards());
                            builder.field(Fields.NUMBER_OF_REPLICAS, indexHealth.getNumberOfReplicas());
                            builder.field(Fields.ACTIVE_PRIMARY_SHARDS, indexHealth.getActivePrimaryShards());
                            builder.field(Fields.ACTIVE_SHARDS, indexHealth.getActiveShards());
                            builder.field(Fields.RELOCATING_SHARDS, indexHealth.getRelocatingShards());
                            builder.field(Fields.INITIALIZING_SHARDS, indexHealth.getInitializingShards());
                            builder.field(Fields.UNASSIGNED_SHARDS, indexHealth.getUnassignedShards());

                            if (!indexHealth.getValidationFailures().isEmpty()) {
                                builder.startArray(Fields.VALIDATION_FAILURES);
                                for (String validationFailure : indexHealth.getValidationFailures()) {
                                    builder.value(validationFailure);
                                }
                                builder.endArray();
                            }

                            if (fLevel > 1) {
                                builder.startObject(Fields.SHARDS);

                                for (ClusterShardHealth shardHealth : indexHealth) {
                                    builder.startObject(Integer.toString(shardHealth.getId()));

                                    builder.field(Fields.STATUS, shardHealth.getStatus().name().toLowerCase(Locale.ROOT));
                                    builder.field(Fields.PRIMARY_ACTIVE, shardHealth.isPrimaryActive());
                                    builder.field(Fields.ACTIVE_SHARDS, shardHealth.getActiveShards());
                                    builder.field(Fields.RELOCATING_SHARDS, shardHealth.getRelocatingShards());
                                    builder.field(Fields.INITIALIZING_SHARDS, shardHealth.getInitializingShards());
                                    builder.field(Fields.UNASSIGNED_SHARDS, shardHealth.getUnassignedShards());

                                    builder.endObject();
                                }

                                builder.endObject();
                            }

                            builder.endObject();
                        }
                        builder.endObject();
                    }

                    builder.endObject();

                    channel.sendResponse(new XContentRestResponse(request, status, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString TIMED_OUT = new XContentBuilderString("timed_out");
        static final XContentBuilderString NUMBER_OF_SHARDS = new XContentBuilderString("number_of_shards");
        static final XContentBuilderString NUMBER_OF_REPLICAS = new XContentBuilderString("number_of_replicas");
        static final XContentBuilderString NUMBER_OF_NODES = new XContentBuilderString("number_of_nodes");
        static final XContentBuilderString NUMBER_OF_DATA_NODES = new XContentBuilderString("number_of_data_nodes");
        static final XContentBuilderString ACTIVE_PRIMARY_SHARDS = new XContentBuilderString("active_primary_shards");
        static final XContentBuilderString ACTIVE_SHARDS = new XContentBuilderString("active_shards");
        static final XContentBuilderString RELOCATING_SHARDS = new XContentBuilderString("relocating_shards");
        static final XContentBuilderString INITIALIZING_SHARDS = new XContentBuilderString("initializing_shards");
        static final XContentBuilderString UNASSIGNED_SHARDS = new XContentBuilderString("unassigned_shards");
        static final XContentBuilderString VALIDATION_FAILURES = new XContentBuilderString("validation_failures");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString PRIMARY_ACTIVE = new XContentBuilderString("primary_active");
    }
}
