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

package org.elasticsearch.rest.action.admin.cluster.ping.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.replication.IndexReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.ShardReplicationPingRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestReplicationPingAction extends BaseRestHandler {

    @Inject public RestReplicationPingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_ping/replication", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/{index}/_ping/replication", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        ReplicationPingRequest replicationPingRequest = new ReplicationPingRequest(RestActions.splitIndices(request.param("index")));
        replicationPingRequest.timeout(request.paramAsTime("timeout", ShardReplicationPingRequest.DEFAULT_TIMEOUT));
        replicationPingRequest.listenerThreaded(false);
        String replicationType = request.param("replication");
        if (replicationType != null) {
            replicationPingRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        client.admin().cluster().ping(replicationPingRequest, new ActionListener<ReplicationPingResponse>() {
            @Override public void onResponse(ReplicationPingResponse result) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);
                    for (IndexReplicationPingResponse indexResponse : result.indices().values()) {
                        builder.startObject(indexResponse.index())
                                .field("ok", true)
                                .startObject("_shards")
                                .field("total", indexResponse.totalShards())
                                .field("successful", indexResponse.successfulShards())
                                .field("failed", indexResponse.failedShards())
                                .endObject()
                                .endObject();
                    }
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}