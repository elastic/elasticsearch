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

package org.elasticsearch.http.action.admin.indices.gateway.snapshot;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.elasticsearch.action.admin.indices.gateway.snapshot.IndexGatewaySnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpActions;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.action.support.replication.ShardReplicationOperationRequest.*;
import static org.elasticsearch.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpGatewaySnapshotAction extends BaseHttpServerHandler {

    @Inject public HttpGatewaySnapshotAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.POST, "/_gateway/snapshot", this);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/_gateway/snapshot", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        GatewaySnapshotRequest gatewaySnapshotRequest = new GatewaySnapshotRequest(HttpActions.splitIndices(request.param("index")));
        gatewaySnapshotRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT));
        gatewaySnapshotRequest.listenerThreaded(false);
        client.admin().indices().execGatewaySnapshot(gatewaySnapshotRequest, new ActionListener<GatewaySnapshotResponse>() {
            @Override public void onResponse(GatewaySnapshotResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    builder.field("ok", true);
                    builder.startObject("indices");
                    for (IndexGatewaySnapshotResponse indexResponse : result.indices().values()) {
                        builder.startObject(indexResponse.index())
                                .field("ok", true)
                                .field("totalShards", indexResponse.totalShards())
                                .field("successfulShards", indexResponse.successfulShards())
                                .field("failedShards", indexResponse.failedShards())
                                .endObject();
                    }
                    builder.endObject();
                    builder.endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override public boolean spawn() {
        // we don't spawn since we fork in index replication based on operation
        return false;
    }
}