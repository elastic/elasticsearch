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

package org.elasticsearch.rest.action.admin.cluster.ping.broadcast;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.rest.RestResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestBroadcastPingAction extends BaseRestHandler {

    @Inject public RestBroadcastPingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_ping/broadcast", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/{index}/_ping/broadcast", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        BroadcastPingRequest broadcastPingRequest = new BroadcastPingRequest(RestActions.splitIndices(request.param("index")));
        broadcastPingRequest.queryHint(request.param("queryHint"));
        BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operationThreading"), BroadcastOperationThreading.SINGLE_THREAD);
        if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
            // since we don't spawn, don't allow no_threads, but change it to a single thread
            operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
        }
        broadcastPingRequest.operationThreading(operationThreading);
        client.admin().cluster().execPing(broadcastPingRequest, new ActionListener<BroadcastPingResponse>() {
            @Override public void onResponse(BroadcastPingResponse result) {
                try {
                    JsonBuilder generator = RestJsonBuilder.cached(request);
                    generator.startObject()
                            .field("ok", true)
                            .field("totalShards", result.totalShards())
                            .field("successfulShards", result.successfulShards())
                            .field("failedShards", result.failedShards())
                            .endObject();
                    channel.sendResponse(new JsonRestResponse(request, OK, generator));
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