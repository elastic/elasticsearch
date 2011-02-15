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

package org.elasticsearch.rest.action.admin.cluster.ping.single;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestSinglePingAction extends BaseRestHandler {

    @Inject public RestSinglePingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/{id}/_ping", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/{index}/{type}/{id}/_ping", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        SinglePingRequest singlePingRequest = new SinglePingRequest(request.param("index"), request.param("type"), request.param("id"));
        // no need to have a threaded listener since we just send back a response
        singlePingRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        singlePingRequest.operationThreaded(true);
        client.admin().cluster().ping(singlePingRequest, new ActionListener<SinglePingResponse>() {
            @Override public void onResponse(SinglePingResponse result) {
                try {
                    XContentBuilder generator = RestXContentBuilder.restContentBuilder(request);
                    generator.startObject().field("ok", true).endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, generator));
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