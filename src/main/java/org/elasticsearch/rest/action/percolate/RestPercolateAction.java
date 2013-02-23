/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.percolate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestPercolateAction extends BaseRestHandler {

    @Inject
    public RestPercolateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/_percolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_percolate", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        PercolateRequest percolateRequest = new PercolateRequest(request.param("index"), request.param("type"));
        percolateRequest.listenerThreaded(false);
        percolateRequest.source(request.content(), request.contentUnsafe());

        // we just send a response, no need to fork
        percolateRequest.listenerThreaded(false);
        // we don't spawn, then fork if local
        percolateRequest.operationThreaded(true);

        percolateRequest.preferLocal(request.paramAsBoolean("prefer_local", percolateRequest.preferLocalShard()));
        client.percolate(percolateRequest, new ActionListener<PercolateResponse>() {
            @Override
            public void onResponse(PercolateResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    builder.field(Fields.OK, true);
                    builder.startArray(Fields.MATCHES);
                    for (String match : response) {
                        builder.value(match);
                    }
                    builder.endArray();

                    builder.endObject();

                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
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
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }
}
