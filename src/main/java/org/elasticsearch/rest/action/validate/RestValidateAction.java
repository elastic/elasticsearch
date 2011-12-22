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

package org.elasticsearch.rest.action.validate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.action.validate.ValidateRequest;
import org.elasticsearch.action.validate.ValidateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.rest.action.support.RestActions.splitTypes;

/**
 *
 */
public class RestValidateAction extends BaseRestHandler {

    @Inject
    public RestValidateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_validate", this);
        controller.registerHandler(POST, "/_validate", this);
        controller.registerHandler(GET, "/{index}/_validate", this);
        controller.registerHandler(POST, "/{index}/_validate", this);
        controller.registerHandler(GET, "/{index}/{type}/_validate", this);
        controller.registerHandler(POST, "/{index}/{type}/_validate", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ValidateRequest validateRequest = new ValidateRequest(RestActions.splitIndices(request.param("index")));
        // we just send back a response, no need to fork a listener
        validateRequest.listenerThreaded(false);
        try {
            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operation_threading"), BroadcastOperationThreading.SINGLE_THREAD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads, but change it to a single thread
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            validateRequest.operationThreading(operationThreading);
            if (request.hasContent()) {
                validateRequest.query(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength(), true);
            } else {
                String source = request.param("source");
                if (source != null) {
                    validateRequest.query(source);
                } else {
                    byte[] querySource = RestActions.parseQuerySource(request);
                    if (querySource != null) {
                        validateRequest.query(querySource);
                    }
                }
            }
            validateRequest.types(splitTypes(request.param("type")));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.validate(validateRequest, new ActionListener<ValidateResponse>() {
            @Override
            public void onResponse(ValidateResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("valid", response.valid());

                    buildBroadcastShardsHeader(builder, response);

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
}
