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

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for get source and head source APIs.
 */
public class RestGetSourceAction extends BaseRestHandler {

    public RestGetSourceAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_source", this);
        controller.registerHandler(HEAD, "/{index}/{type}/{id}/_source", this);
    }

    @Override
    public String getName() {
        return "document_get_source_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        getRequest.operationThreaded(true);
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));
        getRequest.parent(request.param("parent"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));

        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> {
            if (getRequest.fetchSourceContext() != null && !getRequest.fetchSourceContext().fetchSource()) {
                final ActionRequestValidationException validationError = new ActionRequestValidationException();
                validationError.addValidationError("fetching source can not be disabled");
                channel.sendResponse(new BytesRestResponse(channel, validationError));
            } else {
                client.get(getRequest, new RestResponseListener<GetResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final GetResponse response) throws Exception {
                        final XContentBuilder builder = channel.newBuilder(request.getXContentType(), false);
                        // check if doc source (or doc itself) is missing
                        if (response.isSourceEmpty()) {
                            return new BytesRestResponse(NOT_FOUND, builder);
                        } else {
                            builder.rawValue(response.getSourceInternal());
                            return new BytesRestResponse(OK, builder);
                        }
                    }
                });
            }
        };
    }

}
