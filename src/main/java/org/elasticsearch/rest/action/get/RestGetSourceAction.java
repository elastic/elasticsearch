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

package org.elasticsearch.rest.action.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetSourceAction extends BaseActionRequestRestHandler<GetRequest> {

    @Inject
    public RestGetSourceAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_source", this);
    }

    @Override
    protected GetRequest newRequest(RestRequest request) {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        getRequest.listenerThreaded(false);
        getRequest.operationThreaded(true);
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));  // order is important, set it after routing, so it will set the routing
        getRequest.parent(request.param("parent"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", null));

        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        if (getRequest.fetchSourceContext() != null && !getRequest.fetchSourceContext().fetchSource()) {
            ActionRequestValidationException validationError = new ActionRequestValidationException();
            validationError.addValidationError("fetching source can not be disabled");
            throw validationError;
        }
        return getRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, GetRequest request) {
        client.get(request, new RestResponseListener<GetResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetResponse response) throws Exception {
                XContentBuilder builder = channel.newBuilder(response.getSourceInternal());
                if (!response.isExists()) {
                    return new BytesRestResponse(NOT_FOUND, builder);
                } else {
                    XContentHelper.writeDirect(response.getSourceInternal(), builder, restRequest);
                    return new BytesRestResponse(OK, builder);
                }
            }
        });
    }
}
