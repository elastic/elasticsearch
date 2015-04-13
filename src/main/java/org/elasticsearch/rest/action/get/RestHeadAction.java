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

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestHeadAction extends BaseRestHandler {

    @Inject
    public RestHeadAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(HEAD, "/{index}/{type}/{id}", this);
        controller.registerHandler(HEAD, "/{index}/{type}/{id}/_source", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        getRequest.listenerThreaded(false);
        getRequest.operationThreaded(true);
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));  // order is important, set it after routing, so it will set the routing
        getRequest.parent(request.param("parent"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", null));
        // don't get any fields back...
        getRequest.fields(Strings.EMPTY_ARRAY);
        // TODO we can also just return the document size as Content-Length

        client.get(getRequest, new RestResponseListener<GetResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetResponse response) {
                if (!response.isExists()) {
                    return new BytesRestResponse(NOT_FOUND);
                } else {
                    return new BytesRestResponse(OK);
                }
            }
        });
    }
}
