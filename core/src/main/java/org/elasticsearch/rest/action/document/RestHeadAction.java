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

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Base class for {@code HEAD} request handlers for a single document.
 */
public abstract class RestHeadAction extends BaseRestHandler {

    /**
     * Handler to check for document existence.
     */
    public static class Document extends RestHeadAction {

        @Inject
        public Document(Settings settings, RestController controller) {
            super(settings, false);
            controller.registerHandler(HEAD, "/{index}/{type}/{id}", this);
        }
    }

    /**
     * Handler to check for document source existence (may be disabled in the mapping).
     */
    public static class Source extends RestHeadAction {

        @Inject
        public Source(Settings settings, RestController controller) {
            super(settings, true);
            controller.registerHandler(HEAD, "/{index}/{type}/{id}/_source", this);
        }
    }

    private final boolean source;

    /**
     * All subclasses must be registered in {@link org.elasticsearch.common.network.NetworkModule}.
     *
     * @param settings injected settings
     * @param source   {@code false} to check for {@link GetResponse#isExists()}.
     *                 {@code true} to also check for {@link GetResponse#isSourceEmpty()}.
     */
    public RestHeadAction(Settings settings, boolean source) {
        super(settings);
        this.source = source;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        getRequest.operationThreaded(true);
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));  // order is important, set it after routing, so it will set the routing
        getRequest.parent(request.param("parent"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));
        // don't get any fields back...
        getRequest.fields(Strings.EMPTY_ARRAY);
        // TODO we can also just return the document size as Content-Length

        client.get(getRequest, new RestResponseListener<GetResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetResponse response) {
                if (!response.isExists()) {
                    return new BytesRestResponse(NOT_FOUND, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
                } else if (source && response.isSourceEmpty()) { // doc exists, but source might not (disabled in the mapping)
                    return new BytesRestResponse(NOT_FOUND, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
                } else {
                    return new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
                }
            }
        });
    }
}
