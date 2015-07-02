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

package org.elasticsearch.rest;

import org.elasticsearch.action.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * Base handler for REST requests.
 * <p/>
 * This handler makes sure that the headers & context of the handled {@link RestRequest requests} are copied over to
 * the transport requests executed by the associated client. While the context is fully copied over, not all the headers
 * are copied, but a selected few. It is possible to control what headers are copied over by registering them using
 * {@link org.elasticsearch.rest.RestController#registerRelevantHeaders(String...)}
 */
public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {

    private final RestController controller;
    private final Client client;
    protected final ParseFieldMatcher parseFieldMatcher;

    protected BaseRestHandler(Settings settings, RestController controller, Client client) {
        super(settings);
        this.controller = controller;
        this.client = client;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        handleRequest(request, channel, new HeadersAndContextCopyClient(client, request, controller.relevantHeaders()));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception;

    static final class HeadersAndContextCopyClient extends FilterClient {

        private final RestRequest restRequest;
        private final Set<String> headers;

        HeadersAndContextCopyClient(Client in, RestRequest restRequest, Set<String> headers) {
            super(in);
            this.restRequest = restRequest;
            this.headers = headers;
        }

        private static void copyHeadersAndContext(ActionRequest actionRequest, RestRequest restRequest, Set<String> headers) {
            for (String usefulHeader : headers) {
                String headerValue = restRequest.header(usefulHeader);
                if (headerValue != null) {
                    actionRequest.putHeader(usefulHeader, headerValue);
                }
            }
            actionRequest.copyContextFrom(restRequest);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            copyHeadersAndContext(request, restRequest, headers);
            super.doExecute(action, request, listener);
        }
    }
}