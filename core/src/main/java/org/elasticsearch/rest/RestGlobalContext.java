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

import java.util.Set;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

/**
 * Context passed to all Rest actions on construction. Think carefully before
 * adding things to this because they can be seen by all rest actions.
 */
public class RestGlobalContext {
    private final Settings settings;
    private final RestController controller;
    private final Client client;
    private final IndicesQueriesRegistry indicesQueriesRegistry;

    public RestGlobalContext(Settings settings, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry) {
        this.settings = settings;
        this.controller = controller;
        this.client = client;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
    }

    public Settings getSettings() {
        return settings;
    }

    /**
     * Create the client to be used to process a request. This client will copy
     * headers from the rest request into the internal requests but otherwise
     * simply wraps a globally shared client.
     */
    public Client createClient(RestRequest request) {
        return new HeadersAndContextCopyClient(client, request, controller.relevantHeaders());
    }

    public IndicesQueriesRegistry getIndicesQueriesRegistry() {
        return indicesQueriesRegistry;
    }

    static final class HeadersAndContextCopyClient extends FilterClient {

        private final RestRequest restRequest;
        private final Set<String> headers;

        HeadersAndContextCopyClient(Client in, RestRequest restRequest, Set<String> headers) {
            super(in);
            this.restRequest = restRequest;
            this.headers = headers;
        }

        private static void copyHeadersAndContext(ActionRequest<?> actionRequest, RestRequest restRequest, Set<String> headers) {
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
