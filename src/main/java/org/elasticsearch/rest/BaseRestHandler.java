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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.support.RestActions;

import java.util.Set;

/**
 * Base handler for REST requests.
 *
 * This handler makes sure that the headers & context of the handled {@link RestRequest requests} are copied over to
 * the transport requests executed by the associated client. While the context is fully copied over, not all the headers
 * are copied, but a selected few. It is possible to control what headers are copied over by registering them using
 * {@link org.elasticsearch.rest.RestController#registerRelevantHeaders(String...)}
 */
public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {

    private final RestController controller;
    private final Client client;

    protected BaseRestHandler(Settings settings, RestController controller, Client client) {
        super(settings);
        this.controller = controller;
        this.client = client;
    }

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        handleRequest(request, channel, new HeadersAndContextCopyClient(client, request, controller.relevantHeaders()));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception;

    static final class HeadersAndContextCopyClient extends FilterClient {

        private final RestRequest restRequest;
        private final IndicesAdmin indicesAdmin;
        private final ClusterAdmin clusterAdmin;
        private final Set<String> headers;

        HeadersAndContextCopyClient(Client in, RestRequest restRequest, Set<String> headers) {
            super(in);
            this.restRequest = restRequest;
            this.indicesAdmin = new IndicesAdmin(in.admin().indices(), restRequest, headers);
            this.clusterAdmin = new ClusterAdmin(in.admin().cluster(), restRequest, headers);
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
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
            copyHeadersAndContext(request, restRequest, headers);
            return super.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
            copyHeadersAndContext(request, restRequest, headers);
            super.execute(action, request, listener);
        }

        @Override
        public ClusterAdminClient cluster() {
            return clusterAdmin;
        }

        @Override
        public IndicesAdminClient indices() {
            return indicesAdmin;
        }

        private static final class ClusterAdmin extends FilterClient.ClusterAdmin {

            private final RestRequest restRequest;
            private final Set<String> headers;

            private ClusterAdmin(ClusterAdminClient in, RestRequest restRequest, Set<String> headers) {
                super(in);
                this.restRequest = restRequest;
                this.headers = headers;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request) {
                copyHeadersAndContext(request, restRequest, headers);
                return super.execute(action, request);
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request, ActionListener<Response> listener) {
                copyHeadersAndContext(request, restRequest, headers);
                super.execute(action, request, listener);
            }
        }

        private final class IndicesAdmin extends FilterClient.IndicesAdmin {

            private final RestRequest restRequest;
            private final Set<String> headers;

            private IndicesAdmin(IndicesAdminClient in, RestRequest restRequest, Set<String> headers) {
                super(in);
                this.restRequest = restRequest;
                this.headers = headers;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
                copyHeadersAndContext(request, restRequest, headers);
                return super.execute(action, request);
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
                copyHeadersAndContext(request, restRequest, headers);
                super.execute(action, request, listener);
            }
        }
    }
}