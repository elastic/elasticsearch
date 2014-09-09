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

import com.google.common.collect.Sets;
import org.elasticsearch.action.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.inject.Inject;

import java.util.Collections;
import java.util.Set;

/**
 * Client factory that returns a proper {@link Client} given a {@link org.elasticsearch.rest.RestRequest}.
 * Makes it possible to register useful headers that will be copied over from REST requests
 * to corresponding transport requests at execution time.
 */
public final class RestClientFactory {

    private Set<String> relevantHeaders = Sets.newCopyOnWriteArraySet();
    private final Client client;

    @Inject
    public RestClientFactory(Client client) {
        this.client = client;
    }

    /**
     * Returns a proper {@link Client client} given the provided {@link org.elasticsearch.rest.RestRequest}
     */
    public Client client(RestRequest restRequest) {
        return relevantHeaders.size() == 0 ? client : new HeadersAndContextCopyClient(client, restRequest, relevantHeaders);
    }

    /**
     * Controls which REST headers get copied over from a {@link org.elasticsearch.rest.RestRequest} to
     * its corresponding {@link org.elasticsearch.transport.TransportRequest}(s).
     *
     * By default no headers get copied but it is possible to extend this behaviour via plugins by calling this method.
     */
    public void addRelevantHeaders(String... headers) {
        Collections.addAll(relevantHeaders, headers);
    }

    Set<String> relevantHeaders() {
        return relevantHeaders;
    }

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
