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
package org.elasticsearch.client;

import org.elasticsearch.action.*;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;


/**
 * A {@link Client} that contains another {@link Client} which it
 * uses as its basic source, possibly transforming the requests / responses along the
 * way or providing additional functionality.
 */
public abstract class FilterClient extends AbstractClient implements AdminClient {

    protected final Client in;

    /**
     * Creates a new FilterClient
     * @param in the client to delegate to
     * @see #in()
     */
    public FilterClient(Client in) {
        this.in = in;
    }

    @Override
    public void close() {
        in().close();
    }

    @Override
    public AdminClient admin() {
        return this;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(
            Action<Request, Response, RequestBuilder, Client> action, Request request) {
        return in().execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(
            Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
        in().execute(action, request, listener);
    }

    @Override
    public Settings settings() {
        return in().settings();
    }

    @Override
    public ThreadPool threadPool() {
        return in().threadPool();
    }

    /**
     * Returns the delegate {@link Client}
     */
    protected Client in() {
        return in;
    }

    @Override
    public ClusterAdminClient cluster() {
        return in().admin().cluster();
    }

    @Override
    public IndicesAdminClient indices() {
        return in().admin().indices();
    }

    /**
     * A {@link IndicesAdminClient} that contains another {@link IndicesAdminClient} which it
     * uses as its basic source, possibly transforming the requests / responses along the
     * way or providing additional functionality.
     */
    public static class IndicesAdmin extends AbstractIndicesAdminClient {
        protected final IndicesAdminClient in;

        /**
         * Creates a new IndicesAdmin
         * @param in the client to delegate to
         * @see #in()
         */
        public IndicesAdmin(IndicesAdminClient in) {
            this.in = in;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
            return in().execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
            in().execute(action, request, listener);
        }


        /**
         * Returns the delegate {@link Client}
         */
        protected IndicesAdminClient in() {
            return in;
        }

        @Override
        public ThreadPool threadPool() {
            return in().threadPool();
        }
    }

    /**
     * A {@link ClusterAdminClient} that contains another {@link ClusterAdminClient} which it
     * uses as its basic source, possibly transforming the requests / responses along the
     * way or providing additional functionality.
     */
    public static class ClusterAdmin extends AbstractClusterAdminClient {
        protected final ClusterAdminClient in;

        /**
         * Creates a new ClusterAdmin
         * @param in the client to delegate to
         * @see #in()
         */
        public ClusterAdmin(ClusterAdminClient in) {
            this.in = in;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request) {
            return in().execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request, ActionListener<Response> listener) {
            in().execute(action, request, listener);
        }

        /**
         * Returns the delegate {@link Client}
         */
        protected ClusterAdminClient in() {
            return in;
        }

        @Override
        public ThreadPool threadPool() {
            return in().threadPool();
        }
    }
}
