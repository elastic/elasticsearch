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

package org.elasticsearch.client.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/**
 *
 */
public class NodeClient extends AbstractClient {

    private final ESLogger logger;
    private final Settings settings;
    private final ThreadPool threadPool;

    private final NodeAdminClient admin;

    private final ImmutableMap<ClientAction, TransportAction> actions;

    private final Headers headers;
    private final ThreadedActionListener.Wrapper threadedWrapper;

    @Inject
    public NodeClient(Settings settings, ThreadPool threadPool, NodeAdminClient admin, Map<GenericAction, TransportAction> actions, Headers headers) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.admin = admin;
        this.headers = headers;
        MapBuilder<ClientAction, TransportAction> actionsBuilder = new MapBuilder<>();
        for (Map.Entry<GenericAction, TransportAction> entry : actions.entrySet()) {
            if (entry.getKey() instanceof ClientAction) {
                actionsBuilder.put((ClientAction) entry.getKey(), entry.getValue());
            }
        }
        this.actions = actionsBuilder.immutableMap();
        this.threadedWrapper = new ThreadedActionListener.Wrapper(logger, settings, threadPool);
    }

    @Override
    public Settings settings() {
        return this.settings;
    }

    @Override
    public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public AdminClient admin() {
        return this.admin;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder, Client> action, final Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
        headers.applyTo(request);
        listener = threadedWrapper.wrap(listener);
        TransportAction<Request, Response> transportAction = actions.get((ClientAction)action);
        transportAction.execute(request, listener);
    }
}
