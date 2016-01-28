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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 *
 */
public class NodeClient extends AbstractClient {

    private final Map<GenericAction, TransportAction> actions;

    @Inject
    public NodeClient(Settings settings, ThreadPool threadPool, Map<GenericAction, TransportAction> actions) {
        super(settings, threadPool);
        this.actions = unmodifiableMap(actions);
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
            Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        transportAction.execute(request, listener);
    }
}
