/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.support;

import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.internal.InternalClusterAdminClient;

/**
 *
 */
public abstract class BaseClusterRequestBuilder<Request extends ActionRequest, Response extends ActionResponse> implements ActionRequestBuilder<Request, Response> {

    protected final InternalClusterAdminClient client;

    protected final Request request;

    protected BaseClusterRequestBuilder(ClusterAdminClient client, Request request) {
        this.client = (InternalClusterAdminClient) client;
        this.request = request;
    }

    @Override
    public Request request() {
        return request;
    }

    @Override
    public ListenableActionFuture<Response> execute() {
        PlainListenableActionFuture<Response> future = new PlainListenableActionFuture<Response>(request.listenerThreaded(), client.threadPool());
        execute(future);
        return future;
    }

    @Override
    public void execute(ActionListener<Response> listener) {
        doExecute(listener);
    }

    protected abstract void doExecute(ActionListener<Response> listener);
}