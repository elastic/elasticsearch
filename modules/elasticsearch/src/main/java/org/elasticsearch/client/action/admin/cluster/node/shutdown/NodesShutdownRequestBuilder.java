/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.client.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.internal.InternalClusterAdminClient;
import org.elasticsearch.util.TimeValue;

/**
 * @author kimchy (shay.banon)
 */
public class NodesShutdownRequestBuilder {

    private final InternalClusterAdminClient clusterClient;

    private final NodesShutdownRequest request;

    public NodesShutdownRequestBuilder(InternalClusterAdminClient clusterClient) {
        this.clusterClient = clusterClient;
        this.request = new NodesShutdownRequest();
    }

    /**
     * The nodes ids to restart.
     */
    public NodesShutdownRequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return this;
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequestBuilder setDelay(TimeValue delay) {
        request.delay(delay);
        return this;
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequestBuilder setDelay(String delay) {
        request.delay(delay);
        return this;
    }

    /**
     * Executes the operation asynchronously and returns a future.
     */
    public ListenableActionFuture<NodesShutdownResponse> execute() {
        PlainListenableActionFuture<NodesShutdownResponse> future = new PlainListenableActionFuture<NodesShutdownResponse>(request.listenerThreaded(), clusterClient.threadPool());
        clusterClient.nodesShutdown(request, future);
        return future;
    }

    /**
     * Executes the operation asynchronously with the provided listener.
     */
    public void execute(ActionListener<NodesShutdownResponse> listener) {
        clusterClient.nodesShutdown(request, listener);
    }

}