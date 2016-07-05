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

package org.elasticsearch.cockroach;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * An action that can survive restart of requesting or executing node.
 * These actions are using cluster state rather than only transport service to send requests and responses.
 */
public abstract class TransportCockroachAction<Request extends CockroachRequest<Request>, Response extends CockroachResponse>
    extends HandledTransportAction<Request, Response> {

    private final CockroachService cockroachService;

    private final String executor;

    protected TransportCockroachAction(Settings settings, String actionName, boolean canTripCircuitBreaker, ThreadPool threadPool,
                                       TransportService transportService, CockroachService cockroachService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Supplier<Request> requestSupplier, Supplier<Response> responseSupplier, String executor) {
        super(settings, actionName, canTripCircuitBreaker, threadPool, transportService, actionFilters, indexNameExpressionResolver,
            requestSupplier);
        this.cockroachService = cockroachService;
        this.executor = executor;
        cockroachService.registerRequestHandler(actionName, this, requestSupplier, responseSupplier, executor);
    }

    /**
     * Returns the node id where the request has to be executed
     */
    public abstract DiscoveryNode executorNode(Request request, ClusterState clusterState);

    /**
     * Returns the node id where the response should be processed
     */
    public abstract DiscoveryNode responseNode(Request request, ClusterState clusterState);

    /**
     * Checks the current cluster state for compatibility with the request
     * <p>
     * Throws an exception if the supplied request cannot be executed on the cluster in the current state.
     */
    public void validate(Request request, ClusterState clusterState) {

    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        logger.warn("attempt to execute a cockroach action without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        cockroachService.sendRequest((CockroachTask) task, actionName, request, listener);
    }

    /**
     * This operation will be executed on the caller node.
     */
    public abstract void executorNodeOperation(Task task, Request request, ActionListener<Response> listener);

    /**
     * This operation is called on the caller node if the cockroach task was successful.
     * <p>
     * If the original caller node is not available by the time the task finishes, a new node will be elected by calling
     * {@link #responseNode} method.
     */
    public void onResponse(Task task, Request request, Response response, ActionListener<Response> listener) {
        listener.onResponse(response);
    }

    /**
     * This operation is called on the caller node if the cockroach task failed.
     * <p>
     * If the original caller node is not available by the time the task finishes, a new node will be elected by calling
     * {@link #responseNode} method.
     */
    public void onFailure(Task task, Request request, Exception failure, ActionListener<Response> listener) {
        listener.onFailure(failure);
    }

    public String getExecutor() {
        return executor;
    }
}
