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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

import static org.elasticsearch.cluster.ClusterState.registerPrototype;

/**
 * Service responsible for executing restartable actions that can survive disappearance of a coordinating and executor nodes.
 * <p>
 * In order to be resilient to node restarts, the cockroach actions are using the cluster state instead of a transport service to send
 * requests and responses. The execution is done in six phases:
 * <p>
 * 1. The coordinating nodes sends an ordinary transport request to the master node to start a new cockroach action. This action is handled
 * by the {@link CockroachTransportService}.
 * <p>
 * 2. The master node updates the {@link CockroachTasksInProgress} in the cluster state to indicate that there is a new cockroach action
 * running in the system.
 * <p>
 * 3. The {@link CockroachActionExecutor} running on every node in the cluster monitors changes in the cluster state and starts execution
 * of all new actions assigned to the node it is running on.
 * <p>
 * 4. When the action finishes on running on the node, the {@link CockroachActionExecutor} uses the {@link CockroachTransportService} to
 * update the cluster state with the result of the action.
 * <p>
 * 5. The {@link CockroachActionResponseProcessor} is running on every node and monitoring changes to the cluster state as well, when
 * it sees a new action response assigned to the current node that wasn't processed yet, it passes the response to the action caller.
 * After successful processing of the response, it uses {@link CockroachTransportService} to remove the current cockroach task record from
 * {@link CockroachTasksInProgress}.
 * <p>
 * 6. The {@link CockroachActionExecutor} receives cluster state update with no record of the cockroach action that it was running, which
 * indicates that the response was successfully processed. It this moment it considers the task completed and removes the result from
 * memory. However, if connectivity with the master in step 5 fails, the {@link CockroachActionExecutor} would wait for a new master to be
 * elected in order to repost the result using the new master.
 * <p>
 * The {@link CockroachTransportService} is also responsible for running the {@link CockroachActionCoordinator} on the master node. The
 * coordinator is monitoring  changes in the cluster state and reassigns tasks to different nodes when coordinating or executing nodes
 * of currently running tasks go down.
 */
public class CockroachService extends AbstractLifecycleComponent {

    static {
        registerPrototype(CockroachTasksInProgress.TYPE, CockroachTasksInProgress.PROTO);
    }

    private final TaskManager taskManager;
    private final ClusterService clusterService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CockroachActionExecutor cockroachActionExecutor;
    private final CockroachActionResponseProcessor cockroachActionResponseProcessor;
    private final CockroachActionRegistry cockroachActionRegistry;
    private final CockroachTransportService cockroachTransportService;

    @Inject
    public CockroachService(Settings settings, ClusterService clusterService, TransportService transportService,
                            NamedWriteableRegistry namedWriteableRegistry, ThreadPool threadPool) {
        super(settings);
        this.taskManager = transportService.getTaskManager();
        this.clusterService = clusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;

        this.cockroachActionRegistry = new CockroachActionRegistry(settings);

        this.cockroachTransportService =
            new CockroachTransportService(settings, clusterService, transportService, cockroachActionRegistry);

        this.cockroachActionExecutor =
            new CockroachActionExecutor(settings, cockroachActionRegistry, taskManager, cockroachTransportService, threadPool);
        clusterService.addLast(cockroachActionExecutor);

        this.cockroachActionResponseProcessor = new CockroachActionResponseProcessor(settings, cockroachTransportService,
            cockroachActionRegistry, taskManager, threadPool);
        clusterService.addLast(cockroachActionResponseProcessor);
    }

    @Override
    protected void doStart() {
        cockroachTransportService.start();
    }

    @Override
    protected void doStop() {
        cockroachTransportService.stop();
    }

    @Override
    protected void doClose() {
        clusterService.remove(cockroachActionResponseProcessor);
        clusterService.remove(cockroachActionExecutor);
        cockroachTransportService.close();
    }

    public <Response extends CockroachResponse, Request extends CockroachRequest<Request>> void sendRequest(
        final CockroachTask task, final String action, final Request request, ActionListener<Response> listener) {
        try {
            task.setStatusProvider(cockroachActionResponseProcessor.registerResponseListener(task, listener));
            cockroachTransportService.sendRequest(task, action, request, new ActionListener<Empty>() {
                @Override
                public void onResponse(Empty empty) {
                    cockroachActionResponseProcessor.markResponseListenerInitialized(task);
                }

                @Override
                public void onFailure(Exception e) {
                    handleStartException(e, task, request);
                }
            });
        } catch (Exception e) {
            handleStartException(e, task, request);
        }
    }

    private <Request extends CockroachRequest<Request>> void handleStartException(Exception e, Task task, Request request) {
        logger.warn("[{}] failed to start the cockroach action for [{}]", e, task.getAction(), request);
        cockroachActionResponseProcessor.processFailure(task, e);
    }

    /**
     * Registers a new request handler
     *
     * @param action   The action the request handler is associated with
     * @param executor The executor the request handling will be executed on
     */
    public <Request extends CockroachRequest<Request>, Response extends CockroachResponse> void registerRequestHandler(
        String actionName, TransportCockroachAction<Request, Response> action,
        Supplier<Request> requestSupplier, Supplier<Response> responseSupplier,
        String executor) {
        namedWriteableRegistry.register(CockroachRequest.class, actionName, in -> {
            Request request = requestSupplier.get();
            request.readFrom(in);
            return request;
        });
        namedWriteableRegistry.register(CockroachResponse.class, actionName, in -> {
            Response response = responseSupplier.get();
            response.readFrom(in);
            return response;
        });
        cockroachActionRegistry.registerCockroachAction(actionName, action, executor);
    }
}
