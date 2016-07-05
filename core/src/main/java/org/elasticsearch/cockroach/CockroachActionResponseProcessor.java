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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Component that is responsible for routing cockroach action responses to registered listeners if they are still available
 */
public class CockroachActionResponseProcessor extends AbstractComponent implements ClusterStateListener {

    private final ConcurrentMapLong<ResponseHandlerHolder> responseHandlers =
        ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private final Set<String> inFlightNotifications = ConcurrentCollections.newConcurrentSet();
    private final CockroachTransportService cockroachTransportService;
    private final CockroachActionRegistry cockroachActionRegistry;
    private final TaskManager taskManager;
    private final ThreadPool threadPool;

    public CockroachActionResponseProcessor(final Settings settings, final CockroachTransportService cockroachTransportService,
                                            final CockroachActionRegistry cockroachActionRegistry,
                                            final TaskManager taskManager, final ThreadPool threadPool) {
        super(settings);
        this.cockroachTransportService = cockroachTransportService;
        this.cockroachActionRegistry = cockroachActionRegistry;
        this.taskManager = taskManager;
        this.threadPool = threadPool;
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        CockroachTasksInProgress tasks = event.state().custom(CockroachTasksInProgress.TYPE);
        CockroachTasksInProgress previousTasks = event.previousState().custom(CockroachTasksInProgress.TYPE);

        if (Objects.equals(tasks, previousTasks) == false) {
            // We have some changes let's check if they are related to our node
            String localNodeId = event.state().getNodes().getLocalNodeId();
            for (CockroachTasksInProgress.CockroachTaskInProgress taskInProgress : tasks.entries()) {
                if (localNodeId.equals(taskInProgress.getCallerNode())) {
                    if (taskInProgress.isFinished() && inFlightNotifications.contains(taskInProgress.getUuid()) == false) {
                        inFlightNotifications.add(taskInProgress.getUuid());
                        // Let's find corresponding response handler if it still exists on this node
                        TaskId taskId = taskInProgress.getCallerTaskId();
                        ResponseHandlerHolder responseHandlerHolder = null;
                        if (localNodeId.equals(taskId.getNodeId())) {
                            // We started this task - so we should have corresponding response handler
                            responseHandlerHolder = responseHandlers.remove(taskId.getId());
                            if (responseHandlerHolder == null) {
                                logger.warn("missing response handler for task id {}, cockroach task uuid {}",
                                    taskId, taskInProgress.getUuid());
                            }
                        }
                        final Task task;
                        if (responseHandlerHolder == null) {
                            // We don't have responseHandlerHolder, let's register a new task and process response in it
                            task = taskManager.register("cockroach", taskInProgress.getAction() + "[a]", taskInProgress.getRequest());
                        } else {
                            task = responseHandlerHolder.getTask();
                        }
                        TransportCockroachAction action = cockroachActionRegistry.getCockroachActionSafe(taskInProgress.getAction());
                        AcknowledgeListener listener = new AcknowledgeListener(task, taskInProgress.getUuid(), responseHandlerHolder);
                        threadPool.executor(action.getExecutor()).execute(() -> {
                            try {
                                if (taskInProgress.getResponse() != null) {
                                    action.onResponse(task, taskInProgress.getRequest(), taskInProgress.getResponse(), listener);
                                } else {
                                    action.onFailure(task, taskInProgress.getRequest(), taskInProgress.getFailure(), listener);
                                }
                            } catch (Exception e) {
                                logger.warn("failed to notify caller for task id {}, cockroach task uuid {}", e,
                                    taskId, taskInProgress.getUuid());
                            }
                        });
                    }
                }
            }
        }
    }

    private class AcknowledgeListener<Response extends CockroachResponse> implements ActionListener<Response> {

        private final Task task;
        private final String uuid;
        private final ResponseHandlerHolder<Response> responseHandlerHolder;

        public AcknowledgeListener(final Task task, final String uuid, final ResponseHandlerHolder<Response> responseHandlerHolder) {
            this.task = task;
            this.uuid = uuid;
            this.responseHandlerHolder = responseHandlerHolder;
        }

        @Override
        public void onResponse(Response response) {
            cockroachTransportService.acknowledgeResponse(task, uuid, new ActionListener<TransportResponse.Empty>() {
                @Override
                public void onResponse(TransportResponse.Empty empty) {
                    cleanup();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to acknowledge cockroach task {}", e, uuid);
                    cleanup();
                }

                private void cleanup() {
                    if (responseHandlerHolder != null) {
                        responseHandlerHolder.getListener().onResponse(response);
                    } else {
                        taskManager.unregister(task);
                        inFlightNotifications.remove(uuid);
                    }
                }
            });
        }

        @Override
        public void onFailure(Exception e) {
            cockroachTransportService.acknowledgeResponse(task, uuid, new ActionListener<TransportResponse.Empty>() {
                @Override
                public void onResponse(TransportResponse.Empty empty) {
                    cleanup(e);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to acknowledge cockroach task {}", e, uuid);
                    cleanup(e);
                }

                private void cleanup(Exception e) {
                    if (responseHandlerHolder != null) {
                        responseHandlerHolder.getListener().onFailure(e);
                    } else {
                        taskManager.unregister(task);
                        inFlightNotifications.remove(uuid);
                    }
                }

            });

        }

    }

    public <Response extends CockroachResponse> Provider<Task.Status> registerResponseListener(Task task, ActionListener<Response>
        listener) {
        ResponseHandlerHolder<Response> holder = new ResponseHandlerHolder<>(listener, task);
        responseHandlers.put(task.getId(), holder);
        logger.trace("registered listener for task {}", task.getId());
        return holder;
    }

    public void markResponseListenerInitialized(Task task) {
        ResponseHandlerHolder responseHandlerHolder = responseHandlers.get(task.getId());
        if (responseHandlerHolder != null) {
            if (responseHandlerHolder.initialize()) {
                logger.trace("initialized listener for task {}", task.getId());
            }
        }
    }

    public void processFailure(Task task, Exception failure) {
        ResponseHandlerHolder responseHandlerHolder = responseHandlers.remove(task.getId());
        if (responseHandlerHolder != null) {
            responseHandlerHolder.getListener().onFailure(failure);
            logger.trace("sent failure to listener for task {}", task.getId());
        } else {
            logger.warn("failed to unregister listener for task {}", task.getId());
        }
    }

    private static class ResponseHandlerHolder<Response extends CockroachResponse> implements Provider<Task.Status> {
        private final ActionListener<Response> listener;
        private final Task task;
        private final AtomicBoolean initialized;

        public ResponseHandlerHolder(ActionListener<Response> listener, Task task) {
            this.listener = listener;
            this.task = task;
            this.initialized = new AtomicBoolean();
        }

        public ActionListener<Response> getListener() {
            return listener;
        }

        public Task getTask() {
            return task;
        }

        public boolean getInitialized() {
            return initialized.get();
        }

        public boolean initialize() {
            return initialized.getAndSet(true) == false;
        }

        @Override
        public Task.Status get() {
            return new Status(getInitialized());
        }
    }

    public static class Status implements Task.Status {
        public static final String NAME = "cockroach_caller";

        private final boolean initialized;

        public Status(boolean initialized) {
            this.initialized = initialized;
        }

        public Status(StreamInput in) throws IOException {
            initialized = in.readBoolean();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("initialized", initialized);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(initialized);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public boolean isInitialized() {
            return initialized;
        }
    }
}
