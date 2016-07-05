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
import org.elasticsearch.cockroach.CockroachActionRegistry.CockroachActionHolder;
import org.elasticsearch.cockroach.CockroachTasksInProgress.CockroachTaskInProgress;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * This component is responsible for execution of cockroach actions on individual nodes. It runs on all
 * non-transport client nodes in the cluster and monitors cluster state changes to detect started commands.
 */
public class CockroachActionExecutor extends AbstractComponent implements ClusterStateListener {
    private final Map<String, RunningCockroachTask> runningTasks = new HashMap<>();
    private final CockroachActionRegistry cockroachActionRegistry;
    private final TaskManager taskManager;
    private final ThreadPool threadPool;
    private final CockroachTransportService cockroachTransportService;


    public CockroachActionExecutor(final Settings settings, final CockroachActionRegistry cockroachActionRegistry,
                                   final TaskManager taskManager, final CockroachTransportService cockroachTransportService,
                                   final ThreadPool threadPool) {
        super(settings);
        this.cockroachActionRegistry = cockroachActionRegistry;
        this.taskManager = taskManager;
        this.cockroachTransportService = cockroachTransportService;
        this.threadPool = threadPool;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        CockroachTasksInProgress tasks = event.state().custom(CockroachTasksInProgress.TYPE);
        CockroachTasksInProgress previousTasks = event.previousState().custom(CockroachTasksInProgress.TYPE);

        if (Objects.equals(tasks, previousTasks) == false || event.nodesChanged()) {
            // We have some changes let's check if they are related to our node
            String localNodeId = event.state().getNodes().getLocalNodeId();
            Set<String> notVisitedTasks = new HashSet<>(runningTasks.keySet());
            if (tasks != null) {
                for (CockroachTaskInProgress taskInProgress : tasks.entries()) {
                    if (localNodeId.equals(taskInProgress.getExecutorNode())) {
                        RunningCockroachTask cockroachTask = runningTasks.get(taskInProgress.getUuid());
                        if (cockroachTask == null) {
                            // New task - let's start it
                            startTask(taskInProgress);
                        } else {
                            // The task is still running
                            notVisitedTasks.remove(taskInProgress.getUuid());
                            if (taskInProgress.isFinished() == false && cockroachTask.getState() == State.FAILED_NOTIFICATION) {
                                // We tried to notify the master about this task before but the notification failed and
                                // the master doesn't seem to know about it - retry notification
                                restartNotification(cockroachTask);
                            }
                        }
                    }
                }
            }

            for (String s : notVisitedTasks) {
                RunningCockroachTask task = runningTasks.get(s);
                if (task.getState() == State.NOTIFIED || task.getState() == State.DONE) {
                    // Result was sent to the caller and the caller acknowledged acceptance of the result
                    finishTask(s);
                } else if (task.getState() == State.FAILED_NOTIFICATION) {
                    // We tried to send result to master, but it failed and master doesn't know about this task
                    logger.warn("failed to notify master about task {}", task.getUuid());
                    finishTask(s);
                }
                // TODO: task is running locally, but master doesn't know about it
                // We should try cancelling this task when we add cancellation support for cockroach tasks
            }

        }

    }

    private <Request extends CockroachRequest<Request>, Response extends CockroachResponse> void startTask(
        CockroachTaskInProgress<Request, Response> taskInProgress) {
        CockroachActionHolder holder = cockroachActionRegistry.getCockroachActionHolderSafe(taskInProgress.getAction());
        CockroachTask task = (CockroachTask) taskManager.register("cockroach", taskInProgress.getAction() + "[c]",
            taskInProgress.getRequest());
        boolean processed = false;
        try {
            RunningCockroachTask<Response> runningCockroachTask = new RunningCockroachTask<>(task, taskInProgress.getUuid());
            task.setStatusProvider(runningCockroachTask);
            CockroachTaskListener<Response> listener = new CockroachTaskListener<>(runningCockroachTask);
            try {
                runningTasks.put(taskInProgress.getUuid(), runningCockroachTask);
                threadPool.executor(holder.getExecutor()).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    protected void doRun() throws Exception {
                        holder.getCockroachAction().executorNodeOperation(task, taskInProgress.getRequest(), listener);
                    }
                });
            } catch (Exception e) {
                // Submit task failure
                listener.onFailure(e);
            }
            processed = true;
        } finally {
            if (processed == false) {
                // something went wrong - unregistering task
                taskManager.unregister(task);
            }
        }
    }

    private void finishTask(String uuid) {
        RunningCockroachTask task = runningTasks.remove(uuid);
        if (task != null && task.getTask() != null) {
            taskManager.unregister(task.getTask());
        }
    }


    private <Response extends CockroachResponse> void restartNotification(RunningCockroachTask<Response> task) {
        logger.trace("resending notification for task {}", task.getUuid());
        if (task.restartNotification()) {
            if (task.getResponse() != null) {
                cockroachTransportService.sendResponse(
                    task.getTask(), task.getUuid(), task.getResponse(), new PublishingFailureListener<>(task));
            } else {
                cockroachTransportService.sendFailure(
                    task.getTask(), task.getUuid(), task.getFailure(), new PublishingFailureListener<>(task));
            }
        } else {
            logger.warn("attempt to resend notification for task {} in the {} state", task.getUuid(), task.getState());
        }
    }

    private <Response extends CockroachResponse> void startNotification(RunningCockroachTask<Response> task, Exception e) {
        logger.trace("sending notification for failed task {}", task.getUuid());
        if (task.startNotification(e)) {
            cockroachTransportService.sendFailure(task.getTask(), task.getUuid(), e, new PublishingFailureListener<>(task));
        } else {
            logger.warn("attempt to send notification for task {} in the {} state", task.getUuid(), task.getState());
        }
    }

    private <Response extends CockroachResponse> void startNotification(RunningCockroachTask<Response> task, Response response) {
        logger.trace("sending notification for finished task {}", task.getUuid());
        if (task.startNotification(response)) {
            cockroachTransportService.sendResponse(task.getTask(), task.getUuid(), response, new PublishingFailureListener<>(task));
        } else {
            logger.warn("attempt to send notification for task {} in the {} state", task.getUuid(), task.getState());
        }

    }

    private class CockroachTaskListener<Response extends CockroachResponse> implements ActionListener<Response> {
        private final RunningCockroachTask<Response> task;

        public CockroachTaskListener(final RunningCockroachTask<Response> task) {
            this.task = task;
        }

        @Override
        public void onResponse(Response response) {
            startNotification(task, response);
        }

        @Override
        public void onFailure(Exception e) {
            startNotification(task, e);
        }
    }

    private class PublishingFailureListener<Response extends CockroachResponse> implements ActionListener<Empty> {
        private final RunningCockroachTask<Response> task;

        public PublishingFailureListener(final RunningCockroachTask<Response> task) {
            this.task = task;
        }


        @Override
        public void onResponse(Empty empty) {
            logger.trace("notification for task {} was successful", task.getUuid());
            if (task.markAsNotified() == false) {
                logger.warn("attempt to mark task {} in the {} state as NOTIFIED", task.getUuid(), task.getState());
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug("notification for task {} failed - retrying", e, task.getUuid());
            if (task.notificationFailed() == false) {
                logger.warn("attempt to mark restart notification for task {} in the {} state", task.getUuid(), task.getState());
            }
        }
    }

    public enum State {
        STARTED,  // the task is currently running
        DONE,     // the task is done running and trying to notify caller
        FAILED_NOTIFICATION, // the caller notification failed
        NOTIFIED // the caller was notified, the task can be removed
    }

    private static class RunningCockroachTask<Response extends CockroachResponse> implements Provider<Task.Status> {
        private final Task task;
        private final String uuid;
        private final AtomicReference<State> state;
        @Nullable
        private Response response;
        @Nullable
        private Exception failure;

        public RunningCockroachTask(Task task, String uuid) {
            this(task, uuid, State.STARTED);
        }

        public RunningCockroachTask(Task task, String uuid, State state) {
            this.task = task;
            this.uuid = uuid;
            this.state = new AtomicReference<>(state);
        }

        public Task getTask() {
            return task;
        }

        public String getUuid() {
            return uuid;
        }

        public State getState() {
            return state.get();
        }

        public Response getResponse() {
            return response;
        }

        public Exception getFailure() {
            return failure;
        }

        public boolean startNotification(Response response) {
            boolean result = state.compareAndSet(State.STARTED, State.DONE);
            if (result) {
                this.response = response;
            }
            return result;
        }


        public boolean startNotification(Exception failure) {
            boolean result = state.compareAndSet(State.STARTED, State.DONE);
            if (result) {
                this.failure = failure;
            }
            return result;
        }

        public boolean notificationFailed() {
            return state.compareAndSet(State.DONE, State.FAILED_NOTIFICATION);
        }

        public boolean restartNotification() {
            return state.compareAndSet(State.FAILED_NOTIFICATION, State.DONE);
        }

        public boolean markAsNotified() {
            return state.compareAndSet(State.DONE, State.NOTIFIED);
        }

        @Override
        public Task.Status get() {
            return new Status(state.get());
        }
    }

    public static class Status implements Task.Status {
        public static final String NAME = "cockroach_executor";

        private final State state;

        public Status(State state) {
            this.state = requireNonNull(state, "State cannot be null");
        }

        public Status(StreamInput in) throws IOException {
            state = State.valueOf(in.readString());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("state", state.toString());
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(state.toString());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public State getState() {
            return state;
        }
    }

}
