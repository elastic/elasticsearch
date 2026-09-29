/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs a list of {@link Driver}s to completion, each as a child task of the parent task so that progress can be retrieved
 * and cancellation with the Task API.
 */
public class DriverTaskRunner {
    public static final String ACTION_NAME = "indices:data/read/esql/compute";
    private final TaskManager taskManager;
    private final ThreadContext threadContext;
    private final Transport.Connection localConnection;

    public DriverTaskRunner(TransportService transportService) {
        this.taskManager = transportService.getTaskManager();
        this.threadContext = transportService.getThreadPool().getThreadContext();
        this.localConnection = transportService.getLocalNodeConnection();

    }

    public void executeDrivers(Task parentTask, List<Driver> drivers, Executor workerExecutor, ActionListener<Void> listener) {
        final TaskId parentTaskId = new TaskId(taskManager.getNodeId(), parentTask.getId());
        if (drivers.size() == 1) {
            startDriver(parentTaskId, drivers.getFirst(), workerExecutor, listener);
            return;
        }
        DriverRunner runner = new DriverRunner(threadContext) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                startDriver(parentTaskId, driver, workerExecutor, driverListener);
            }
        };
        runner.runToCompletion(drivers, listener);
    }

    private void startDriver(TaskId parentTaskId, Driver driver, Executor workerExecutor, ActionListener<Void> driverListener) {
        final ActionListener<Void> listener = ContextPreservingActionListener.wrapPreservingContext(driverListener, threadContext);
        try (var ignored = threadContext.newTraceContext()) {
            final Releasable finishTask;
            try {
                finishTask = registerTask(parentTaskId, driver);
            } catch (Exception e) {
                driver.abort(e, listener);
                return;
            }
            Driver.start(
                threadContext,
                workerExecutor,
                driver,
                Driver.DEFAULT_MAX_ITERATIONS,
                ActionListener.releaseBefore(finishTask, listener)
            );
        }
    }

    private Releasable registerTask(TaskId parentTaskId, Driver driver) {
        final Releasable unregisterChildNode = taskManager.registerChildConnection(parentTaskId.getId(), localConnection);
        final DriverRequest request = new DriverRequest(driver);
        request.setParentTask(parentTaskId);
        final Task task;
        try {
            task = taskManager.register("transport", ACTION_NAME, request);
        } catch (Exception e) {
            Releasables.closeWhileHandlingException(unregisterChildNode);
            throw e;
        }
        return Releasables.wrap(unregisterChildNode, () -> taskManager.unregister(task));
    }

    private static class DriverRequest extends UntypedActionRequest {
        private final Driver driver;

        DriverRequest(Driver driver) {
            this.driver = driver;
        }

        DriverRequest(StreamInput in) {
            throw new UnsupportedOperationException("Driver request should never leave the current node");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Driver request should never leave the current node");
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            if (parentTaskId.isSet() == false) {
                assert false : "DriverRequest must have a parent task";
                throw new IllegalStateException("DriverRequest must have a parent task");
            }
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                protected void onCancelled() {
                    String reason = Objects.requireNonNullElse(getReasonCancelled(), "cancelled");
                    driver.cancel(reason);
                }

                @Override
                public String getDescription() {
                    return driver.describe();
                }

                @Override
                public Status getStatus() {
                    return driver.status();
                }
            };
        }
    }
}
