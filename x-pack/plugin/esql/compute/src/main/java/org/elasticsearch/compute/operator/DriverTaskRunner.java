/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * A {@link DriverRunner} that executes {@link Driver} with a child task so that we can retrieve the progress with the Task API.
 */
public class DriverTaskRunner {
    public static final String ACTION_NAME = "indices:data/read/esql/compute";
    private final TransportService transportService;

    public DriverTaskRunner(TransportService transportService, Executor executor) {
        this.transportService = transportService;
        transportService.registerRequestHandler(ACTION_NAME, executor, DriverRequest::new, new DriverRequestHandler(transportService));
    }

    public void executeDrivers(Task parentTask, List<Driver> drivers, Executor executor, ActionListener<Void> listener) {
        var runner = new DriverRunner(transportService.getThreadPool().getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                transportService.sendChildRequest(
                    transportService.getLocalNode(),
                    ACTION_NAME,
                    new DriverRequest(driver, executor),
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    TransportResponseHandler.empty(
                        executor,
                        // The TransportResponseHandler can be notified while the Driver is still running during node shutdown
                        // or the Driver hasn't started when the parent task is canceled. In such cases, we should abort
                        // the Driver and wait for it to finish.
                        ActionListener.wrap(driverListener::onResponse, e -> driver.abort(e, driverListener))
                    )
                );
            }
        };
        runner.runToCompletion(drivers, listener);
    }

    private static class DriverRequest extends ActionRequest implements CompositeIndicesRequest {
        private final Driver driver;
        private final Executor executor;

        DriverRequest(Driver driver, Executor executor) {
            this.driver = driver;
            this.executor = executor;
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

    private record DriverRequestHandler(TransportService transportService) implements TransportRequestHandler<DriverRequest> {
        @Override
        public void messageReceived(DriverRequest request, TransportChannel channel, Task task) {
            var listener = new ChannelActionListener<ActionResponse.Empty>(channel);
            Driver.start(
                transportService.getThreadPool().getThreadContext(),
                request.executor,
                request.driver,
                Driver.DEFAULT_MAX_ITERATIONS,
                listener.map(unused -> ActionResponse.Empty.INSTANCE)
            );
        }
    }
}
