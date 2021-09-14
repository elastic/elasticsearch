/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;

public class RequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final TaskManager taskManager;
    private final Writeable.Reader<Request> requestReader;

    public RequestHandlerRegistry(String action, Writeable.Reader<Request> requestReader, TaskManager taskManager,
                                  TransportRequestHandler<Request> handler, String executor, boolean forceExecution,
                                  boolean canTripCircuitBreaker) {
        this.action = action;
        this.requestReader = requestReader;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.canTripCircuitBreaker = canTripCircuitBreaker;
        this.executor = executor;
        this.taskManager = taskManager;
    }

    public String getAction() {
        return action;
    }

    public Request newRequest(StreamInput in) throws IOException {
        return requestReader.read(in);
    }

    public void processMessageReceived(Request request, TransportChannel channel) throws Exception {
        final Task task = taskManager.register(channel.getChannelType(), action, request);
        Releasable unregisterTask = () -> taskManager.unregister(task);
        try {
            if (channel instanceof TcpTransportChannel && task instanceof CancellableTask) {
                final TcpChannel tcpChannel = ((TcpTransportChannel) channel).getChannel();
                final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(tcpChannel, (CancellableTask) task);
                unregisterTask = Releasables.wrap(unregisterTask, stopTracking);
            }
            final TaskTransportChannel taskTransportChannel = new TaskTransportChannel(channel, unregisterTask);
            handler.messageReceived(request, taskTransportChannel, task);
            unregisterTask = null;
        } finally {
            Releasables.close(unregisterTask);
        }
    }

    public boolean isForceExecution() {
        return forceExecution;
    }

    public boolean canTripCircuitBreaker() {
        return canTripCircuitBreaker;
    }

    public String getExecutor() {
        return executor;
    }

    public TransportRequestHandler<Request> getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return handler.toString();
    }

    public static <R extends TransportRequest> RequestHandlerRegistry<R> replaceHandler(RequestHandlerRegistry<R> registry,
                                                                                        TransportRequestHandler<R> handler) {
        return new RequestHandlerRegistry<>(registry.action, registry.requestReader, registry.taskManager, handler,
            registry.executor, registry.forceExecution, registry.canTripCircuitBreaker);
    }
}
