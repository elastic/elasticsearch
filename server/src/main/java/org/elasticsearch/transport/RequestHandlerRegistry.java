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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class RequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final ThreadPool threadPool;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final TaskManager taskManager;
    private final Writeable.Reader<Request> requestReader;

    public RequestHandlerRegistry(String action, Writeable.Reader<Request> requestReader, ThreadPool threadPool, TaskManager taskManager,
                                  TransportRequestHandler<Request> handler, String executor, boolean forceExecution,
                                  boolean canTripCircuitBreaker) {
        this.action = action;
        this.requestReader = requestReader;
        this.threadPool = threadPool;
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
        processMessageReceived(request, channel, new AtomicReference<>(() -> {}));
    }

    private void processMessageReceived(Request request, TransportChannel channel, AtomicReference<Releasable> releasable)
        throws Exception {
        final Task task = taskManager.register(channel.getChannelType(), action, request);
        boolean success = false;
        // TODO: Review releasable logic
        Releasable[] releasables = new Releasable[2];
        releasables[0] = () -> taskManager.unregister(task);
        try {
            if (channel instanceof TcpTransportChannel && task instanceof CancellableTask) {
                final TcpChannel tcpChannel = ((TcpTransportChannel) channel).getChannel();
                final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(tcpChannel, (CancellableTask) task);
                releasables[0] = Releasables.wrap(releasables[0], stopTracking);
                releasables[1] = releasable.get();
            }
            final TaskTransportChannel taskTransportChannel = new TaskTransportChannel(channel, releasables);
            handler.messageReceived(request, taskTransportChannel, task);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(releasables);
            }
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

    public void dispatchMessage(Request request, TransportChannel channel) {
        final AtomicReference<Releasable> releasable = new AtomicReference<>();
        try {
            releasable.set(handler.preDispatchValidation(request));
            threadPool.executor(executor).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    Releasables.close(releasable.get());
                    TransportChannel.sendErrorResponse(channel, action, request, e);
                }

                @Override
                protected void doRun() throws Exception {
                    processMessageReceived(request, channel, releasable);
                }

                @Override
                public boolean isForceExecution() {
                    return forceExecution;
                }

                @Override
                public String toString() {
                    return "processing of [" + action + "]: " + request;
                }
            });
        } catch (Exception e) {
            Releasables.close(releasable.get());
            TransportChannel.sendErrorResponse(channel, action, request, e);
        }
    }

    @Override
    public String toString() {
        return handler.toString();
    }

    public static <R extends TransportRequest> RequestHandlerRegistry<R> replaceHandler(RequestHandlerRegistry<R> registry,
                                                                                        TransportRequestHandler<R> handler) {
        return new RequestHandlerRegistry<>(registry.action, registry.requestReader, registry.threadPool, registry.taskManager, handler,
            registry.executor, registry.forceExecution, registry.canTripCircuitBreaker);
    }
}
