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

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;
import java.util.function.Supplier;

/**
 *
 */
public class RequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final Supplier<Request> requestFactory;
    private final TaskManager taskManager;

    public RequestHandlerRegistry(String action, Supplier<Request> requestFactory, TaskManager taskManager,
                                  TransportRequestHandler<Request> handler, String executor, boolean forceExecution,
                                  boolean canTripCircuitBreaker) {
        this.action = action;
        this.requestFactory = requestFactory;
        assert newRequest() != null;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.canTripCircuitBreaker = canTripCircuitBreaker;
        this.executor = executor;
        this.taskManager = taskManager;
    }

    public String getAction() {
        return action;
    }

    public Request newRequest() {
            return requestFactory.get();
    }

    public void processMessageReceived(Request request, TransportChannel channel) throws Exception {
        final Task task = taskManager.register(channel.getChannelType(), action, request);
        if (task == null) {
            handler.messageReceived(request, channel);
        } else {
            boolean success = false;
            try {
                handler.messageReceived(request, new TransportChannelWrapper(taskManager, task, channel), task);
                success = true;
            } finally {
                if (success == false) {
                    taskManager.unregister(task);
                }
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

    @Override
    public String toString() {
        return handler.toString();
    }

    private static class TransportChannelWrapper extends DelegatingTransportChannel {

        private final Task task;

        private final TaskManager taskManager;

        public TransportChannelWrapper(TaskManager taskManager, Task task, TransportChannel channel) {
            super(channel);
            this.task = task;
            this.taskManager = taskManager;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            endTask();
            super.sendResponse(response);
        }

        @Override
        public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
            endTask();
            super.sendResponse(response, options);
        }

        @Override
        public void sendResponse(Throwable error) throws IOException {
            endTask();
            super.sendResponse(error);
        }

        private void endTask() {
            taskManager.unregister(task);
        }
    }
}
