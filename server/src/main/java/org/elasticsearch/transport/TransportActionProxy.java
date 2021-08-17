/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;

/**
 * TransportActionProxy allows an arbitrary action to be executed on a defined target node while the initial request is sent to a second
 * node that acts as a request proxy to the target node. This is useful if a node is not directly connected to a target node but is
 * connected to an intermediate node that establishes a transitive connection.
 */
public final class TransportActionProxy {

    private TransportActionProxy() {} // no instance

    private static class ProxyRequestHandler<T extends ProxyRequest<TransportRequest>> implements TransportRequestHandler<T> {

        private final TransportService service;
        private final String action;
        private final Function<TransportRequest, Writeable.Reader<? extends TransportResponse>> responseFunction;

        ProxyRequestHandler(TransportService service, String action, Function<TransportRequest,
                Writeable.Reader<? extends TransportResponse>> responseFunction) {
            this.service = service;
            this.action = action;
            this.responseFunction = responseFunction;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            DiscoveryNode targetNode = request.targetNode;
            TransportRequest wrappedRequest = request.wrapped;
            assert assertConsistentTaskType(task, wrappedRequest);
            TaskId taskId = task.taskInfo(service.localNode.getId(), false).getTaskId();
            wrappedRequest.setParentTask(taskId);
            service.sendRequest(targetNode, action, wrappedRequest,
                    new ProxyResponseHandler<>(channel, responseFunction.apply(wrappedRequest)));
        }

        private boolean assertConsistentTaskType(Task proxyTask, TransportRequest wrapped) {
            final Task targetTask = wrapped.createTask(0, proxyTask.getType(), proxyTask.getAction(), TaskId.EMPTY_TASK_ID, Map.of());
            assert targetTask instanceof CancellableTask == proxyTask instanceof CancellableTask :
                "Cancellable property of proxy action [" + proxyTask.getAction() + "] is configured inconsistently: " +
                    "expected [" + (targetTask instanceof CancellableTask) + "] actual [" + (proxyTask instanceof CancellableTask) + "]";
            return true;
        }
    }

    private static class ProxyResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final Writeable.Reader<T> reader;
        private final TransportChannel channel;

        ProxyResponseHandler(TransportChannel channel, Writeable.Reader<T> reader) {
            this.reader = reader;
            this.channel = channel;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return reader.read(in);
        }

        @Override
        public void handleResponse(T response) {
            try {
                response.incRef();
                channel.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            try {
                channel.sendResponse(exp);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static class ProxyRequest<T extends TransportRequest> extends TransportRequest {
        final T wrapped;
        final DiscoveryNode targetNode;

        ProxyRequest(T wrapped, DiscoveryNode targetNode) {
            this.wrapped = wrapped;
            this.targetNode = targetNode;
        }

        ProxyRequest(StreamInput in, Writeable.Reader<T> reader) throws IOException {
            super(in);
            targetNode = new DiscoveryNode(in);
            wrapped = reader.read(in);
            setParentTask(wrapped.getParentTask());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            wrapped.writeTo(out);
        }
    }

    private static class CancellableProxyRequest<T extends TransportRequest> extends ProxyRequest<T> {
        CancellableProxyRequest(StreamInput in, Writeable.Reader<T> reader) throws IOException {
            super(in, reader);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return "proxy task [" + wrapped.getDescription() + "]";
                }
            };
        }
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
     * response type changes based on the upcoming request (quite rare)
     */
    public static void registerProxyActionWithDynamicResponseType(TransportService service, String action, boolean cancellable,
                                                                  Function<TransportRequest,
                                                                      Writeable.Reader<? extends TransportResponse>> responseFunction) {
        RequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandler(action);
        service.registerRequestHandler(getProxyAction(action), ThreadPool.Names.SAME, true, false,
            in -> cancellable ?
                new CancellableProxyRequest<>(in, requestHandler::newRequest) :
                new ProxyRequest<>(in, requestHandler::newRequest),
            new ProxyRequestHandler<>(service, action, responseFunction));
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
     * response type is always the same (most of the cases).
     */
    public static void registerProxyAction(TransportService service, String action, boolean cancellable,
                                           Writeable.Reader<? extends TransportResponse> reader) {
        RequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandler(action);
        service.registerRequestHandler(getProxyAction(action), ThreadPool.Names.SAME, true, false,
            in -> cancellable ?
                new CancellableProxyRequest<>(in, requestHandler::newRequest) :
                new ProxyRequest<>(in, requestHandler::newRequest),
            new ProxyRequestHandler<>(service, action, request -> reader));
    }

    private static final String PROXY_ACTION_PREFIX = "internal:transport/proxy/";
    /**
     * Returns the corresponding proxy action for the given action
     */
    public static String getProxyAction(String action) {
        return  PROXY_ACTION_PREFIX + action;
    }

    /**
     * Wraps the actual request in a proxy request object that encodes the target node.
     */
    public static TransportRequest wrapRequest(DiscoveryNode node, TransportRequest request) {
        return new ProxyRequest<>(request, node);
    }

    /**
     * Unwraps a proxy request and returns the original request
     */
    public static TransportRequest unwrapRequest(TransportRequest request) {
        if (request instanceof ProxyRequest) {
            return ((ProxyRequest<?>)request).wrapped;
        }
        return request;
    }

    /**
     * Unwraps a proxy action and returns the underlying action
     */
    public static String unwrapAction(String action) {
        assert isProxyAction(action) : "Attempted to unwrap non-proxy action: " + action;
        return action.substring(PROXY_ACTION_PREFIX.length());
    }

    /**
     * Returns <code>true</code> iff the given action is a proxy action
     */
    public static boolean isProxyAction(String action) {
        return action.startsWith(PROXY_ACTION_PREFIX);
    }

    /**
     * Returns <code>true</code> iff the given request is a proxy request
     */
    public static boolean isProxyRequest(TransportRequest request) {
        return request instanceof ProxyRequest;
    }
}
