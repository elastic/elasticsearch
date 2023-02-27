/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * RemoteClusterActionProxy wraps internal action executed by system user as normal cluster actions so that they
 * can be granted with normal privileges.
 */
public final class RemoteClusterActionProxy {

    private static final Logger logger = LogManager.getLogger(RemoteClusterActionProxy.class);

    private RemoteClusterActionProxy() {} // no instance

    private static class ProxyRequestHandler<T extends ProxyRequest<TransportRequest>> implements TransportRequestHandler<T> {

        private final TransportService service;
        private final String action;
        private final Function<TransportRequest, Writeable.Reader<? extends TransportResponse>> responseFunction;

        ProxyRequestHandler(
            TransportService service,
            String action,
            Function<TransportRequest, Writeable.Reader<? extends TransportResponse>> responseFunction
        ) {
            this.service = service;
            this.action = action;
            this.responseFunction = responseFunction;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            final DiscoveryNode targetNode = request.targetNode != null ? request.targetNode : service.getLocalNode();
            TransportRequest wrappedRequest = request.wrapped;
            TaskId taskId = task.taskInfo(service.localNode.getId(), false).taskId();
            wrappedRequest.setParentTask(taskId);
            final ThreadContext threadContext = service.getThreadPool().getThreadContext();
            try (var ignore = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                service.sendRequest(
                    targetNode,
                    action,
                    wrappedRequest,
                    new ProxyResponseHandler<>(channel, responseFunction.apply(request))
                );
            }
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
        @Nullable
        final DiscoveryNode targetNode;

        ProxyRequest(T wrapped, @Nullable DiscoveryNode targetNode) {
            this.wrapped = wrapped;
            this.targetNode = targetNode;
        }

        ProxyRequest(StreamInput in, Writeable.Reader<T> reader) throws IOException {
            super(in);
            targetNode = in.readOptionalWriteable(DiscoveryNode::new);
            wrapped = reader.read(in);
            setParentTask(wrapped.getParentTask());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(targetNode);
            wrapped.writeTo(out);
        }
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
     * response type is always the same (most of the cases).
     */
    public static void registerProxyAction(TransportService service, String action, Writeable.Reader<? extends TransportResponse> reader) {
        RequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandler(action);
        if (requestHandler == null) {
            throw new IllegalArgumentException("no request handler found for [" + action + "]");
        }
        service.registerRequestHandler(
            getProxyAction(action),
            ThreadPool.Names.SAME,
            true,
            false,
            in -> new ProxyRequest<>(in, requestHandler::newRequest),
            new ProxyRequestHandler<>(service, action, request -> reader)
        );
    }

    private static final String REMOTE_CLUSTER_PROXY_ACTION_PREFIX = "cluster:internal/remote_cluster/proxy/";

    /**
     * Returns the corresponding proxy action for the given action
     */
    public static String getProxyAction(String action) {
        return REMOTE_CLUSTER_PROXY_ACTION_PREFIX + action;
    }

    /**
     * Wraps the actual request in a proxy request object that encodes the target node.
     */
    public static TransportRequest wrapRequest(@Nullable DiscoveryNode node, TransportRequest request) {
        return new ProxyRequest<>(request, node);
    }
}
