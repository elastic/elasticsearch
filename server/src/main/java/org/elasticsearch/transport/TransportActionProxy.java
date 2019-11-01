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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * TransportActionProxy allows an arbitrary action to be executed on a defined target node while the initial request is sent to a second
 * node that acts as a request proxy to the target node. This is useful if a node is not directly connected to a target node but is
 * connected to an intermediate node that establishes a transitive connection.
 */
public final class TransportActionProxy {

    private TransportActionProxy() {} // no instance

    private static class ProxyRequestHandler<T extends ProxyRequest> implements TransportRequestHandler<T> {

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
            service.sendRequest(targetNode, action, wrappedRequest,
                    new ProxyResponseHandler<>(channel, responseFunction.apply(wrappedRequest)));
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

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            wrapped.writeTo(out);
        }
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
     * response type changes based on the upcoming request (quite rare)
     */
    public static void registerProxyActionWithDynamicResponseType(TransportService service, String action,
                                                                  Function<TransportRequest,
                                                                      Writeable.Reader<? extends TransportResponse>> responseFunction) {
        RequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandler(action);
        service.registerRequestHandler(getProxyAction(action), ThreadPool.Names.SAME, true, false,
            in -> new ProxyRequest<>(in, requestHandler::newRequest), new ProxyRequestHandler<>(service, action, responseFunction));
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
     * response type is always the same (most of the cases).
     */
    public static void registerProxyAction(TransportService service, String action,
                                           Writeable.Reader<? extends TransportResponse> reader) {
        RequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandler(action);
        service.registerRequestHandler(getProxyAction(action), ThreadPool.Names.SAME, true, false,
            in -> new ProxyRequest<>(in, requestHandler::newRequest), new ProxyRequestHandler<>(service, action, request -> reader));
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
            return ((ProxyRequest)request).wrapped;
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
