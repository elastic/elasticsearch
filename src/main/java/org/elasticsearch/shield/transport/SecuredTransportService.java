/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 *
 */
public class SecuredTransportService extends TransportService {

    private final TransportFilter filter;

    @Inject
    public SecuredTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportFilter filter) {
        super(settings, transport, threadPool);
        this.filter = filter;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        try {
            filter.outboundRequest(action, request);
        } catch (Throwable t) {
            handler.handleException(new TransportException("failed sending request", t));
            return;
        }
        super.sendRequest(node, action, request, options, new SecuredResponseHandler(handler, filter));
    }

    @Override
    public void registerHandler(String action, TransportRequestHandler handler) {
        super.registerHandler(action, new SecuredRequestHandler(action, handler, filter));
    }

    static class SecuredRequestHandler implements TransportRequestHandler {

        private final String action;
        private final TransportRequestHandler handler;
        private final TransportFilter filter;

        SecuredRequestHandler(String action, TransportRequestHandler handler, TransportFilter filter) {
            this.action = action;
            this.handler = handler;
            this.filter = filter;
        }

        @Override
        public TransportRequest newInstance() {
            return handler.newInstance();
        }

        @Override
        public void messageReceived(TransportRequest request, TransportChannel channel) throws Exception {
            try {
                filter.inboundRequest(action, request);
            } catch (Throwable t) {
                channel.sendResponse(t);
                return;
            }
            handler.messageReceived(request, new SecuredTransportChannel(channel, filter));
        }

        @Override
        public String executor() {
            return handler.executor();
        }

        @Override
        public boolean isForceExecution() {
            return handler.isForceExecution();
        }
    }

    static class SecuredResponseHandler implements TransportResponseHandler {

        private final TransportResponseHandler handler;
        private final TransportFilter filter;

        SecuredResponseHandler(TransportResponseHandler handler, TransportFilter filter) {
            this.handler = handler;
            this.filter = filter;
        }

        @Override
        public TransportResponse newInstance() {
            return handler.newInstance();
        }

        @Override
        public void handleResponse(TransportResponse response) {
            try {
                filter.inboundResponse(response);
            } catch (Throwable t) {
                handleException(new TransportException("response received but rejected locally", t));
                return;
            }
            handler.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            handler.handleException(exp);
        }

        @Override
        public String executor() {
            return handler.executor();
        }
    }

    static class SecuredTransportChannel implements TransportChannel {

        private final TransportChannel channel;
        private final TransportFilter filter;

        SecuredTransportChannel(TransportChannel channel, TransportFilter filter) {
            this.channel = channel;
            this.filter = filter;
        }

        @Override
        public String action() {
            return channel.action();
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            if (filter(response)) {
                channel.sendResponse(response);
            }
        }

        @Override
        public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
            if (filter(response)) {
                channel.sendResponse(response, options);
            }
        }

        private boolean filter(TransportResponse response) throws IOException {
            try {
                filter.outboundResponse(channel.action(), response);
            } catch (Throwable t) {
                channel.sendResponse(t);
                return false;
            }
            return true;
        }

        @Override
        public void sendResponse(Throwable error) throws IOException {
            channel.sendResponse(error);
        }
    }
}
