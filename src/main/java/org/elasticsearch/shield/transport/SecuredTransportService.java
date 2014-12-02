/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.shield.transport.netty.SecuredMessageChannelHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

/**
 *
 */
public class SecuredTransportService extends TransportService {

    private final ClientTransportFilter clientFilter;
    private final ServerTransportFilters serverTransportFilters;

    @Inject
    public SecuredTransportService(Settings settings, Transport transport, ThreadPool threadPool, ClientTransportFilter clientFilter, ServerTransportFilters serverTransportFilters) {
        super(settings, transport, threadPool);
        this.clientFilter = clientFilter;
        this.serverTransportFilters = serverTransportFilters;
    }

    @Override
    public void registerHandler(String action, TransportRequestHandler handler) {
        // Only try to access the profile, if we use netty and SSL
        // otherwise use the regular secured request handler (this still allows for LocalTransport)
        if (transport instanceof NettySecuredTransport) {
            super.registerHandler(action, new ProfileSecuredRequestHandler(action, handler, serverTransportFilters));
        } else {
            super.registerHandler(action, new SecuredRequestHandler(action, handler, serverTransportFilters));
        }
    }

    @Override
    public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) {
        try {
            clientFilter.outbound(action, request);
            super.sendRequest(node, action, request, options, handler);
        } catch (Throwable t) {
            handler.handleException(new TransportException("failed sending request", t));
        }
    }

    static abstract class AbstractSecuredRequestHandler implements TransportRequestHandler {

        protected TransportRequestHandler handler;

        public AbstractSecuredRequestHandler(TransportRequestHandler handler) {
            this.handler = handler;
        }

        @Override
        public TransportRequest newInstance() {
            return handler.newInstance();
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

    static class SecuredRequestHandler extends AbstractSecuredRequestHandler {

        protected final String action;
        protected final ServerTransportFilter transportFilter;

        SecuredRequestHandler(String action, TransportRequestHandler handler, ServerTransportFilters serverTransportFilters) {
            super(handler);
            this.action = action;
            this.transportFilter = serverTransportFilters.getTransportFilterForProfile("default");
        }

        @Override @SuppressWarnings("unchecked")
        public void messageReceived(TransportRequest request, TransportChannel channel) throws Exception {
            try {
                transportFilter.inbound(action, request);
            } catch (Throwable t) {
                channel.sendResponse(t);
                return;
            }
            handler.messageReceived(request, channel);
        }
    }

    static class ProfileSecuredRequestHandler extends AbstractSecuredRequestHandler {

        protected final String action;
        protected final ServerTransportFilters serverTransportFilters;

        ProfileSecuredRequestHandler(String action, TransportRequestHandler handler, ServerTransportFilters serverTransportFilters) {
            super(handler);
            this.action = action;
            this.serverTransportFilters = serverTransportFilters;
        }

        @Override @SuppressWarnings("unchecked")
        public void messageReceived(TransportRequest request, TransportChannel channel) throws Exception {
            try {
                SecuredMessageChannelHandler.VisibleNettyTransportChannel nettyTransportChannel = (SecuredMessageChannelHandler.VisibleNettyTransportChannel) channel;
                String profile = nettyTransportChannel.getProfile();
                ServerTransportFilter filter = serverTransportFilters.getTransportFilterForProfile(profile);
                filter.inbound(action, request);
            } catch (Throwable t) {
                channel.sendResponse(t);
                return;
            }
            handler.messageReceived(request, channel);
        }
    }
}
