/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.netty.NettyTransportChannel;

import java.util.Map;

/**
 *
 */
public class ShieldServerTransportService extends TransportService {

    public static final String SETTING_NAME = "shield.type";

    private final ServerTransportFilter clientProfileFilter;
    private final ServerTransportFilter nodeProfileFilter;
    private final ClientTransportFilter clientFilter;
    private final Map<String, ServerTransportFilter> profileFilters;

    @Inject
    public ShieldServerTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                        ServerTransportFilter.ClientProfile clientProfileFilter,
                                        ServerTransportFilter.NodeProfile nodeProfileFilter,
                                        ClientTransportFilter clientTransportFilter) {
        super(settings, transport, threadPool);
        this.clientProfileFilter = clientProfileFilter;
        this.nodeProfileFilter = nodeProfileFilter;
        this.clientFilter = clientTransportFilter;
        this.profileFilters = initializeProfileFilters();
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

    @Override
    public void registerHandler(String action, TransportRequestHandler handler) {
        // Only try to access the profile, if we use netty and SSL
        // otherwise use the regular secured request handler (this still allows for LocalTransport)
        if (profileFilters != null) {
            super.registerHandler(action, new ProfileSecuredRequestHandler(action, handler, profileFilters));
        } else {
            super.registerHandler(action, new SecuredRequestHandler(action, handler, nodeProfileFilter));
        }
    }

    private Map<String, ServerTransportFilter> initializeProfileFilters() {
        if (!(transport instanceof ShieldNettyTransport)) {
            return null;
        }

        Map<String, Settings> profileSettings = settings.getGroups("transport.profiles.", true);
        Map<String, ServerTransportFilter> profileFilters = Maps.newHashMapWithExpectedSize(profileSettings.size());

        for (Map.Entry<String, Settings> entry : profileSettings.entrySet()) {
            String type = entry.getValue().get(SETTING_NAME, "node");
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), clientProfileFilter);
                    break;
                default:
                    profileFilters.put(entry.getKey(), nodeProfileFilter);
            }
        }

        if (!profileFilters.containsKey("default")) {
            profileFilters.put("default", nodeProfileFilter);
        }

        return profileFilters;
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

        SecuredRequestHandler(String action, TransportRequestHandler handler, ServerTransportFilter serverTransportFilter) {
            super(handler);
            this.action = action;
            this.transportFilter = serverTransportFilter;
        }

        @Override
        @SuppressWarnings("unchecked")
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
        private final Map<String, ServerTransportFilter> profileFilters;

        public ProfileSecuredRequestHandler(String action, TransportRequestHandler handler, Map<String, ServerTransportFilter> profileFilters) {
            super(handler);
            this.action = action;
            this.profileFilters = profileFilters;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void messageReceived(TransportRequest request, TransportChannel channel) throws Exception {
            try {
                NettyTransportChannel nettyTransportChannel = (NettyTransportChannel) channel;
                String profile = nettyTransportChannel.getProfileName();
                ServerTransportFilter filter = profileFilters.get(profile);
                if (filter == null) {
                    filter = profileFilters.get("default");
                }
                filter.inbound(action, request);
            } catch (Throwable t) {
                channel.sendResponse(t);
                return;
            }
            handler.messageReceived(request, channel);
        }
    }
}
