/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.action.ShieldActionMapper;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.authz.accesscontrol.RequestContext;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_CLIENT_AUTH_DEFAULT;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_CLIENT_AUTH_SETTING;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_PROFILE_CLIENT_AUTH_SETTING;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_PROFILE_SSL_SETTING;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_SSL_DEFAULT;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.TRANSPORT_SSL_SETTING;

/**
 *
 */
public class ShieldServerTransportService extends TransportService {

    public static final String SETTING_NAME = "shield.type";

    protected final AuthenticationService authcService;
    protected final AuthorizationService authzService;
    protected final ShieldActionMapper actionMapper;
    protected final ClientTransportFilter clientFilter;
    protected final ShieldLicenseState licenseState;

    protected final Map<String, ServerTransportFilter> profileFilters;

    @Inject
    public ShieldServerTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                        AuthenticationService authcService,
                                        AuthorizationService authzService,
                                        ShieldActionMapper actionMapper,
                                        ClientTransportFilter clientTransportFilter,
                                        ShieldLicenseState licenseState) {
        super(settings, transport, threadPool);
        this.authcService = authcService;
        this.authzService = authzService;
        this.actionMapper = actionMapper;
        this.clientFilter = clientTransportFilter;
        this.licenseState = licenseState;
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
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory, String executor, TransportRequestHandler<Request> handler) {
        TransportRequestHandler<Request> wrappedHandler = new ProfileSecuredRequestHandler<>(action, handler, profileFilters, licenseState);
        super.registerRequestHandler(action, requestFactory, executor, wrappedHandler);
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request, String executor, boolean forceExecution, TransportRequestHandler<Request> handler) {
        TransportRequestHandler<Request> wrappedHandler = new ProfileSecuredRequestHandler<>(action, handler, profileFilters, licenseState);
        super.registerRequestHandler(action, request, executor, forceExecution, wrappedHandler);
    }

    protected Map<String, ServerTransportFilter> initializeProfileFilters() {
        if (!(transport instanceof ShieldNettyTransport)) {
            return Collections.<String, ServerTransportFilter>singletonMap(NettyTransport.DEFAULT_PROFILE, new ServerTransportFilter.NodeProfile(authcService, authzService, actionMapper, false));
        }

        Map<String, Settings> profileSettingsMap = settings.getGroups("transport.profiles.", true);
        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileSettingsMap.size() + 1);

        for (Map.Entry<String, Settings> entry : profileSettingsMap.entrySet()) {
            Settings profileSettings = entry.getValue();
            final boolean profileSsl = profileSettings.getAsBoolean(TRANSPORT_PROFILE_SSL_SETTING, settings.getAsBoolean(TRANSPORT_SSL_SETTING, TRANSPORT_SSL_DEFAULT));
            final boolean clientAuth = SSLClientAuth.parse(profileSettings.get(TRANSPORT_PROFILE_CLIENT_AUTH_SETTING, settings.get(TRANSPORT_CLIENT_AUTH_SETTING)), TRANSPORT_CLIENT_AUTH_DEFAULT).enabled();
            final boolean extractClientCert = profileSsl && clientAuth;
            String type = entry.getValue().get(SETTING_NAME, "node");
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.ClientProfile(authcService, authzService, actionMapper, extractClientCert));
                    break;
                default:
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.NodeProfile(authcService, authzService, actionMapper, extractClientCert));
            }
        }

        if (!profileFilters.containsKey(NettyTransport.DEFAULT_PROFILE)) {
            final boolean profileSsl = settings.getAsBoolean(TRANSPORT_SSL_SETTING, TRANSPORT_SSL_DEFAULT);
            final boolean clientAuth = SSLClientAuth.parse(settings.get(TRANSPORT_CLIENT_AUTH_SETTING), TRANSPORT_CLIENT_AUTH_DEFAULT).enabled();
            final boolean extractClientCert = profileSsl && clientAuth;
            profileFilters.put(NettyTransport.DEFAULT_PROFILE, new ServerTransportFilter.NodeProfile(authcService, authzService, actionMapper, extractClientCert));
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    ServerTransportFilter transportFilter(String profile) {
        return profileFilters.get(profile);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        protected final String action;
        protected final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final ShieldLicenseState licenseState;

        public ProfileSecuredRequestHandler(String action, TransportRequestHandler<T> handler, Map<String, ServerTransportFilter> profileFilters, ShieldLicenseState licenseState) {
            this.action = action;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.licenseState = licenseState;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void messageReceived(T request, TransportChannel channel) throws Exception {
            try {
                if (licenseState.securityEnabled()) {
                    String profile = channel.getProfileName();
                    ServerTransportFilter filter = profileFilters.get(profile);

                    if (filter == null) {
                        if (TransportService.DIRECT_RESPONSE_PROFILE.equals(profile)) {
                            // apply the default filter to local requests. We never know what the request is or who sent it...
                            filter = profileFilters.get("default");
                        } else {
                            throw new IllegalStateException("transport profile [" + profile + "] is not associated with a transport filter");
                        }
                    }
                    assert filter != null;
                    filter.inbound(action, request, channel);
                }
                RequestContext context = new RequestContext(request);
                RequestContext.setCurrent(context);
                handler.messageReceived(request, channel);
            } catch (Throwable t) {
                channel.sendResponse(t);
            } finally {
                RequestContext.removeCurrent();
            }
        }
    }

}
