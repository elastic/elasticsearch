/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.action.ShieldActionMapper;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.authz.AuthorizationUtils;
import org.elasticsearch.shield.authz.accesscontrol.RequestContext;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.tasks.Task;
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
import org.elasticsearch.transport.TransportSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.CLIENT_AUTH_SETTING;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.PROFILE_CLIENT_AUTH_SETTING;
import static org.elasticsearch.shield.transport.netty.ShieldNettyTransport.SSL_SETTING;

/**
 *
 */
public class ShieldServerTransportService extends TransportService {

    public static final String SETTING_NAME = "shield.type";

    protected final AuthenticationService authcService;
    protected final AuthorizationService authzService;
    protected final ShieldActionMapper actionMapper;
    protected final ClientTransportFilter clientFilter;
    protected final SecurityLicenseState licenseState;

    protected final Map<String, ServerTransportFilter> profileFilters;

    @Inject
    public ShieldServerTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                        AuthenticationService authcService,
                                        AuthorizationService authzService,
                                        ShieldActionMapper actionMapper,
                                        ClientTransportFilter clientTransportFilter,
                                        SecurityLicenseState licenseState) {
        super(settings, transport, threadPool);
        this.authcService = authcService;
        this.authzService = authzService;
        this.actionMapper = actionMapper;
        this.clientFilter = clientTransportFilter;
        this.licenseState = licenseState;
        this.profileFilters = initializeProfileFilters();
    }

    @Override
    public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request,
                                                          TransportRequestOptions options, TransportResponseHandler<T> handler) {
        // Sometimes a system action gets executed like a internal create index request or update mappings request
        // which means that the user is copied over to system actions so we need to change the user
        if ((clientFilter instanceof ClientTransportFilter.Node) &&
                AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
            final ThreadContext.StoredContext original = threadPool.getThreadContext().newStoredContext();
            try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                try {
                    clientFilter.outbound(action, request);
                    super.sendRequest(node, action, request, options, new ContextRestoreResponseHandler<>(original, handler));
                } catch (Throwable t) {
                    handler.handleException(new TransportException("failed sending request", t));
                }
            }
        } else {
            try {
                clientFilter.outbound(action, request);
                super.sendRequest(node, action, request, options, handler);
            } catch (Throwable t) {
                handler.handleException(new TransportException("failed sending request", t));
            }
        }
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory, String
            executor, TransportRequestHandler<Request> handler) {
        TransportRequestHandler<Request> wrappedHandler = new ProfileSecuredRequestHandler<>(action, handler, profileFilters,
                licenseState, threadPool.getThreadContext());
        super.registerRequestHandler(action, requestFactory, executor, wrappedHandler);
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request, String executor,
                                                                          boolean forceExecution,
                                                                          TransportRequestHandler<Request> handler) {
        TransportRequestHandler<Request> wrappedHandler = new ProfileSecuredRequestHandler<>(action, handler, profileFilters,
                licenseState, threadPool.getThreadContext());
        super.registerRequestHandler(action, request, executor, forceExecution, wrappedHandler);
    }

    protected Map<String, ServerTransportFilter> initializeProfileFilters() {
        if (!(transport instanceof ShieldNettyTransport)) {
            return Collections.<String, ServerTransportFilter>singletonMap(TransportSettings.DEFAULT_PROFILE,
                    new ServerTransportFilter.NodeProfile(authcService, authzService, actionMapper, threadPool.getThreadContext(), false));
        }

        Map<String, Settings> profileSettingsMap = settings.getGroups("transport.profiles.", true);
        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileSettingsMap.size() + 1);

        for (Map.Entry<String, Settings> entry : profileSettingsMap.entrySet()) {
            Settings profileSettings = entry.getValue();
            final boolean profileSsl = ShieldNettyTransport.profileSsl(profileSettings, settings);
            final boolean clientAuth = PROFILE_CLIENT_AUTH_SETTING.get(profileSettings, settings).enabled();
            final boolean extractClientCert = profileSsl && clientAuth;
            String type = entry.getValue().get(SETTING_NAME, "node");
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.ClientProfile(authcService, authzService, actionMapper,
                            threadPool.getThreadContext(), extractClientCert));
                    break;
                default:
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.NodeProfile(authcService, authzService, actionMapper,
                            threadPool.getThreadContext(), extractClientCert));
            }
        }

        if (!profileFilters.containsKey(TransportSettings.DEFAULT_PROFILE)) {
            final boolean profileSsl = SSL_SETTING.get(settings);
            final boolean clientAuth = CLIENT_AUTH_SETTING.get(settings).enabled();
            final boolean extractClientCert = profileSsl && clientAuth;
            profileFilters.put(TransportSettings.DEFAULT_PROFILE, new ServerTransportFilter.NodeProfile(authcService, authzService,
                    actionMapper, threadPool.getThreadContext(), extractClientCert));
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
        private final SecurityLicenseState licenseState;
        private final ThreadContext threadContext;

        public ProfileSecuredRequestHandler(String action, TransportRequestHandler<T> handler,
                                            Map<String, ServerTransportFilter> profileFilters, SecurityLicenseState licenseState,
                                            ThreadContext threadContext) {
            this.action = action;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.licenseState = licenseState;
            this.threadContext = threadContext;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContext()) {
                if (licenseState.authenticationAndAuthorizationEnabled()) {
                    String profile = channel.getProfileName();
                    ServerTransportFilter filter = profileFilters.get(profile);

                    if (filter == null) {
                        if (TransportService.DIRECT_RESPONSE_PROFILE.equals(profile)) {
                            // apply the default filter to local requests. We never know what the request is or who sent it...
                            filter = profileFilters.get("default");
                        } else {
                            String msg = "transport profile [" + profile + "] is not associated with a transport filter";
                            throw new IllegalStateException(msg);
                        }
                    }
                    assert filter != null;
                    filter.inbound(action, request, channel);
                }
                // FIXME we should remove the RequestContext completely since we have ThreadContext but cannot yet due to the query cache
                RequestContext context = new RequestContext(request, threadContext);
                RequestContext.setCurrent(context);
                handler.messageReceived(request, channel, task);
            } catch (Throwable t) {
                channel.sendResponse(t);
            } finally {
                RequestContext.removeCurrent();
            }
        }

        @Override
        public void messageReceived(T request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("task parameter is required for this operation");
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the4 handle methods
     * are invoked we restore the context.
     */
    private final static class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {
        private final TransportResponseHandler<T> delegate;
        private final ThreadContext.StoredContext threadContext;

        private ContextRestoreResponseHandler(ThreadContext.StoredContext threadContext, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.threadContext = threadContext;
        }

        @Override
        public T newInstance() {
            return delegate.newInstance();
        }

        @Override
        public void handleResponse(T response) {
            threadContext.restore();
            delegate.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            threadContext.restore();
            delegate.handleException(exp);
        }

        @Override
        public String executor() {
            return delegate.executor();
        }
    }
}
