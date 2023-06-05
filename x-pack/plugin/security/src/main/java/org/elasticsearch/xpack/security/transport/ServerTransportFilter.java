/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TaskTransportChannel;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

/**
 * The server transport filter that should be used in nodes as it ensures that an incoming
 * request is properly authenticated and authorized
 */
class ServerTransportFilter {

    private static final Logger logger = LogManager.getLogger(ServerTransportFilter.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SecurityActionMapper actionMapper = new SecurityActionMapper();
    private final ThreadContext threadContext;
    private final boolean extractClientCert;
    private final DestructiveOperations destructiveOperations;
    private final SecurityContext securityContext;

    ServerTransportFilter(
        AuthenticationService authcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext
    ) {
        this.authcService = authcService;
        this.authzService = authzService;
        this.threadContext = threadContext;
        this.extractClientCert = extractClientCert;
        this.destructiveOperations = destructiveOperations;
        this.securityContext = securityContext;
    }

    /**
     * Called just after the given request was received by the transport. Any exception
     * thrown by this method will stop the request from being handled and the error will
     * be sent back to the sender.
     */
    void inbound(String action, TransportRequest request, TransportChannel transportChannel, ActionListener<Void> listener) {
        if (CloseIndexAction.NAME.equals(action) || OpenIndexAction.NAME.equals(action) || DeleteIndexAction.NAME.equals(action)) {
            IndicesRequest indicesRequest = (IndicesRequest) request;
            try {
                destructiveOperations.failDestructive(indicesRequest.indices());
            } catch (IllegalArgumentException e) {
                listener.onFailure(e);
                return;
            }
        }
        /*
         here we don't have a fallback user, as all incoming request are
         expected to have a user attached (either in headers or in context)
         We can make this assumption because in nodes we make sure all outgoing
         requests from all the nodes are attached with a user (either a serialize
         user an authentication token
         */
        String securityAction = SecurityActionMapper.action(action, request);

        TransportChannel unwrappedChannel = transportChannel;
        if (unwrappedChannel instanceof TaskTransportChannel) {
            unwrappedChannel = ((TaskTransportChannel) unwrappedChannel).getChannel();
        }

        if (extractClientCert && (unwrappedChannel instanceof TcpTransportChannel)) {
            TcpChannel tcpChannel = ((TcpTransportChannel) unwrappedChannel).getChannel();
            if (tcpChannel instanceof Netty4TcpChannel) {
                if (tcpChannel.isOpen()) {
                    SSLEngineUtils.extractClientCertificates(logger, threadContext, tcpChannel);
                }
            }
        }

        TransportVersion version = transportChannel.getVersion();
        authenticate(securityAction, request, listener.wrapFailure((l, authentication) -> {
            if (authentication != null) {
                if (securityAction.equals(TransportService.HANDSHAKE_ACTION_NAME)
                    && SystemUser.is(authentication.getEffectiveSubject().getUser()) == false) {
                    securityContext.executeAsSystemUser(version, original -> {
                        final Authentication replaced = securityContext.getAuthentication();
                        authzService.authorize(replaced, securityAction, request, l);
                    });
                } else {
                    authzService.authorize(authentication, securityAction, request, l);
                }
            } else {
                l.onFailure(new IllegalStateException("no authentication present but auth is allowed"));
            }
        }));
    }

    protected void authenticate(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener
    ) {
        authcService.authenticate(securityAction, request, true, authenticationListener);
    }

    protected final ThreadContext getThreadContext() {
        return threadContext;
    }

    // Package private for testing
    boolean isExtractClientCert() {
        return extractClientCert;
    }

}
