/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

public class RemoteAccessServerTransportFilter extends ServerTransportFilter {
    private final RemoteAccessAuthenticationService remoteAccessAuthcService;

    RemoteAccessServerTransportFilter(
        AuthenticationService authcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext
    ) {
        super(authcService, authzService, threadContext, extractClientCert, destructiveOperations, securityContext);
        this.remoteAccessAuthcService = new RemoteAccessAuthenticationService(authcService);
    }

    @Override
    protected void authenticate(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener
    ) {
        if (securityAction.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
            super.authenticate(securityAction, request, authenticationListener);
        } else {
            remoteAccessAuthcService.authenticate(securityAction, request, false, authenticationListener);
        }
    }
}
