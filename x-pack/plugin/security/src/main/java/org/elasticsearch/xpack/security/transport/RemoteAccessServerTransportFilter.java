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
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

final class RemoteAccessServerTransportFilter extends ServerTransportFilter {
    private final RemoteAccessAuthenticationService remoteAccessAuthcService;

    RemoteAccessServerTransportFilter(
        RemoteAccessAuthenticationService remoteAccessAuthcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext
    ) {
        super(
            remoteAccessAuthcService.getAuthenticationService(),
            authzService,
            threadContext,
            extractClientCert,
            destructiveOperations,
            securityContext
        );
        this.remoteAccessAuthcService = remoteAccessAuthcService;
    }

    @Override
    protected void authenticate(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener
    ) {
        if (securityAction.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
            super.authenticate(securityAction, request, authenticationListener);
        } else if (false == SecurityServerTransportInterceptor.REMOTE_ACCESS_ACTION_ALLOWLIST.contains(securityAction)) {
            authenticationListener.onFailure(
                new IllegalArgumentException("action [" + securityAction + "] is not allow-listed for remote access")
            );
        } else {
            remoteAccessAuthcService.authenticate(securityAction, request, false, authenticationListener);
        }
    }
}
