/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.CustomServerTransportFilterAuthenticator;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

final class CustomRemoteServerTransportFilter extends ServerTransportFilter {
    private static final Logger logger = LogManager.getLogger(CustomRemoteServerTransportFilter.class);

    private final CustomServerTransportFilterAuthenticator authenticator;

    CustomRemoteServerTransportFilter(
        CustomServerTransportFilterAuthenticator authenticator,
        AuthenticationService authcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext
    ) {
        super(authcService, authzService, threadContext, extractClientCert, destructiveOperations, securityContext);
        this.authenticator = authenticator;
    }

    @Override
    public void authenticate(String securityAction, TransportRequest request, ActionListener<Authentication> authenticationListener) {
        logger.info("Custom authenticator authenticating request for action: {}", securityAction);
        authenticator.authenticate(securityAction, request, authenticationListener);
    }
}
