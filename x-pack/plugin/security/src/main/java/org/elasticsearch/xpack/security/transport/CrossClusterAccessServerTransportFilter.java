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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

final class CrossClusterAccessServerTransportFilter extends ServerTransportFilter {
    // pkg-private for testing
    static final Set<String> ALLOWED_TRANSPORT_HEADERS;
    static {
        final Set<String> allowedHeaders = new HashSet<>(
            Set.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
        );
        allowedHeaders.addAll(Task.HEADERS_TO_COPY);
        ALLOWED_TRANSPORT_HEADERS = Set.copyOf(allowedHeaders);
    }

    private final CrossClusterAccessAuthenticationService crossClusterAccessAuthcService;

    CrossClusterAccessServerTransportFilter(
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext
    ) {
        super(
            crossClusterAccessAuthcService.getAuthenticationService(),
            authzService,
            threadContext,
            extractClientCert,
            destructiveOperations,
            securityContext
        );
        this.crossClusterAccessAuthcService = crossClusterAccessAuthcService;
    }

    @Override
    protected void authenticate(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener
    ) {
        if (false == SecurityServerTransportInterceptor.CROSS_CLUSTER_ACCESS_ACTION_ALLOWLIST.contains(securityAction)) {
            authenticationListener.onFailure(
                new IllegalArgumentException(
                    "action ["
                        + securityAction
                        + "] is not allowed as a cross cluster operation on the dedicated remote cluster server port"
                )
            );
        } else {
            try {
                ensureOnlyAllowedHeadersInThreadContext();
            } catch (Exception ex) {
                authenticationListener.onFailure(ex);
                return;
            }
            crossClusterAccessAuthcService.authenticate(securityAction, request, authenticationListener);
        }
    }

    private void ensureOnlyAllowedHeadersInThreadContext() {
        for (String header : getThreadContext().getHeaders().keySet()) {
            if (false == ALLOWED_TRANSPORT_HEADERS.contains(header)) {
                throw new IllegalArgumentException(
                    "transport request header ["
                        + header
                        + "] is not allowed for cross cluster requests through the dedicated remote cluster server port"
                );
            }
        }
    }
}
