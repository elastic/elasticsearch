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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

final class CrossClusterAccessServerTransportFilter extends ServerTransportFilter {

    private static final Logger logger = LogManager.getLogger(CrossClusterAccessServerTransportFilter.class);

    // pkg-private for testing
    static final Set<String> ALLOWED_TRANSPORT_HEADERS;
    static {
        final Set<String> allowedHeaders = new HashSet<>(
            Set.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
        );
        allowedHeaders.add(AuditUtil.AUDIT_REQUEST_ID);
        allowedHeaders.addAll(Task.HEADERS_TO_COPY);
        ALLOWED_TRANSPORT_HEADERS = Set.copyOf(allowedHeaders);
    }

    private final CrossClusterAccessAuthenticationService crossClusterAccessAuthcService;
    private final XPackLicenseState licenseState;

    CrossClusterAccessServerTransportFilter(
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        AuthorizationService authzService,
        ThreadContext threadContext,
        boolean extractClientCert,
        DestructiveOperations destructiveOperations,
        SecurityContext securityContext,
        XPackLicenseState licenseState
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
        this.licenseState = licenseState;
    }

    @Override
    protected void authenticate(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener
    ) {
        if (false == Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.check(licenseState)) {
            onFailureWithDebugLog(
                securityAction,
                request,
                authenticationListener,
                LicenseUtils.newComplianceException(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.getName())
            );
        } else {
            try {
                validateHeaders();
            } catch (Exception ex) {
                onFailureWithDebugLog(securityAction, request, authenticationListener, ex);
                return;
            }
            crossClusterAccessAuthcService.authenticate(securityAction, request, authenticationListener);
        }
    }

    private void validateHeaders() {
        final ThreadContext threadContext = getThreadContext();
        ensureRequiredHeaderInContext(threadContext, CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
        ensureRequiredHeaderInContext(threadContext, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        for (String header : threadContext.getHeaders().keySet()) {
            if (false == ALLOWED_TRANSPORT_HEADERS.contains(header)) {
                throw new IllegalArgumentException(
                    "Transport request header ["
                        + header
                        + "] is not allowed for cross cluster requests through the dedicated remote cluster server port"
                );
            }
        }
    }

    private static void ensureRequiredHeaderInContext(ThreadContext threadContext, String requiredHeader) {
        if (threadContext.getHeader(requiredHeader) == null) {
            throw new IllegalArgumentException(
                "Cross cluster requests through the dedicated remote cluster server port require transport header ["
                    + requiredHeader
                    + "] but none found. "
                    + "Please ensure you have configured remote cluster credentials on the cluster originating the request."
            );
        }
    }

    private static void onFailureWithDebugLog(
        final String securityAction,
        final TransportRequest request,
        final ActionListener<Authentication> authenticationListener,
        final Exception ex
    ) {
        logger.debug(
            () -> format(
                "Cross cluster access request [%s] for action [%s] rejected before authentication",
                request.getClass(),
                securityAction
            ),
            ex
        );
        authenticationListener.onFailure(ex);
    }
}
