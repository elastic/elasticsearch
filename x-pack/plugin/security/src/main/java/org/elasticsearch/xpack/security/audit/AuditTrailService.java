/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

public class AuditTrailService {

    private static final Logger logger = LogManager.getLogger(AuditTrailService.class);

    private static final AuditTrail NOOP_AUDIT_TRAIL = new NoopAuditTrail();
    private final @Nullable AuditTrail auditTrail;
    private final XPackLicenseState licenseState;
    private final Duration minLogPeriod = Duration.ofMinutes(30);
    protected AtomicReference<Instant> nextLogInstantAtomic = new AtomicReference<>(Instant.EPOCH);

    public AuditTrailService(@Nullable AuditTrail auditTrail, XPackLicenseState licenseState) {
        this.auditTrail = auditTrail;
        this.licenseState = licenseState;
    }

    public AuditTrail get() {
        if (auditTrail != null) {
            if (Security.AUDITING_FEATURE.check(licenseState)) {
                return auditTrail;
            } else {
                maybeLogAuditingDisabled();
                return NOOP_AUDIT_TRAIL;
            }
        } else {
            return NOOP_AUDIT_TRAIL;
        }
    }

    // TODO: this method only exists for access to LoggingAuditTrail in a Node for testing.
    // DO NOT USE IT, IT WILL BE REMOVED IN THE FUTURE
    public AuditTrail getAuditTrail() {
        return auditTrail;
    }

    private void maybeLogAuditingDisabled() {
        Instant nowInstant = Instant.now();
        Instant nextLogInstant = nextLogInstantAtomic.get();
        if (nextLogInstant.isBefore(nowInstant)) {
            if (nextLogInstantAtomic.compareAndSet(nextLogInstant, nowInstant.plus(minLogPeriod))) {
                logger.warn(
                    "Auditing logging is DISABLED because the currently active license ["
                        + licenseState.getOperationMode()
                        + "] does not permit it"
                );
            }
        }
    }

    private static class NoopAuditTrail implements AuditTrail {

        @Override
        public String name() {
            return "noop";
        }

        @Override
        public void authenticationSuccess(RestRequest request) {}

        @Override
        public void authenticationSuccess(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest
        ) {}

        @Override
        public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void anonymousAccessDenied(String requestId, HttpPreRequest request) {}

        @Override
        public void authenticationFailed(String requestId, HttpPreRequest request) {}

        @Override
        public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {}

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, HttpPreRequest request) {}

        @Override
        public void authenticationFailed(
            String requestId,
            String realm,
            AuthenticationToken token,
            String action,
            TransportRequest transportRequest
        ) {}

        @Override
        public void authenticationFailed(String requestId, String realm, AuthenticationToken token, HttpPreRequest request) {}

        @Override
        public void accessGranted(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void accessDenied(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void tamperedRequest(String requestId, HttpPreRequest request) {}

        @Override
        public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {}

        @Override
        public void connectionGranted(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule) {}

        @Override
        public void connectionDenied(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule) {}

        @Override
        public void runAsGranted(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void runAsDenied(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void runAsDenied(
            String requestId,
            Authentication authentication,
            HttpPreRequest request,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void explicitIndexAccessEvent(
            String requestId,
            AuditLevel eventType,
            Authentication authentication,
            String action,
            String indices,
            String requestName,
            InetSocketAddress remoteAddress,
            AuthorizationInfo authorizationInfo
        ) {}

        @Override
        public void coordinatingActionResponse(
            String requestId,
            Authentication authentication,
            String action,
            TransportRequest transportRequest,
            TransportResponse transportResponse
        ) {}
    }
}
