/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class AuditTrailService {

    private static final Logger logger = LogManager.getLogger(AuditTrailService.class);

    private static final AuditTrail NOOP_AUDIT_TRAIL = new NoopAuditTrail();
    private final CompositeAuditTrail compositeAuditTrail;
    private final XPackLicenseState licenseState;
    private final Duration minLogPeriod = Duration.ofMinutes(30);
    protected AtomicReference<Instant> nextLogInstantAtomic = new AtomicReference<>(Instant.EPOCH);

    public AuditTrailService(List<AuditTrail> auditTrails, XPackLicenseState licenseState) {
        this.compositeAuditTrail = new CompositeAuditTrail(Collections.unmodifiableList(auditTrails));
        this.licenseState = licenseState;
    }

    public AuditTrail get() {
        if (compositeAuditTrail.isEmpty() == false) {
            if (licenseState.checkFeature(Feature.SECURITY_AUDITING)) {
                return compositeAuditTrail;
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
    public List<AuditTrail> getAuditTrails() {
        return compositeAuditTrail.auditTrails;
    }

    private void maybeLogAuditingDisabled() {
        Instant nowInstant = Instant.now();
        Instant nextLogInstant = nextLogInstantAtomic.get();
        if (nextLogInstant.isBefore(nowInstant)) {
            if (nextLogInstantAtomic.compareAndSet(nextLogInstant, nowInstant.plus(minLogPeriod))) {
                logger.warn("Auditing logging is DISABLED because the currently active license [" +
                        licenseState.getOperationMode() + "] does not permit it");
            }
        }
    }

    private static class NoopAuditTrail implements AuditTrail {

        @Override
        public String name() {
            return "noop";
        }

        @Override
        public void authenticationSuccess(String requestId, Authentication authentication, RestRequest request) {}

        @Override
        public void authenticationSuccess(String requestId, Authentication authentication, String action,
                                          TransportRequest transportRequest) {}

        @Override
        public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void anonymousAccessDenied(String requestId, RestRequest request) {}

        @Override
        public void authenticationFailed(String requestId, RestRequest request) {}

        @Override
        public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {}

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {}

        @Override
        public void authenticationFailed(String requestId, String realm, AuthenticationToken token,
                                         String action, TransportRequest transportRequest) {}

        @Override
        public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {}

        @Override
        public void accessGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                  AuthorizationInfo authorizationInfo) {}

        @Override
        public void accessDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                 AuthorizationInfo authorizationInfo) {}

        @Override
        public void tamperedRequest(String requestId, RestRequest request) {}

        @Override
        public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {}

        @Override
        public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {}

        @Override
        public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {}

        @Override
        public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {}

        @Override
        public void runAsGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                 AuthorizationInfo authorizationInfo) {}

        @Override
        public void runAsDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                AuthorizationInfo authorizationInfo) {}

        @Override
        public void runAsDenied(String requestId, Authentication authentication, RestRequest request,
                                AuthorizationInfo authorizationInfo) {}

        @Override
        public void explicitIndexAccessEvent(String requestId, AuditLevel eventType, Authentication authentication,
                                             String action, String indices, String requestName, TransportAddress remoteAddress,
                                             AuthorizationInfo authorizationInfo) {}

        @Override
        public void coordinatingActionResponse(String requestId, Authentication authentication, String action,
                                               TransportRequest transportRequest,
                                               TransportResponse transportResponse) { }
    }

    private static class CompositeAuditTrail implements AuditTrail {

        private final List<AuditTrail> auditTrails;

        private CompositeAuditTrail(List<AuditTrail> auditTrails) {
            this.auditTrails = auditTrails;
        }

        boolean isEmpty() {
            return auditTrails.isEmpty();
        }

        @Override
        public String name() {
            return "service";
        }

        @Override
        public void authenticationSuccess(String requestId, Authentication authentication, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(requestId, authentication, request);
            }
        }

        @Override
        public void authenticationSuccess(String requestId, Authentication authentication, String action,
                                          TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(requestId, authentication, action, transportRequest);
            }
        }

        @Override
        public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(requestId, action, transportRequest);
            }
        }

        @Override
        public void anonymousAccessDenied(String requestId, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(requestId, request);
            }
        }

        @Override
        public void authenticationFailed(String requestId, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, request);
            }
        }

        @Override
        public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, action, transportRequest);
            }
        }

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, token, action, transportRequest);
            }
        }

        @Override
        public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action,
                                         TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, realm, token, action, transportRequest);
            }
        }

        @Override
        public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, token, request);
            }
        }

        @Override
        public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, realm, token, request);
            }
        }

        @Override
        public void accessGranted(String requestId, Authentication authentication, String action, TransportRequest msg,
                                  AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessGranted(requestId, authentication, action, msg, authorizationInfo);
            }
        }

        @Override
        public void accessDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                 AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessDenied(requestId, authentication, action, transportRequest, authorizationInfo);
            }
        }

        @Override
        public void coordinatingActionResponse(String requestId, Authentication authentication, String action,
                                               TransportRequest transportRequest,
                                               TransportResponse transportResponse) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.coordinatingActionResponse(requestId, authentication, action, transportRequest, transportResponse);
            }
        }

        @Override
        public void tamperedRequest(String requestId, RestRequest request) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, request);
            }
        }

        @Override
        public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, action, transportRequest);
            }
        }

        @Override
        public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, authentication, action, transportRequest);
            }
        }

        @Override
        public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionGranted(inetAddress, profile, rule);
            }
        }

        @Override
        public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionDenied(inetAddress, profile, rule);
            }
        }

        @Override
        public void runAsGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                 AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsGranted(requestId, authentication, action, transportRequest, authorizationInfo);
            }
        }

        @Override
        public void runAsDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                                AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(requestId, authentication, action, transportRequest, authorizationInfo);
            }
        }

        @Override
        public void runAsDenied(String requestId, Authentication authentication, RestRequest request,
                                AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(requestId, authentication, request, authorizationInfo);
            }
        }

        @Override
        public void explicitIndexAccessEvent(String requestId, AuditLevel eventType, Authentication authentication, String action,
                                             String indices, String requestName, TransportAddress remoteAddress,
                                             AuthorizationInfo authorizationInfo) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.explicitIndexAccessEvent(requestId, eventType, authentication, action, indices, requestName, remoteAddress,
                    authorizationInfo);
            }
        }
    }
}
