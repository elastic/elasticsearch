/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.license.License;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.junit.Before;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuditTrailServiceTests extends ESTestCase {
    private List<AuditTrail> auditTrails;
    private AuditTrailService service;

    private AuthenticationToken token;
    private TransportRequest request;
    private RestRequest restRequest;
    private MockLicenseState licenseState;
    private boolean isAuditingAllowed;

    @Before
    public void init() throws Exception {
        List<AuditTrail> auditTrailsBuilder = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            auditTrailsBuilder.add(mock(AuditTrail.class));
        }
        auditTrails = unmodifiableList(auditTrailsBuilder);
        licenseState = mock(MockLicenseState.class);
        service = new AuditTrailService(auditTrails, licenseState);
        isAuditingAllowed = randomBoolean();
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(isAuditingAllowed);
        token = mock(AuthenticationToken.class);
        request = mock(TransportRequest.class);
        restRequest = mock(RestRequest.class);
    }

    public void testLogWhenLicenseProhibitsAuditing() throws Exception {
        MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.start();
        Logger auditTrailServiceLogger = LogManager.getLogger(AuditTrailService.class);
        Loggers.addAppender(auditTrailServiceLogger, mockLogAppender);
        when(licenseState.getOperationMode()).thenReturn(randomFrom(License.OperationMode.values()));
        if (isAuditingAllowed) {
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "audit disabled because of license",
                    AuditTrailService.class.getName(),
                    Level.WARN,
                    "Auditing logging is DISABLED because the currently active license ["
                        + licenseState.getOperationMode()
                        + "] does not permit it"
                )
            );
        } else {
            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "audit disabled because of license",
                    AuditTrailService.class.getName(),
                    Level.WARN,
                    "Auditing logging is DISABLED because the currently active license ["
                        + licenseState.getOperationMode()
                        + "] does not permit it"
                )
            );
        }
        for (int i = 1; i <= randomIntBetween(2, 6); i++) {
            service.get();
        }
        mockLogAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(auditTrailServiceLogger, mockLogAppender);
    }

    public void testNoLogRecentlyWhenLicenseProhibitsAuditing() throws Exception {
        MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.start();
        Logger auditTrailServiceLogger = LogManager.getLogger(AuditTrailService.class);
        Loggers.addAppender(auditTrailServiceLogger, mockLogAppender);
        service.nextLogInstantAtomic.set(randomFrom(Instant.now().minus(Duration.ofMinutes(5)), Instant.now()));
        mockLogAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "audit disabled because of license",
                AuditTrailService.class.getName(),
                Level.WARN,
                "Security auditing is DISABLED because the currently active license [*] does not permit it"
            )
        );
        for (int i = 1; i <= randomIntBetween(2, 6); i++) {
            service.get();
        }
        mockLogAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(auditTrailServiceLogger, mockLogAppender);
    }

    public void testAuthenticationFailed() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, token, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, token, "_action", request);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, "_action", request);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, restRequest.getHttpRequest());
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRest() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, token, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, token, restRequest.getHttpRequest());
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_realm", token, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, "_realm", token, "_action", request);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_realm", token, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(requestId, "_realm", token, restRequest.getHttpRequest());
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAnonymousAccess() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().anonymousAccessDenied(requestId, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).anonymousAccessDenied(requestId, "_action", request);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAccessGranted() throws Exception {
        Authentication authentication = new Authentication(
            new User("_username", "r1"),
            new RealmRef(null, null, null),
            new RealmRef(null, null, null)
        );
        AuthorizationInfo authzInfo = () -> Collections.singletonMap(
            PRINCIPAL_ROLES_FIELD_NAME,
            new String[] { randomAlphaOfLengthBetween(1, 6) }
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().accessGranted(requestId, authentication, "_action", request, authzInfo);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).accessGranted(requestId, authentication, "_action", request, authzInfo);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAccessDenied() throws Exception {
        Authentication authentication = new Authentication(
            new User("_username", "r1"),
            new RealmRef(null, null, null),
            new RealmRef(null, null, null)
        );
        AuthorizationInfo authzInfo = () -> Collections.singletonMap(
            PRINCIPAL_ROLES_FIELD_NAME,
            new String[] { randomAlphaOfLengthBetween(1, 6) }
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().accessDenied(requestId, authentication, "_action", request, authzInfo);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).accessDenied(requestId, authentication, "_action", request, authzInfo);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testConnectionGranted() throws Exception {
        InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        SecurityIpFilterRule rule = randomBoolean() ? SecurityIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.get().connectionGranted(inetAddress, "client", rule);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).connectionGranted(inetAddress, "client", rule);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testConnectionDenied() throws Exception {
        InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        service.get().connectionDenied(inetAddress, "client", rule);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).connectionDenied(inetAddress, "client", rule);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationSuccessRest() throws Exception {
        Authentication authentication = new Authentication(
            new User("_username", "r1"),
            new RealmRef("_realm", null, null),
            new RealmRef(null, null, null)
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationSuccess(restRequest);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationSuccess(restRequest);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        Authentication authentication = new Authentication(
            new User("_username", "r1"),
            new RealmRef("_realm", null, null),
            new RealmRef(null, null, null)
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationSuccess(requestId, authentication, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationSuccess(requestId, authentication, "_action", request);
            }
        } else {
            verifyNoMoreInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }
}
