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
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
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
import java.util.Collections;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuditTrailServiceTests extends ESTestCase {
    private AuditTrail auditTrail;
    private AuditTrailService service;

    private AuthenticationToken token;
    private TransportRequest request;
    private RestRequest restRequest;
    private MockLicenseState licenseState;
    private boolean isAuditingAllowed;

    @Before
    public void init() throws Exception {
        auditTrail = mock(AuditTrail.class);
        licenseState = mock(MockLicenseState.class);
        service = new AuditTrailService(auditTrail, licenseState);
        isAuditingAllowed = randomBoolean();
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
            verify(auditTrail).authenticationFailed(requestId, token, "_action", request);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationFailed(requestId, "_action", request);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationFailed(requestId, restRequest.getHttpRequest());
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationFailedRest() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, token, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationFailed(requestId, token, restRequest.getHttpRequest());
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationFailedRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_realm", token, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationFailed(requestId, "_realm", token, "_action", request);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationFailed(requestId, "_realm", token, restRequest.getHttpRequest());
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationFailed(requestId, "_realm", token, restRequest.getHttpRequest());
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAnonymousAccess() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().anonymousAccessDenied(requestId, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).anonymousAccessDenied(requestId, "_action", request);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAccessGranted() throws Exception {
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("not-_username", "not-r1"))
            .realmRef(new RealmRef("_realm", "_type", "node", null))
            .runAs()
            .user(new User("_username", "r1"))
            .realmRef(new RealmRef("_look", "_type", "node", null))
            .build();
        AuthorizationInfo authzInfo = () -> Collections.singletonMap(
            PRINCIPAL_ROLES_FIELD_NAME,
            new String[] { randomAlphaOfLengthBetween(1, 6) }
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().accessGranted(requestId, authentication, "_action", request, authzInfo);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).accessGranted(requestId, authentication, "_action", request, authzInfo);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAccessDenied() throws Exception {
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("not-_username", "not-r1"))
            .realmRef(new RealmRef("_realm", "_type", "node", null))
            .runAs()
            .user(new User("_username", "r1"))
            .realmRef(new RealmRef("_look", "_type", "node", null))
            .build();
        AuthorizationInfo authzInfo = () -> Collections.singletonMap(
            PRINCIPAL_ROLES_FIELD_NAME,
            new String[] { randomAlphaOfLengthBetween(1, 6) }
        );
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().accessDenied(requestId, authentication, "_action", request, authzInfo);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).accessDenied(requestId, authentication, "_action", request, authzInfo);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testConnectionGranted() throws Exception {
        InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        SecurityIpFilterRule rule = randomBoolean() ? SecurityIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.get().connectionGranted(inetAddress, "client", rule);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).connectionGranted(inetAddress, "client", rule);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testConnectionDenied() throws Exception {
        InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        service.get().connectionDenied(inetAddress, "client", rule);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).connectionDenied(inetAddress, "client", rule);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationSuccessRest() throws Exception {
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("not-_username", "not-r1"))
            .realmRef(new RealmRef("_realm", "_type", "node", null))
            .runAs()
            .user(new User("_username", "r1"))
            .realmRef(new RealmRef("_look", "_type", "node", null))
            .build();
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationSuccess(restRequest);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationSuccess(restRequest);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("not-_username", "not-r1"))
            .realmRef(new RealmRef("_realm", "_type", "node", null))
            .runAs()
            .user(new User("_username", "r1"))
            .realmRef(new RealmRef("_look", "_type", "node", null))
            .build();
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.get().authenticationSuccess(requestId, authentication, "_action", request);
        verify(licenseState).isAllowed(Security.AUDITING_FEATURE);
        if (isAuditingAllowed) {
            verify(auditTrail).authenticationSuccess(requestId, authentication, "_action", request);
        } else {
            verifyNoMoreInteractions(auditTrail);
        }
    }
}
