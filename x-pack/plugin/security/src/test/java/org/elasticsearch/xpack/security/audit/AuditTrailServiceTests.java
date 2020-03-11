/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AuditTrailServiceTests extends ESTestCase {
    private List<AuditTrail> auditTrails;
    private AuditTrailService service;

    private AuthenticationToken token;
    private TransportMessage message;
    private RestRequest restRequest;

    @Before
    public void init() throws Exception {
        List<AuditTrail> auditTrailsBuilder = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            auditTrailsBuilder.add(mock(AuditTrail.class));
        }
        auditTrails = unmodifiableList(auditTrailsBuilder);
        service = new AuditTrailService(auditTrails);
        token = mock(AuthenticationToken.class);
        message = mock(TransportMessage.class);
        restRequest = mock(RestRequest.class);
    }

    public void testAuthenticationFailed() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, token, "_action", message);
        }
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, "_action", message);
        }
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, restRequest);
        }
    }

    public void testAuthenticationFailedRest() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, token, restRequest);
        }
    }

    public void testAuthenticationFailedRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, "_realm", token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, "_realm", token, "_action", message);
        }
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationFailed(requestId, "_realm", token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(requestId, "_realm", token, restRequest);
        }
    }

    public void testAnonymousAccess() throws Exception {
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.anonymousAccessDenied(requestId, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).anonymousAccessDenied(requestId, "_action", message);
        }
    }

    public void testAccessGranted() throws Exception {
        Authentication authentication =new Authentication(new User("_username", "r1"), new RealmRef(null, null, null),
                new RealmRef(null, null, null));
        AuthorizationInfo authzInfo =
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { randomAlphaOfLengthBetween(1, 6) });
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.accessGranted(requestId, authentication, "_action", message, authzInfo);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessGranted(requestId, authentication, "_action", message, authzInfo);
        }
    }

    public void testAccessDenied() throws Exception {
        Authentication authentication = new Authentication(new User("_username", "r1"), new RealmRef(null, null, null),
                new RealmRef(null, null, null));
        AuthorizationInfo authzInfo =
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { randomAlphaOfLengthBetween(1, 6) });
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.accessDenied(requestId, authentication, "_action", message, authzInfo);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessDenied(requestId, authentication, "_action", message, authzInfo);
        }
    }

    public void testConnectionGranted() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = randomBoolean() ? SecurityIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.connectionGranted(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionGranted(inetAddress, "client", rule);
        }
    }

    public void testConnectionDenied() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        service.connectionDenied(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionDenied(inetAddress, "client", rule);
        }
    }

    public void testAuthenticationSuccessRest() throws Exception {
        User user = new User("_username", "r1");
        String realm = "_realm";
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationSuccess(requestId, realm, user, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationSuccess(requestId, realm, user, restRequest);
        }
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        User user = new User("_username", "r1");
        String realm = "_realm";
        final String requestId = randomAlphaOfLengthBetween(6, 12);
        service.authenticationSuccess(requestId, realm, user, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationSuccess(requestId, realm, user, "_action", message);
        }
    }
}
