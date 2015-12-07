/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 */
public class AuditTrailServiceTests extends ESTestCase {
    private Set<AuditTrail> auditTrails;
    private AuditTrailService service;

    private AuthenticationToken token;
    private TransportMessage message;
    private RestRequest restRequest;

    @Before
    public void init() throws Exception {
        Set<AuditTrail> auditTrailsBuilder = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            auditTrailsBuilder.add(mock(AuditTrail.class));
        }
        auditTrails = unmodifiableSet(auditTrailsBuilder);
        service = new AuditTrailService(Settings.EMPTY, auditTrails);
        token = mock(AuthenticationToken.class);
        message = mock(TransportMessage.class);
        restRequest = mock(RestRequest.class);
    }

    public void testAuthenticationFailed() throws Exception {
        service.authenticationFailed(token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        service.authenticationFailed("_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed("_action", message);
        }
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        service.authenticationFailed(restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(restRequest);
        }
    }

    public void testAuthenticationFailedRest() throws Exception {
        service.authenticationFailed(token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testAuthenticationFailedRealm() throws Exception {
        service.authenticationFailed("_realm", token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed("_realm", token, "_action", message);
        }
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        service.authenticationFailed("_realm", token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed("_realm", token, restRequest);
        }
    }

    public void testAnonymousAccess() throws Exception {
        service.anonymousAccessDenied("_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).anonymousAccessDenied("_action", message);
        }
    }

    public void testAccessGranted() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
        service.accessGranted(user, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessGranted(user, "_action", message);
        }
    }

    public void testAccessDenied() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
        service.accessDenied(user, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessDenied(user, "_action", message);
        }
    }

    public void testConnectionGranted() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        ShieldIpFilterRule rule = randomBoolean() ? ShieldIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.connectionGranted(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionGranted(inetAddress, "client", rule);
        }
    }

    public void testConnectionDenied() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        ShieldIpFilterRule rule = new ShieldIpFilterRule(false, "_all");
        service.connectionDenied(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionDenied(inetAddress, "client", rule);
        }
    }
}
