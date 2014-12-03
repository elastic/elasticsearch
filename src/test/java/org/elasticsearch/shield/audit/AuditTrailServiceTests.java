/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 */
public class AuditTrailServiceTests extends ElasticsearchTestCase {

    private Set<AuditTrail> auditTrails;
    private AuditTrailService service;

    private AuthenticationToken token;
    private TransportMessage message;
    private RestRequest restRequest;

    @Before
    public void init() throws Exception {
        ImmutableSet.Builder<AuditTrail> builder = ImmutableSet.builder();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            builder.add(mock(AuditTrail.class));
        }
        auditTrails = builder.build();
        service = new AuditTrailService(ImmutableSettings.EMPTY, auditTrails);
        token = mock(AuthenticationToken.class);
        message = mock(TransportMessage.class);
        restRequest = mock(RestRequest.class);
    }

    @Test
    public void testAuthenticationFailed() throws Exception {
        service.authenticationFailed(token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    @Test
    public void testAuthenticationFailed_Rest() throws Exception {
        service.authenticationFailed(token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    @Test
    public void testAuthenticationFailed_Realm() throws Exception {
        service.authenticationFailed("_realm", token, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed("_realm", token, "_action", message);
        }
    }

    @Test
    public void testAuthenticationFailed_Rest_Realm() throws Exception {
        service.authenticationFailed("_realm", token, restRequest);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).authenticationFailed("_realm", token, restRequest);
        }
    }

    @Test
    public void testAnonymousAccess() throws Exception {
        service.anonymousAccess("_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).anonymousAccess("_action", message);
        }
    }

    @Test
    public void testAccessGranted() throws Exception {
        User user = new User.Simple("_username", "r1");
        service.accessGranted(user, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessGranted(user, "_action", message);
        }
    }

    @Test
    public void testAccessDenied() throws Exception {
        User user = new User.Simple("_username", "r1");
        service.accessDenied(user, "_action", message);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).accessDenied(user, "_action", message);
        }
    }

    @Test
    public void testConnectionGranted() throws Exception {
        InetAddress inetAddress = InetAddress.getLocalHost();
        ShieldIpFilterRule rule = randomBoolean() ? ShieldIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.connectionGranted(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionGranted(inetAddress, "client", rule);
        }
    }

    @Test
    public void testConnectionDenied() throws Exception {
        InetAddress inetAddress = InetAddress.getLocalHost();
        ShieldIpFilterRule rule = new ShieldIpFilterRule(false, "_all");
        service.connectionDenied(inetAddress, "client", rule);
        for (AuditTrail auditTrail : auditTrails) {
            verify(auditTrail).connectionDenied(inetAddress, "client", rule);
        }
    }
}
