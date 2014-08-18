/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.shield.test.ShieldAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;


/**
 *
 */
public class InternalAuthenticationServiceTests extends ElasticsearchTestCase {

    InternalAuthenticationService service;
    TransportMessage message;
    Realm firstRealm;
    Realm secondRealm;
    AuditTrail auditTrail;
    AuthenticationToken token;

    @Before
    public void init() throws Exception {
        token = mock(AuthenticationToken.class);
        message = mock(TransportMessage.class);
        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn("first");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        Realms realms = mock(Realms.class);
        when(realms.realms()).thenReturn(new Realm[] {firstRealm, secondRealm});

        auditTrail = mock(AuditTrail.class);
        service = new InternalAuthenticationService(ImmutableSettings.EMPTY, realms, auditTrail);
    }

    @Test @SuppressWarnings("unchecked")
    public void testToken_FirstMissing_SecondFound() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(token);

        AuthenticationToken result = service.token("_action", message);
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    @Test
    public void testToken_Missing() throws Exception {
        try {
            service.token("_action", message);
            fail("expected authentication exception with missing auth token");
        } catch (AuthenticationException ae) {
            assertThat(ae.getMessage(), equalTo("Missing authentication token for request [_action]"));
            assertContainsWWWAuthenticateHeader(ae);
        }
        verify(auditTrail).anonymousAccess("_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    @Test
    public void testToken_MissingWithDefault() throws Exception {
        AuthenticationToken result = service.token("_action", message, token);
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    @Test @SuppressWarnings("unchecked")
    public void testAuthenticate_BothSupport_SecondSucceeds() throws Exception {
        User user = new User.Simple("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(null); // first fails
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        User result = service.authenticate("_action", message, token);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verify(auditTrail).authenticationFailed("first", token, "_action", message);
    }

    @Test @SuppressWarnings("unchecked")
    public void testAuthenticate_FirstNotSupporting_SecondSucceeds() throws Exception {
        User user = new User.Simple("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        User result = service.authenticate("_action", message, token);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verifyZeroInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(token);
    }

}
