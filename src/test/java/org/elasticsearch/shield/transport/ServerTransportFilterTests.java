/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ServerTransportFilterTests extends ElasticsearchTestCase {

    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private ServerTransportFilter filter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        filter = new ServerTransportFilter.NodeProfile(authcService, authzService);
    }

    @Test
    public void testInbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, null)).thenReturn(user);
        filter.inbound("_action", request);
        verify(authzService).authorize(user, "_action", request);
    }

    @Test
    public void testInbound_AuthenticationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        doThrow(new AuthenticationException("authc failed")).when(authcService).authenticate("_action", request, null);
        try {
            filter.inbound("_action", request);
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyZeroInteractions(authzService);
    }

    @Test
    public void testInbound_AuthorizationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, null)).thenReturn(user);
        doThrow(new AuthorizationException("authz failed")).when(authzService).authorize(user, "_action", request);
        try {
            filter.inbound("_action", request);
            fail("expected filter inbound to throw an authorization exception on authorization error");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), equalTo("authz failed"));
        }
    }

}
