/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.action.ShieldActionMapper;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty.NettyTransportChannel;
import org.junit.Before;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;
import static org.elasticsearch.shield.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ServerTransportFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private ServerTransportFilter filter;
    private NettyTransportChannel channel;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        channel = mock(NettyTransportChannel.class);
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
        filter = new ServerTransportFilter.NodeProfile(authcService, authzService, new ShieldActionMapper(),
                new ThreadContext(Settings.EMPTY), false);
    }

    public void testInbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, null)).thenReturn(user);
        filter.inbound("_action", request, channel);
        verify(authzService).authorize(user, "_action", request);
    }

    public void testInboundAuthenticationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        doThrow(authenticationError("authc failed")).when(authcService).authenticate("_action", request, null);
        try {
            filter.inbound("_action", request, channel);
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyZeroInteractions(authzService);
    }

    public void testInboundAuthorizationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, null)).thenReturn(user);
        doThrow(authorizationError("authz failed")).when(authzService).authorize(user, "_action", request);
        try {
            filter.inbound("_action", request, channel);
            fail("expected filter inbound to throw an authorization exception on authorization error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authz failed"));
        }
    }
}
