/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.system.SystemRealm;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Mockito.*;


/**
 *
 */
public class SecurityFilterTests extends ElasticsearchTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private SecurityFilter filter;
    private AuthenticationService authcService;
    private AuthorizationService authzService;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        filter = new SecurityFilter(ImmutableSettings.EMPTY, authcService, authzService);
    }

    @Test
    public void testProcess_WithoutDefaultToken() throws Exception {
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User.Simple("_username", "r1");
        when(authcService.token("_action", request, null)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenReturn(user);
        filter.process("_action", request, null);
        verify(authzService).authorize(user, "_action", request);
    }

    @Test
    public void testProcess_WithDefaultToken() throws Exception {
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        AuthenticationToken defaultToken = mock(AuthenticationToken.class);
        User user = new User.Simple("_username", "r1");
        when(authcService.token("_action", request, defaultToken)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenReturn(user);
        filter.process("_action", request, defaultToken);
        verify(authzService).authorize(user, "_action", request);
    }

    @Test
    public void testProcess_AuthenticationFails_Authenticate() throws Exception {
        thrown.expect(AuthenticationException.class);
        thrown.expectMessage("failed authc");
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(authcService.token("_action", request, null)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenThrow(new AuthenticationException("failed authc"));
        filter.process("_action", request, null);
    }

    @Test
    public void testProcess_AuthenticationFails_NoToken() throws Exception {
        thrown.expect(AuthenticationException.class);
        thrown.expectMessage("failed authc");
        TransportRequest request = new InternalRequest();
        when(authcService.token("_action", request, null)).thenThrow(new AuthenticationException("failed authc"));
        filter.process("_action", request, null);
    }

    @Test
    public void testProcess_AuthorizationFails() throws Exception {
        thrown.expect(AuthorizationException.class);
        thrown.expectMessage("failed authz");
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User.Simple("_username", "r1");
        when(authcService.token("_action", request, null)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenReturn(user);
        doThrow(new AuthorizationException("failed authz")).when(authzService).authorize(user, "_action", request);
        filter.process("_action", request, null);
    }

    @Test
    public void testTransport_InboundRequest() throws Exception {
        filter = mock(SecurityFilter.class);
        SecurityFilter.Transport transport = new SecurityFilter.Transport(filter);
        InternalRequest request = new InternalRequest();
        transport.inboundRequest("_action", request);
        verify(filter).process("_action", request, SystemRealm.TOKEN);
    }

    @Test
    public void testTransport_InboundRequest_Exception() throws Exception {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("process-error");
        filter = mock(SecurityFilter.class);
        SecurityFilter.Transport transport = new SecurityFilter.Transport(filter);
        InternalRequest request = new InternalRequest();
        doThrow(new RuntimeException("process-error")).when(filter).process("_action", request, SystemRealm.TOKEN);
        transport.inboundRequest("_action", request);
    }

    @Test
    public void testAction_Process() throws Exception {
        filter = mock(SecurityFilter.class);
        SecurityFilter.Action action = new SecurityFilter.Action(filter);
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        action.process("_action", request, listener, chain);
        verify(filter).process("_action", request, null);
        verify(chain).continueProcessing("_action", request, listener);
    }

    @Test
    public void testAction_Process_Exception() throws Exception {
        filter = mock(SecurityFilter.class);
        SecurityFilter.Action action = new SecurityFilter.Action(filter);
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        RuntimeException exception = new RuntimeException("process-error");
        doThrow(exception).when(filter).process("_action", request, null);
        action.process("_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    private static class InternalRequest extends TransportRequest {
    }
}
