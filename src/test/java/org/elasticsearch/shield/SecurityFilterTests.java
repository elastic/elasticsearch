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
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;
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
    private RestController restController;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        restController = mock(RestController.class);
        filter = new SecurityFilter(ImmutableSettings.EMPTY, authcService, authzService, restController);
    }

    @Test
    public void testProcess() throws Exception {
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User.Simple("_username", "r1");
        when(authcService.token("_action", request, SystemRealm.TOKEN)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenReturn(user);
        filter.process("_action", request);
        verify(authzService).authorize(user, "_action", request);
    }

    @Test
    public void testProcess_AuthenticationFails_Authenticate() throws Exception {
        thrown.expect(AuthenticationException.class);
        thrown.expectMessage("failed authc");
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(authcService.token("_action", request, SystemRealm.TOKEN)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenThrow(new AuthenticationException("failed authc"));
        filter.process("_action", request);
    }

    @Test
    public void testProcess_AuthenticationFails_NoToken() throws Exception {
        thrown.expect(AuthenticationException.class);
        thrown.expectMessage("failed authc");
        TransportRequest request = new InternalRequest();
        when(authcService.token("_action", request, SystemRealm.TOKEN)).thenThrow(new AuthenticationException("failed authc"));
        filter.process("_action", request);
    }

    @Test
    public void testProcess_AuthorizationFails() throws Exception {
        thrown.expect(AuthorizationException.class);
        thrown.expectMessage("failed authz");
        TransportRequest request = new InternalRequest();
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User.Simple("_username", "r1");
        when(authcService.token("_action", request, SystemRealm.TOKEN)).thenReturn(token);
        when(authcService.authenticate("_action", request, token)).thenReturn(user);
        doThrow(new AuthorizationException("failed authz")).when(authzService).authorize(user, "_action", request);
        filter.process("_action", request);
    }

    @Test
    public void testTransport_InboundRequest() throws Exception {
        filter = mock(SecurityFilter.class);
        SecurityFilter.Transport transport = new SecurityFilter.Transport(filter);
        InternalRequest request = new InternalRequest();
        transport.inboundRequest("_action", request);
        verify(filter).process("_action", request);
    }

    @Test
    public void testTransport_InboundRequest_Exception() throws Exception {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("process-error");
        filter = mock(SecurityFilter.class);
        SecurityFilter.Transport transport = new SecurityFilter.Transport(filter);
        InternalRequest request = new InternalRequest();
        doThrow(new RuntimeException("process-error")).when(filter).process("_action", request);
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
        verify(filter).process("_action", request);
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
        doThrow(exception).when(filter).process("_action", request);
        action.process("_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    @Test
    public void testRest_WithToken() throws Exception {
        SecurityFilter.Rest rest = new SecurityFilter.Rest(filter);
        RestRequest request = mock(RestRequest.class);
        RestChannel channel = mock(RestChannel.class);
        RestFilterChain chain = mock(RestFilterChain.class);
        rest.process(request, channel, chain);
        verify(authcService).verifyToken(request);
    }

    @Test
    public void testRest_WithoutToken() throws Exception {
        AuthenticationException exception = new AuthenticationException("no token");
        thrown.expect(AuthenticationException.class);
        thrown.expectMessage("no token");
        SecurityFilter.Rest rest = new SecurityFilter.Rest(filter);
        RestRequest request = mock(RestRequest.class);
        RestChannel channel = mock(RestChannel.class);
        RestFilterChain chain = mock(RestFilterChain.class);
        doThrow(exception).when(authcService).verifyToken(request);
        rest.process(request, channel, chain);
    }

    private static class InternalRequest extends TransportRequest {
    }
}
