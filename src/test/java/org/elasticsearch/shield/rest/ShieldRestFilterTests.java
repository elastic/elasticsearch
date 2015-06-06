/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ShieldRestFilterTests extends ElasticsearchTestCase {

    private AuthenticationService authcService;
    private RestChannel channel;
    private RestFilterChain chain;
    private ShieldRestFilter filter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        RestController restController = mock(RestController.class);
        channel = mock(RestChannel.class);
        chain = mock(RestFilterChain.class);
        filter = new ShieldRestFilter(authcService, restController, Settings.EMPTY);
        verify(restController).registerFilter(filter);
    }

    @Test
    public void testProcess() throws Exception {
        RestRequest request = mock(RestRequest.class);
        User user = new User.Simple("_user", "r1");
        when(authcService.authenticate(request)).thenReturn(user);
        filter.process(request, channel, chain);
        verify(chain).continueProcessing(request, channel);
        verifyZeroInteractions(channel);
    }

    @Test
    public void testProcess_AuthenticationError() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(authcService.authenticate(request)).thenThrow(new AuthenticationException("failed authc"));
        try {
            filter.process(request, channel, chain);
            fail("expected rest filter process to throw an authentication exception when authentication fails");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), equalTo("failed authc"));
        }
        verifyZeroInteractions(channel);
        verifyZeroInteractions(chain);
    }

    @Test
    public void testProcess_OptionsMethod() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(request.method()).thenReturn(RestRequest.Method.OPTIONS);
        filter.process(request, channel, chain);
        verify(chain).continueProcessing(request, channel);
        verifyZeroInteractions(channel);
        verifyZeroInteractions(authcService);
    }
}
