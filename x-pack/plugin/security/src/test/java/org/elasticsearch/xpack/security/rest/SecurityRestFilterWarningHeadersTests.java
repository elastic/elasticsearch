/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityRestFilterWarningHeadersTests extends ESTestCase {
    private ThreadContext threadContext;
    private AuthenticationService authcService;
    private SecondaryAuthenticator secondaryAuthenticator;
    private RestHandler restHandler;

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        restHandler = mock(RestHandler.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        secondaryAuthenticator = new SecondaryAuthenticator(Settings.EMPTY, threadContext, authcService);
    }

    public void testResponseHeadersOnFailure() throws Exception {
        MapBuilder<String, List<String>> headers = new MapBuilder<>();
        headers.put("Warning", Collections.singletonList("Some warning header"));
        headers.put("X-elastic-product", Collections.singletonList("Some product header"));
        Map<String, List<String>> afterHeaders;

        // Remove all the headers on authentication failures
        afterHeaders = testProcessAuthenticationFailed(RestStatus.BAD_REQUEST, headers);
        assertEquals(afterHeaders.size(), 0);
        afterHeaders = testProcessAuthenticationFailed(RestStatus.INTERNAL_SERVER_ERROR, headers);
        assertEquals(afterHeaders.size(), 0);
        afterHeaders = testProcessAuthenticationFailed(RestStatus.UNAUTHORIZED, headers);
        assertEquals(afterHeaders.size(), 0);
        afterHeaders = testProcessAuthenticationFailed(RestStatus.FORBIDDEN, headers);
        assertEquals(afterHeaders.size(), 0);

        // On rest handling failures only remove headers if rest status is UNAUTHORIZED or FORBIDDEN
        afterHeaders = testProcessRestHandlingFailed(RestStatus.BAD_REQUEST, headers);
        assertEquals(afterHeaders.size(), 2);
        afterHeaders = testProcessRestHandlingFailed(RestStatus.INTERNAL_SERVER_ERROR, headers);
        assertEquals(afterHeaders.size(), 2);
        afterHeaders = testProcessRestHandlingFailed(RestStatus.UNAUTHORIZED, headers);
        assertEquals(afterHeaders.size(), 0);
        afterHeaders = testProcessRestHandlingFailed(RestStatus.FORBIDDEN, headers);
        assertEquals(afterHeaders.size(), 0);
    }

    private Map<String, List<String>> testProcessRestHandlingFailed(RestStatus restStatus, MapBuilder<String, List<String>> headers)
        throws Exception {
        RestChannel channel = mock(RestChannel.class);
        SecurityRestFilter filter = new SecurityRestFilter(
            Settings.EMPTY,
            threadContext,
            authcService,
            secondaryAuthenticator,
            restHandler,
            false
        );
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        Authentication primaryAuthentication = mock(Authentication.class);
        when(primaryAuthentication.encode()).thenReturn(randomAlphaOfLengthBetween(12, 36));
        doAnswer(i -> {
            final Object[] arguments = i.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) arguments[arguments.length - 1];
            callback.onResponse(primaryAuthentication);
            return null;
        }).when(authcService).authenticate(eq(request), anyActionListener());
        Authentication secondaryAuthentication = mock(Authentication.class);
        when(secondaryAuthentication.encode()).thenReturn(randomAlphaOfLengthBetween(12, 36));
        doAnswer(i -> {
            final Object[] arguments = i.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) arguments[arguments.length - 1];
            callback.onResponse(secondaryAuthentication);
            return null;
        }).when(authcService).authenticate(eq(request), eq(false), anyActionListener());
        doThrow(new ElasticsearchStatusException("Rest handling failed", restStatus, "")).when(restHandler)
            .handleRequest(request, channel, null);
        when(channel.request()).thenReturn(request);
        when(channel.newErrorBuilder()).thenReturn(JsonXContent.contentBuilder());
        filter.handleRequest(request, channel, null);
        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(channel).sendResponse(response.capture());
        RestResponse restResponse = response.getValue();
        return restResponse.filterHeaders(headers.immutableMap());
    }

    private Map<String, List<String>> testProcessAuthenticationFailed(RestStatus restStatus, MapBuilder<String, List<String>> headers)
        throws Exception {
        RestChannel channel = mock(RestChannel.class);
        SecurityRestFilter filter = new SecurityRestFilter(
            Settings.EMPTY,
            threadContext,
            authcService,
            secondaryAuthenticator,
            restHandler,
            false
        );
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        doAnswer((i) -> {
            ActionListener<?> callback = (ActionListener<?>) i.getArguments()[1];
            callback.onFailure(new ElasticsearchStatusException("Authentication failed", restStatus, ""));
            return Void.TYPE;
        }).when(authcService).authenticate(eq(request), anyActionListener());
        when(channel.request()).thenReturn(request);
        when(channel.newErrorBuilder()).thenReturn(JsonXContent.contentBuilder());
        filter.handleRequest(request, channel, null);
        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(channel).sendResponse(response.capture());
        RestResponse restResponse = response.getValue();
        return restResponse.filterHeaders(headers.immutableMap());
    }
}
