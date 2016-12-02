/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.ssl.SSLService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityRestFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private RestChannel channel;
    private SecurityRestFilter filter;
    private XPackLicenseState licenseState;
    private RestHandler restHandler;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        channel = mock(RestChannel.class);
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        restHandler = mock(RestHandler.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        filter = new SecurityRestFilter(Settings.EMPTY, licenseState, mock(SSLService.class),
            threadPool.getThreadContext(), authcService, restHandler);
    }

    public void testProcess() throws Exception {
        RestRequest request = mock(RestRequest.class);
        Authentication authentication = mock(Authentication.class);
        doAnswer((i) -> {
            ActionListener callback =
                (ActionListener) i.getArguments()[1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(request), any(ActionListener.class));
        filter.handleRequest(request, channel, null);
        verify(restHandler).handleRequest(request, channel, null);
        verifyZeroInteractions(channel);
    }

    public void testProcessBasicLicense() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(licenseState.isAuthAllowed()).thenReturn(false);
        filter.handleRequest(request, channel, null);
        verifyZeroInteractions(restHandler);
        verifyZeroInteractions(channel, authcService);
    }

    public void testProcessAuthenticationError() throws Exception {
        RestRequest request = mock(RestRequest.class);
        Exception exception = authenticationError("failed authc");
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            callback.onFailure(exception);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(request), any(ActionListener.class));
        when(channel.request()).thenReturn(request);
        when(channel.newErrorBuilder()).thenReturn(JsonXContent.contentBuilder());
        filter.handleRequest(request, channel, null);
        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(channel).sendResponse(response.capture());
        assertEquals(RestStatus.UNAUTHORIZED, response.getValue().status());
        verifyZeroInteractions(restHandler);
    }

    public void testProcessOptionsMethod() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(request.method()).thenReturn(RestRequest.Method.OPTIONS);
        filter.handleRequest(request, channel, null);
        verifyZeroInteractions(restHandler);
        verifyZeroInteractions(channel);
        verifyZeroInteractions(authcService);
    }
}
