/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportCreateServiceAccountTokenActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private SecurityContext securityContext;
    private TransportCreateServiceAccountTokenAction transportCreateServiceAccountTokenAction;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws IOException {
        serviceAccountService = mock(ServiceAccountService.class);
        securityContext = mock(SecurityContext.class);

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        transportCreateServiceAccountTokenAction = new TransportCreateServiceAccountTokenAction(
            transportService,
            ActionFilters.EMPTY,
            serviceAccountService,
            securityContext
        );
    }

    public void testAuthenticationIsRequired() {
        when(securityContext.getAuthentication()).thenReturn(null);
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportCreateServiceAccountTokenAction.doExecute(mock(Task.class), mock(CreateServiceAccountTokenRequest.class), future);
        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("authentication is required"));
    }

    public void testExecutionWillDelegate() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(
            "elastic",
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportCreateServiceAccountTokenAction.doExecute(mock(Task.class), request, future);
        verify(serviceAccountService).createIndexToken(eq(authentication), eq(request), eq(future));
    }

    public void testRejectsNonElasticNamespace() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(
            randomValueOtherThan("elastic", () -> randomAlphaOfLengthBetween(3, 8)),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportCreateServiceAccountTokenAction.doExecute(mock(Task.class), request, future);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("built-in service account token creation only supports the [elastic] namespace"));
        verifyNoInteractions(serviceAccountService);
    }
}
