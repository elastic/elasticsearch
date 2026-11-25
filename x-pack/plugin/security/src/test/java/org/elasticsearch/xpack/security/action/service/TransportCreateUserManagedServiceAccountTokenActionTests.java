/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
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

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportCreateUserManagedServiceAccountTokenActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private SecurityContext securityContext;
    private TransportCreateUserManagedServiceAccountTokenAction action;

    @Before
    public void init() {
        serviceAccountService = mock(ServiceAccountService.class);
        securityContext = mock(SecurityContext.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        action = new TransportCreateUserManagedServiceAccountTokenAction(
            transportService,
            ActionFilters.EMPTY,
            serviceAccountService,
            securityContext
        );
    }

    public void testAuthenticationIsRequired() {
        when(securityContext.getAuthentication()).thenReturn(null);
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), newRequest("engineering"), future);
        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("authentication is required"));
        verifyNoInteractions(serviceAccountService);
    }

    public void testExecutionWillDelegateToTheUserManagedPathForAnyNamespace() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        for (String namespace : List.of("elastic", "engineering")) {
            final CreateServiceAccountTokenRequest request = newRequest(namespace);
            final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
            action.doExecute(mock(Task.class), request, future);
            verify(serviceAccountService).createUserManagedToken(authentication, request, future);
        }
        verify(serviceAccountService, never()).createBuiltInToken(any(), any(), any());
    }

    private static CreateServiceAccountTokenRequest newRequest(String namespace) {
        return new CreateServiceAccountTokenRequest(namespace, randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
    }
}
