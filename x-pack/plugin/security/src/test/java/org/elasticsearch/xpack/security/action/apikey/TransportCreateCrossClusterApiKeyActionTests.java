/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequestTests.randomCrossClusterApiKeyAccessField;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportCreateCrossClusterApiKeyActionTests extends ESTestCase {

    private ApiKeyService apiKeyService;
    private SecurityContext securityContext;
    private TransportCreateCrossClusterApiKeyAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        apiKeyService = mock(ApiKeyService.class);
        securityContext = mock(SecurityContext.class);
        action = new TransportCreateCrossClusterApiKeyAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            apiKeyService,
            securityContext
        );
    }

    public void testApiKeyWillBeCreatedWithEmptyUserRoleDescriptors() throws IOException {
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder().build()
        );
        when(securityContext.getAuthentication()).thenReturn(authentication);

        final var request = CreateCrossClusterApiKeyRequest.withNameAndAccess(
            randomAlphaOfLengthBetween(3, 8),
            randomCrossClusterApiKeyAccessField()
        );
        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        verify(apiKeyService).createApiKey(same(authentication), same(request), eq(Set.of()), same(future));
    }

    public void testAuthenticationIsRequired() throws IOException {
        final var request = CreateCrossClusterApiKeyRequest.withNameAndAccess(
            randomAlphaOfLengthBetween(3, 8),
            randomCrossClusterApiKeyAccessField()
        );
        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("authentication is required"));
        verifyNoInteractions(apiKeyService);
    }

    public void testCannotCreateDerivedCrossClusterApiKey() throws IOException {
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);

        final var request = CreateCrossClusterApiKeyRequest.withNameAndAccess(
            randomAlphaOfLengthBetween(3, 8),
            randomCrossClusterApiKeyAccessField()
        );
        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString("authentication via API key not supported: An API key cannot be used to create a cross-cluster API key")
        );
        verifyNoInteractions(apiKeyService);
    }
}
