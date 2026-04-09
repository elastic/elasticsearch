/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyCredentials;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCloneApiKeyActionTests extends ESTestCase {

    private TransportCloneApiKeyAction action;
    private ApiKeyService apiKeyService;

    @Before
    public void setupMocks() {
        apiKeyService = mock(ApiKeyService.class);
        TransportService transportService = mock(TransportService.class);
        action = new TransportCloneApiKeyAction(transportService, new ActionFilters(Set.of()), apiKeyService);
    }

    public void testInvalidCredentialFormat() {
        CloneApiKeyRequest request = validRequest();
        when(apiKeyService.parseCredentialsFromApiKeyString(any())).thenThrow(new IllegalArgumentException("invalid ApiKey value"));

        PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("invalid API key credential"));
    }

    public void testNullCredentials() {
        CloneApiKeyRequest request = validRequest();
        when(apiKeyService.parseCredentialsFromApiKeyString(any())).thenReturn(null);

        PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testCloneSuccess() {
        CloneApiKeyRequest request = validRequest();
        ApiKeyCredentials credentials = new ApiKeyCredentials(
            "source-id",
            new SecureString("secret".toCharArray()),
            org.elasticsearch.xpack.core.security.action.apikey.ApiKey.Type.REST
        );
        when(apiKeyService.parseCredentialsFromApiKeyString(any())).thenReturn(credentials);

        CreateApiKeyResponse response = new CreateApiKeyResponse(
            request.getName(),
            "new-id",
            new SecureString("new-secret".toCharArray()),
            null
        );
        doAnswer(inv -> {
            Object[] args = inv.getArguments();
            assertThat(args[0], sameInstance(request));
            assertThat(args[1], instanceOf(ApiKeyCredentials.class));
            @SuppressWarnings("unchecked")
            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[2];
            listener.onResponse(response);
            return null;
        }).when(apiKeyService).cloneApiKey(same(request), any(ApiKeyCredentials.class), any());

        PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verify(apiKeyService).cloneApiKey(same(request), any(ApiKeyCredentials.class), any());
    }

    private static CloneApiKeyRequest validRequest() {
        CloneApiKeyRequest request = new CloneApiKeyRequest();
        request.setApiKey(new SecureString(Base64.getEncoder().encodeToString("id:secret".getBytes(StandardCharsets.UTF_8)).toCharArray()));
        request.setName("cloned-key");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        return request;
    }
}
