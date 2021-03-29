/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountsTokenStore;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateServiceAccountTokenActionTests extends ESTestCase {

    private IndexServiceAccountsTokenStore indexServiceAccountsTokenStore;
    private SecurityContext securityContext;
    private TransportCreateServiceAccountTokenAction transportCreateServiceAccountTokenAction;

    @Before
    public void init() {
        indexServiceAccountsTokenStore = mock(IndexServiceAccountsTokenStore.class);
        securityContext = mock(SecurityContext.class);
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .build();
        transportCreateServiceAccountTokenAction = new TransportCreateServiceAccountTokenAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            indexServiceAccountsTokenStore, securityContext, new HttpTlsRuntimeCheck(settings));
    }

    public void testAuthenticationIsRequired() {
        when(securityContext.getAuthentication()).thenReturn(null);
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportCreateServiceAccountTokenAction.doExecute(mock(Task.class), mock(CreateServiceAccountTokenRequest.class), future);
        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("authentication is required"));
    }

    public void testExecutionWillDelegate() {
        final Authentication authentication = mock(Authentication.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        final CreateServiceAccountTokenRequest request = mock(CreateServiceAccountTokenRequest.class);
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportCreateServiceAccountTokenAction.doExecute(mock(Task.class), request, future);
        verify(indexServiceAccountsTokenStore).createToken(authentication, request, future);
    }

    public void testTlsRequired() {
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", false)
            .build();
        TransportCreateServiceAccountTokenAction action = new TransportCreateServiceAccountTokenAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            indexServiceAccountsTokenStore, securityContext, new HttpTlsRuntimeCheck(settings));

        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), mock(CreateServiceAccountTokenRequest.class), future);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[create service account token] requires TLS for the HTTP interface"));
    }
}
