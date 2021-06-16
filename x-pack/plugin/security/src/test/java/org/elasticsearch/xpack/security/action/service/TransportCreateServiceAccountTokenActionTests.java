/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateServiceAccountTokenActionTests extends ESTestCase {

    private IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private SecurityContext securityContext;
    private TransportCreateServiceAccountTokenAction transportCreateServiceAccountTokenAction;
    private Transport transport;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws IOException {
        indexServiceAccountTokenStore = mock(IndexServiceAccountTokenStore.class);
        securityContext = mock(SecurityContext.class);
        final Settings.Builder builder = Settings.builder()
            .put("xpack.security.enabled", true);
        transport = mock(Transport.class);
        final TransportAddress transportAddress;
        if (randomBoolean()) {
            transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        } else {
            transportAddress = new TransportAddress(InetAddress.getLocalHost(), 9300);
        }
        if (randomBoolean()) {
            builder.put("xpack.security.http.ssl.enabled", true);
        } else {
            builder.put("discovery.type", "single-node");
        }
        when(transport.boundAddress()).thenReturn(
            new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));
        transportCreateServiceAccountTokenAction = new TransportCreateServiceAccountTokenAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            indexServiceAccountTokenStore, securityContext, new HttpTlsRuntimeCheck(builder.build(), new SetOnce<>(transport)));
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
        verify(indexServiceAccountTokenStore).createToken(authentication, request, future);
    }

    public void testTlsRequired() {
        Mockito.reset(transport);
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", false)
            .build();
        final TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        when(transport.boundAddress()).thenReturn(
            new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));

        TransportCreateServiceAccountTokenAction action = new TransportCreateServiceAccountTokenAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            indexServiceAccountTokenStore, securityContext, new HttpTlsRuntimeCheck(settings, new SetOnce<>(transport)));

        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), mock(CreateServiceAccountTokenRequest.class), future);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[create service account token] requires TLS for the HTTP interface"));
    }
}
