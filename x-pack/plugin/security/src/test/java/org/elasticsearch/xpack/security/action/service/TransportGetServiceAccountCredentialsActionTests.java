/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;
import org.junit.Before;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetServiceAccountCredentialsActionTests extends ESTestCase {

    private TransportGetServiceAccountCredentialsAction transportGetServiceAccountCredentialsAction;
    private ServiceAccountService serviceAccountService;
    private Transport transport;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws UnknownHostException {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", "node_name")
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
        final Settings settings = builder.build();
        serviceAccountService = mock(ServiceAccountService.class);
        transportGetServiceAccountCredentialsAction = new TransportGetServiceAccountCredentialsAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            settings, serviceAccountService, new HttpTlsRuntimeCheck(settings, new SetOnce<>(transport)));
    }

    public void testDoExecuteWillDelegate() {
        final GetServiceAccountCredentialsRequest request =
            new GetServiceAccountCredentialsRequest(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        @SuppressWarnings("unchecked")
        final ActionListener<GetServiceAccountCredentialsResponse> listener = mock(ActionListener.class);
        transportGetServiceAccountCredentialsAction.doExecute(mock(Task.class), request, listener);
        verify(serviceAccountService).findTokensFor(
            eq(new ServiceAccount.ServiceAccountId(request.getNamespace(), request.getServiceName())),
            eq("node_name"), eq(listener));
    }

    public void testTlsRequired() {
        Mockito.reset(transport);
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", false)
            .build();
        final TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        when(transport.boundAddress()).thenReturn(
            new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));
        final TransportGetServiceAccountCredentialsAction action = new TransportGetServiceAccountCredentialsAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            settings, mock(ServiceAccountService.class), new HttpTlsRuntimeCheck(settings, new SetOnce<>(transport)));

        final PlainActionFuture<GetServiceAccountCredentialsResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), mock(GetServiceAccountCredentialsRequest.class), future);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[get service account tokens] requires TLS for the HTTP interface"));
    }
}
