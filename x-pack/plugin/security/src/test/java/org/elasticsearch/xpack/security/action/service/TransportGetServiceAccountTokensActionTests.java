/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.TlsRuntimeCheck;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportGetServiceAccountTokensActionTests extends ESTestCase {

    private TransportGetServiceAccountTokensAction transportGetServiceAccountTokensAction;
    private ServiceAccountService serviceAccountService;

    @Before
    public void init() {
        final Settings settings = Settings.builder()
            .put("node.name", "node_name")
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.transport.ssl.enabled", true)
            .build();
        serviceAccountService = mock(ServiceAccountService.class);
        transportGetServiceAccountTokensAction = new TransportGetServiceAccountTokensAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            settings, serviceAccountService, new TlsRuntimeCheck(settings));
    }

    public void testDoExecuteWillDelegate() {
        final GetServiceAccountTokensRequest request =
            new GetServiceAccountTokensRequest(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        @SuppressWarnings("rawtypes")
        final ActionListener listener = mock(ActionListener.class);
        //noinspection unchecked
        transportGetServiceAccountTokensAction.doExecute(mock(Task.class), request, listener);
        verify(serviceAccountService).findTokensFor(
            eq(new ServiceAccount.ServiceAccountId(request.getNamespace(), request.getServiceName())),
            eq("node_name"), eq(listener));
    }

    public void testTlsRequired() {
        final boolean httpTls = randomBoolean();
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", httpTls)
            .put("xpack.security.transport.ssl.enabled", randomFrom(false == httpTls, false))
            .build();
        final TransportGetServiceAccountTokensAction action = new TransportGetServiceAccountTokensAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()),
            settings, mock(ServiceAccountService.class), new TlsRuntimeCheck(settings));

        final PlainActionFuture<GetServiceAccountTokensResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), mock(GetServiceAccountTokensRequest.class), future);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[get service account tokens] requires TLS for both HTTP and Transport"));
    }
}
