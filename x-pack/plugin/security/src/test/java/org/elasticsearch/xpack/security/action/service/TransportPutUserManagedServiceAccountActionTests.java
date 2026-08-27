/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.PutResult;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportPutUserManagedServiceAccountActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportPutUserManagedServiceAccountAction action;

    @Before
    public void init() {
        serviceAccountService = mock(ServiceAccountService.class);
        stubPutResult(randomFrom(PutResult.values()));
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        action = new TransportPutUserManagedServiceAccountAction(transportService, ActionFilters.EMPTY, serviceAccountService);
    }

    public void testTheRequestIsUnpackedForTheService() {
        final List<String> roles = randomList(1, 3, () -> randomAlphaOfLengthBetween(3, 8));
        final boolean enabled = randomBoolean();
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            "engineering",
            "deploy_bot",
            roles,
            enabled
        );
        request.setRefreshPolicy(refreshPolicy);

        action.doExecute(mock(Task.class), request, new PlainActionFuture<>());

        verify(serviceAccountService).putUserManagedAccount(
            eq(new ServiceAccountId("engineering", "deploy_bot")),
            eq(roles),
            eq(enabled),
            eq(refreshPolicy),
            any()
        );
    }

    public void testWhetherTheAccountWasCreatedOrReplacedIsReported() {
        for (PutResult result : PutResult.values()) {
            stubPutResult(result);
            final PlainActionFuture<PutUserManagedServiceAccountResponse> future = new PlainActionFuture<>();
            action.doExecute(mock(Task.class), newRequest(), future);
            assertThat(future.actionGet().created(), is(result == PutResult.CREATED));
        }
    }

    private static PutUserManagedServiceAccountRequest newRequest() {
        return new PutUserManagedServiceAccountRequest("engineering", "deploy_bot", List.of("deployer"), randomBoolean());
    }

    private void stubPutResult(PutResult result) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<PutResult> listener = (ActionListener<PutResult>) invocation.getArguments()[4];
            listener.onResponse(result);
            return null;
        }).when(serviceAccountService).putUserManagedAccount(any(), any(), anyBoolean(), any(), any());
    }
}
