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
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportDeleteUserManagedServiceAccountActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportDeleteUserManagedServiceAccountAction action;

    @Before
    public void init() {
        serviceAccountService = mock(ServiceAccountService.class);
        stubDeleteResult(randomBoolean());
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        action = new TransportDeleteUserManagedServiceAccountAction(transportService, ActionFilters.EMPTY, serviceAccountService);
    }

    public void testTheRequestIsUnpackedForTheService() {
        for (boolean force : List.of(true, false)) {
            final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
            final DeleteUserManagedServiceAccountRequest request = new DeleteUserManagedServiceAccountRequest("engineering", "deploy_bot");
            request.setForce(force);
            request.setRefreshPolicy(refreshPolicy);

            action.doExecute(mock(Task.class), request, new PlainActionFuture<>());

            verify(serviceAccountService).deleteUserManagedAccount(
                eq(new ServiceAccountId("engineering", "deploy_bot")),
                eq(force),
                eq(refreshPolicy),
                any()
            );
        }
    }

    public void testWhetherAnAccountWasDeletedIsReported() {
        for (boolean found : List.of(true, false)) {
            stubDeleteResult(found);
            final PlainActionFuture<DeleteUserManagedServiceAccountResponse> future = new PlainActionFuture<>();
            action.doExecute(mock(Task.class), new DeleteUserManagedServiceAccountRequest("engineering", "deploy_bot"), future);
            assertThat(future.actionGet().found(), is(found));
        }
    }

    private void stubDeleteResult(boolean found) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[3];
            listener.onResponse(found);
            return null;
        }).when(serviceAccountService).deleteUserManagedAccount(any(), anyBoolean(), any(), any());
    }
}
