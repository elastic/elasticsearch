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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportDeleteUserManagedServiceAccountTokenActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportDeleteUserManagedServiceAccountTokenAction action;

    @Before
    public void init() {
        serviceAccountService = mock(ServiceAccountService.class);
        stubDeleteResult(randomBoolean());
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        action = new TransportDeleteUserManagedServiceAccountTokenAction(transportService, ActionFilters.EMPTY, serviceAccountService);
    }

    public void testExecutionWillDelegateToTheUserManagedPathForAnyNamespace() {
        for (String namespace : List.of("elastic", "engineering")) {
            final DeleteServiceAccountTokenRequest request = newRequest(namespace);
            action.doExecute(mock(Task.class), request, new PlainActionFuture<>());
            verify(serviceAccountService).deleteUserManagedToken(eq(request), any());
        }
        verify(serviceAccountService, never()).deleteBuiltInToken(any(), any());
    }

    public void testWhetherATokenWasDeletedIsReported() {
        for (boolean found : List.of(true, false)) {
            stubDeleteResult(found);
            final PlainActionFuture<DeleteServiceAccountTokenResponse> future = new PlainActionFuture<>();
            action.doExecute(mock(Task.class), newRequest("engineering"), future);
            assertThat(future.actionGet().found(), is(found));
        }
    }

    private static DeleteServiceAccountTokenRequest newRequest(String namespace) {
        return new DeleteServiceAccountTokenRequest(namespace, randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
    }

    private void stubDeleteResult(boolean found) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(found);
            return null;
        }).when(serviceAccountService).deleteUserManagedToken(any(), any());
    }
}
