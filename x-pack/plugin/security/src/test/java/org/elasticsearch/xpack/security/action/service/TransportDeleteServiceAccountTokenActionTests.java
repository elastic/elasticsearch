/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportDeleteServiceAccountTokenActionTests extends ESTestCase {

    private IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private HttpTlsRuntimeCheck httpTlsRuntimeCheck;
    private TransportDeleteServiceAccountTokenAction transportDeleteServiceAccountTokenAction;

    @Before
    public void init() {
        indexServiceAccountTokenStore = mock(IndexServiceAccountTokenStore.class);
        httpTlsRuntimeCheck = mock(HttpTlsRuntimeCheck.class);
        transportDeleteServiceAccountTokenAction = new TransportDeleteServiceAccountTokenAction(
            mock(TransportService.class), new ActionFilters(Collections.emptySet()), indexServiceAccountTokenStore, httpTlsRuntimeCheck);

        doAnswer(invocationOnMock -> {
            final Object[] arguments = invocationOnMock.getArguments();
            ((Runnable) arguments[2]).run();
            return null;
        }).when(httpTlsRuntimeCheck).checkTlsThenExecute(any(), any(), any());
    }

    public void testDoExecuteWillDelegate() {
        final DeleteServiceAccountTokenRequest request = new DeleteServiceAccountTokenRequest(
            randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        @SuppressWarnings("unchecked") final ActionListener<DeleteServiceAccountTokenResponse> listener = mock(ActionListener.class);
        transportDeleteServiceAccountTokenAction.doExecute(mock(Task.class), request, listener);
        verify(indexServiceAccountTokenStore).deleteToken(eq(request), anyActionListener());
    }

}
