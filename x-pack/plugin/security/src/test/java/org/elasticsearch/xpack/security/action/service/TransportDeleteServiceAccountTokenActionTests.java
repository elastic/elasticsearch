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
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteServiceAccountTokenActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportDeleteServiceAccountTokenAction transportDeleteServiceAccountTokenAction;

    @Before
    public void init() {
        serviceAccountService = mock(ServiceAccountService.class);
        transportDeleteServiceAccountTokenAction = new TransportDeleteServiceAccountTokenAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            serviceAccountService
        );
    }

    public void testDoExecuteWillDelegate() {
        final DeleteServiceAccountTokenRequest request = new DeleteServiceAccountTokenRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        @SuppressWarnings("unchecked")
        final ActionListener<DeleteServiceAccountTokenResponse> listener = mock(ActionListener.class);
        when(listener.delegateFailureAndWrap(any())).thenCallRealMethod();
        transportDeleteServiceAccountTokenAction.doExecute(mock(Task.class), request, listener);
        verify(serviceAccountService).deleteIndexToken(eq(request), anyActionListener());
    }
}
