/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class TransportDeleteManagedServiceAccountTokenActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportDeleteManagedServiceAccountTokenAction transportDeleteManagedServiceAccountTokenAction;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws IOException {
        serviceAccountService = mock(ServiceAccountService.class);

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        transportDeleteManagedServiceAccountTokenAction = new TransportDeleteManagedServiceAccountTokenAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            serviceAccountService
        );
    }

    public void testRejectsElasticNamespace() {
        final DeleteServiceAccountTokenRequest request = new DeleteServiceAccountTokenRequest("elastic", "fleet-server", "token-1");
        final PlainActionFuture<DeleteServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportDeleteManagedServiceAccountTokenAction.doExecute(mock(Task.class), request, future);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("the [elastic] namespace is reserved for built-in service accounts"));
        verifyNoInteractions(serviceAccountService);
    }

    public void testExecutionWillDelegate() {
        final DeleteServiceAccountTokenRequest request = new DeleteServiceAccountTokenRequest("my-team", "worker", "token-1");
        final PlainActionFuture<DeleteServiceAccountTokenResponse> future = new PlainActionFuture<>();
        transportDeleteManagedServiceAccountTokenAction.doExecute(mock(Task.class), request, future);
        verify(serviceAccountService).deleteIndexToken(eq(request), any());
    }
}
