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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportGetServiceAccountActionTests extends ESTestCase {

    private TransportGetServiceAccountAction transportGetServiceAccountAction;

    @Before
    public void init() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocation -> {
            ActionListener<List<ServiceAccountInfo>> listener = invocation.getArgument(2);
            listener.onResponse(List.of());
            return null;
        }).when(serviceAccountService).getManagedAccountInfos(any(), any(), any());
        transportGetServiceAccountAction = new TransportGetServiceAccountAction(
            transportService,
            ActionFilters.EMPTY,
            serviceAccountService
        );
    }

    public void testDoExecute() {
        final GetServiceAccountRequest request1 = randomFrom(
            new GetServiceAccountRequest(null, null),
            new GetServiceAccountRequest("elastic", null)
        );
        final PlainActionFuture<GetServiceAccountResponse> future1 = new PlainActionFuture<>();
        transportGetServiceAccountAction.doExecute(mock(Task.class), request1, future1);
        final GetServiceAccountResponse getServiceAccountResponse1 = future1.actionGet();
        assertThat(getServiceAccountResponse1.getServiceAccountInfos().length, equalTo(4));
        assertThat(
            Arrays.stream(getServiceAccountResponse1.getServiceAccountInfos()).map(ServiceAccountInfo::getPrincipal).toList(),
            containsInAnyOrder("elastic/auto-ops", "elastic/fleet-server", "elastic/fleet-server-remote", "elastic/kibana")
        );

        final GetServiceAccountRequest request2 = new GetServiceAccountRequest("elastic", "fleet-server");
        final PlainActionFuture<GetServiceAccountResponse> future2 = new PlainActionFuture<>();
        transportGetServiceAccountAction.doExecute(mock(Task.class), request2, future2);
        final GetServiceAccountResponse getServiceAccountResponse2 = future2.actionGet();
        assertThat(getServiceAccountResponse2.getServiceAccountInfos().length, equalTo(1));
        assertThat(getServiceAccountResponse2.getServiceAccountInfos()[0].getPrincipal(), equalTo("elastic/fleet-server"));

        final GetServiceAccountRequest request3 = randomFrom(
            new GetServiceAccountRequest("foo", null),
            new GetServiceAccountRequest("elastic", "foo"),
            new GetServiceAccountRequest("foo", "bar")
        );
        final PlainActionFuture<GetServiceAccountResponse> future3 = new PlainActionFuture<>();
        transportGetServiceAccountAction.doExecute(mock(Task.class), request3, future3);
        final GetServiceAccountResponse getServiceAccountResponse3 = future3.actionGet();
        assertThat(getServiceAccountResponse3.getServiceAccountInfos().length, equalTo(0));
    }

    public void testBuiltInLookupSkipsManagedStoreWhenSecurityIndexUnavailable() {
        ServiceAccountService failingServiceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocation -> {
            ActionListener<List<ServiceAccountInfo>> listener = invocation.getArgument(2);
            listener.onFailure(new ElasticsearchException("security index unavailable"));
            return null;
        }).when(failingServiceAccountService).getManagedAccountInfos(any(), any(), any());
        TransportGetServiceAccountAction action = new TransportGetServiceAccountAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            ActionFilters.EMPTY,
            failingServiceAccountService
        );

        final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), new GetServiceAccountRequest("elastic", "fleet-server"), future);
        assertThat(future.actionGet().getServiceAccountInfos()[0].getPrincipal(), equalTo("elastic/fleet-server"));
        verify(failingServiceAccountService, never()).getManagedAccountInfos(any(), any(), any());
    }

    public void testUnfilteredLookupStillUsesManagedStoreWhenRequested() {
        ServiceAccountService failingServiceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocation -> {
            ActionListener<List<ServiceAccountInfo>> listener = invocation.getArgument(2);
            listener.onFailure(new ElasticsearchException("security index unavailable"));
            return null;
        }).when(failingServiceAccountService).getManagedAccountInfos(any(), any(), any());
        TransportGetServiceAccountAction action = new TransportGetServiceAccountAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            ActionFilters.EMPTY,
            failingServiceAccountService
        );

        final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), new GetServiceAccountRequest(null, null, EnumSet.allOf(ServiceAccountManagedBy.class)), future);
        expectThrows(ElasticsearchException.class, future::actionGet);
        verify(failingServiceAccountService).getManagedAccountInfos(eq(null), eq(null), any());
    }

    public void testUnfilteredLookupSkipsManagedStoreByDefault() {
        ServiceAccountService failingServiceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocation -> {
            ActionListener<List<ServiceAccountInfo>> listener = invocation.getArgument(2);
            listener.onFailure(new ElasticsearchException("security index unavailable"));
            return null;
        }).when(failingServiceAccountService).getManagedAccountInfos(any(), any(), any());
        TransportGetServiceAccountAction action = new TransportGetServiceAccountAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            ActionFilters.EMPTY,
            failingServiceAccountService
        );

        final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), new GetServiceAccountRequest(null, null), future);
        assertThat(future.actionGet().getServiceAccountInfos().length, equalTo(4));
        verify(failingServiceAccountService, never()).getManagedAccountInfos(any(), any(), any());
    }
}
