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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportGetServiceAccountActionTests extends ESTestCase {

    private static final List<String> ALL_BUILT_IN_PRINCIPALS = List.of(
        "elastic/auto-ops",
        "elastic/fleet-server",
        "elastic/fleet-server-remote",
        "elastic/kibana"
    );

    private ServiceAccountService serviceAccountService;
    private TransportGetServiceAccountAction transportGetServiceAccountAction;

    @Before
    public void init() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        serviceAccountService = mock(ServiceAccountService.class);
        stubUserManagedAccounts(List.of());
        transportGetServiceAccountAction = new TransportGetServiceAccountAction(
            transportService,
            ActionFilters.EMPTY,
            serviceAccountService
        );
    }

    public void testTheBuiltInAccountsAreReported() {
        assertThat(principalsFor(new GetServiceAccountRequest(null, null)), equalTo(ALL_BUILT_IN_PRINCIPALS));
        assertThat(principalsFor(new GetServiceAccountRequest("elastic", null)), equalTo(ALL_BUILT_IN_PRINCIPALS));
        assertThat(principalsFor(new GetServiceAccountRequest("elastic", "fleet-server")), contains("elastic/fleet-server"));
    }

    public void testANameNoBuiltInAccountCarriesMatchesNothing() {
        assertThat(principalsFor(new GetServiceAccountRequest("foo", null)), equalTo(List.of()));
        assertThat(principalsFor(new GetServiceAccountRequest("elastic", "foo")), equalTo(List.of()));
        assertThat(principalsFor(new GetServiceAccountRequest("foo", "bar")), equalTo(List.of()));
    }

    public void testTheAccountStoreIsNotConsultedUnlessUserManagedAccountsAreAsked() {
        stubUserManagedAccountsFailure();
        assertThat(principalsFor(new GetServiceAccountRequest(null, null)), equalTo(ALL_BUILT_IN_PRINCIPALS));
        assertThat(principalsFor(elasticOnly()), equalTo(ALL_BUILT_IN_PRINCIPALS));
        verify(serviceAccountService, never()).getUserManagedAccountInfos(any(), any(), any());
    }

    public void testBothKindsAreReportedTogether() {
        stubUserManagedAccounts(
            List.of(
                new ServiceAccountInfo.UserManaged("engineering/deploy_bot", List.of("deployer"), true),
                new ServiceAccountInfo.UserManaged("aaa/first", List.of("reader"), false)
            )
        );
        final List<ServiceAccountInfo> infos = infosFor(bothKinds());
        assertThat(
            infos.stream().map(ServiceAccountInfo::principal).toList(),
            equalTo(
                List.of(
                    "aaa/first",
                    "elastic/auto-ops",
                    "elastic/fleet-server",
                    "elastic/fleet-server-remote",
                    "elastic/kibana",
                    "engineering/deploy_bot"
                )
            )
        );
        assertThat(
            infos.stream().map(ServiceAccountInfo::managedBy).toList(),
            equalTo(
                List.of(
                    ServiceAccountManagedBy.USER,
                    ServiceAccountManagedBy.ELASTIC,
                    ServiceAccountManagedBy.ELASTIC,
                    ServiceAccountManagedBy.ELASTIC,
                    ServiceAccountManagedBy.ELASTIC,
                    ServiceAccountManagedBy.USER
                )
            )
        );
    }

    public void testAskingOnlyForUserManagedAccountsExcludesTheBuiltInOnes() {
        assertThat(infosFor(userOnly()), empty());

        final ServiceAccountInfo.UserManaged account = new ServiceAccountInfo.UserManaged(
            "engineering/deploy_bot",
            List.of("deployer"),
            true
        );
        stubUserManagedAccounts(List.of(account));
        assertThat(infosFor(userOnly()), contains(account));
    }

    public void testTheNameFilterIsPassedToTheAccountStore() {
        infosFor(new GetServiceAccountRequest("engineering", "deploy_bot", EnumSet.allOf(ServiceAccountManagedBy.class)));
        verify(serviceAccountService).getUserManagedAccountInfos(eq("engineering"), eq("deploy_bot"), any());
    }

    public void testAFailedAccountStoreReadFailsTheRequest() {
        stubUserManagedAccountsFailure();
        for (GetServiceAccountRequest request : List.of(bothKinds(), userOnly())) {
            final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
            transportGetServiceAccountAction.doExecute(mock(Task.class), request, future);
            final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
            assertThat(e.getMessage(), equalTo("account store unavailable"));
        }
    }

    private static GetServiceAccountRequest elasticOnly() {
        return new GetServiceAccountRequest(null, null, EnumSet.of(ServiceAccountManagedBy.ELASTIC));
    }

    private static GetServiceAccountRequest userOnly() {
        return new GetServiceAccountRequest(null, null, EnumSet.of(ServiceAccountManagedBy.USER));
    }

    private static GetServiceAccountRequest bothKinds() {
        return new GetServiceAccountRequest(null, null, EnumSet.allOf(ServiceAccountManagedBy.class));
    }

    private List<String> principalsFor(GetServiceAccountRequest request) {
        return infosFor(request).stream().map(ServiceAccountInfo::principal).toList();
    }

    private List<ServiceAccountInfo> infosFor(GetServiceAccountRequest request) {
        final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
        transportGetServiceAccountAction.doExecute(mock(Task.class), request, future);
        return Arrays.asList(future.actionGet().getServiceAccountInfos());
    }

    private void stubUserManagedAccounts(List<ServiceAccountInfo> infos) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<List<ServiceAccountInfo>> listener = (ActionListener<List<ServiceAccountInfo>>) invocation
                .getArguments()[2];
            listener.onResponse(infos);
            return null;
        }).when(serviceAccountService).getUserManagedAccountInfos(any(), any(), any());
    }

    private void stubUserManagedAccountsFailure() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<List<ServiceAccountInfo>> listener = (ActionListener<List<ServiceAccountInfo>>) invocation
                .getArguments()[2];
            listener.onFailure(new ElasticsearchException("account store unavailable"));
            return null;
        }).when(serviceAccountService).getUserManagedAccountInfos(any(), any(), any());
    }
}
