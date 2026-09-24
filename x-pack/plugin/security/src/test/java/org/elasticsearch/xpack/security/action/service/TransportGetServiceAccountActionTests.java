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
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountAuthor;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountType;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
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

    private static final ServiceAccountAuthor ALICE = new ServiceAccountAuthor("alice", "Alice", null, "native1", "native", null);
    private static final ServiceAccountAuthor BOB = new ServiceAccountAuthor("bob", null, null, "ldap1", "ldap", null);

    private ServiceAccountService serviceAccountService;
    private ProfileService profileService;
    private TransportGetServiceAccountAction transportGetServiceAccountAction;

    @Before
    public void init() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        serviceAccountService = mock(ServiceAccountService.class);
        profileService = mock(ProfileService.class);
        stubUserManagedAccounts(List.of());
        transportGetServiceAccountAction = new TransportGetServiceAccountAction(
            transportService,
            ActionFilters.EMPTY,
            serviceAccountService,
            profileService
        );
    }

    /**
     * One profile lookup covers every author of every account, creator then editor in account order, and the uids
     * come back in that order and land on the authors they belong to. Built-in accounts and accounts with no
     * attribution have no authors and pass through untouched, and an author without a profile leaves the field out.
     */
    public void testProfileUidsAreLookedUpForEveryAuthorWhenAskedFor() {
        final ServiceAccountInfo.UserManaged edited = attributed("apps/edited", ALICE, BOB);
        final ServiceAccountInfo.UserManaged created = attributed("apps/created", BOB, null);
        final ServiceAccountInfo.UserManaged legacy = new ServiceAccountInfo.UserManaged("apps/legacy", List.of(), true, null);
        stubUserManagedAccounts(List.of(edited, created, legacy));
        final boolean bobHasAProfile = randomBoolean();
        final String bobUid = bobHasAProfile ? "u_bob" : null;
        // alice (creator of edited), bob (editor of edited), bob (creator of created)
        stubProfileUids(Arrays.asList("u_alice", bobUid, bobUid));

        final List<ServiceAccountInfo> infos = infosFor(new GetServiceAccountRequest("apps", null, bothKinds().getType(), true));

        verify(profileService).resolveProfileUidsForServiceAccountAuthors(eq(List.of(ALICE, BOB, BOB)), any());
        assertThat(
            infos.stream().filter(info -> info instanceof ServiceAccountInfo.UserManaged).toList(),
            contains(created.withProfileUids(bobUid, null), edited.withProfileUids("u_alice", bobUid), legacy)
        );
    }

    public void testProfilesAreNotConsultedUnlessAskedFor() {
        stubUserManagedAccounts(List.of(attributed("apps/edited", ALICE, BOB)));
        infosFor(new GetServiceAccountRequest("apps", null, bothKinds().getType(), false));
        verify(profileService, never()).resolveProfileUidsForServiceAccountAuthors(any(), any());
    }

    public void testProfilesAreNotConsultedWhenNoAccountHasAnAuthor() {
        stubUserManagedAccounts(List.of(new ServiceAccountInfo.UserManaged("apps/legacy", List.of(), true, null)));
        infosFor(new GetServiceAccountRequest(null, null, bothKinds().getType(), true));
        infosFor(new GetServiceAccountRequest(null, null, builtInOnly().getType(), true));
        verify(profileService, never()).resolveProfileUidsForServiceAccountAuthors(any(), any());
    }

    /**
     * No profile index means no author has a profile, so the accounts are reported as they are rather than failing.
     */
    public void testAMissingProfileIndexLeavesTheAccountsWithoutProfileUids() {
        final ServiceAccountInfo.UserManaged edited = attributed("apps/edited", ALICE, BOB);
        stubUserManagedAccounts(List.of(edited));
        stubProfileUids(null);
        assertThat(infosFor(new GetServiceAccountRequest("apps", null, userManagedOnly().getType(), true)), contains(edited));
    }

    public void testAFailedProfileLookupFailsTheRequest() {
        stubUserManagedAccounts(List.of(attributed("apps/edited", ALICE, BOB)));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<String>> listener = (ActionListener<Collection<String>>) invocation.getArguments()[1];
            listener.onFailure(new ElasticsearchException("profiles unavailable"));
            return null;
        }).when(profileService).resolveProfileUidsForServiceAccountAuthors(any(), any());

        final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
        transportGetServiceAccountAction.doExecute(
            mock(Task.class),
            new GetServiceAccountRequest("apps", null, userManagedOnly().getType(), true),
            future
        );
        assertThat(expectThrows(ElasticsearchException.class, future::actionGet).getMessage(), equalTo("profiles unavailable"));
    }

    private static ServiceAccountInfo.UserManaged attributed(String principal, ServiceAccountAuthor creator, ServiceAccountAuthor editor) {
        return new ServiceAccountInfo.UserManaged(
            principal,
            List.of("role"),
            true,
            null,
            creator,
            Instant.ofEpochMilli(1_700_000_000_000L),
            editor,
            editor == null ? null : Instant.ofEpochMilli(1_700_000_001_000L)
        );
    }

    private void stubProfileUids(Collection<String> profileUids) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<String>> listener = (ActionListener<Collection<String>>) invocation.getArguments()[1];
            listener.onResponse(profileUids);
            return null;
        }).when(profileService).resolveProfileUidsForServiceAccountAuthors(any(), any());
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
        assertThat(principalsFor(builtInOnly()), equalTo(ALL_BUILT_IN_PRINCIPALS));
        verify(serviceAccountService, never()).getUserManagedAccountInfos(any(), any(), any());
    }

    public void testBothKindsAreReportedTogether() {
        stubUserManagedAccounts(
            List.of(
                new ServiceAccountInfo.UserManaged("engineering/deploy_bot", List.of("deployer"), true, null),
                new ServiceAccountInfo.UserManaged("aaa/first", List.of("reader"), false, null)
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
            infos.stream().map(ServiceAccountInfo::type).toList(),
            equalTo(
                List.of(
                    ServiceAccountType.USER_MANAGED,
                    ServiceAccountType.BUILT_IN,
                    ServiceAccountType.BUILT_IN,
                    ServiceAccountType.BUILT_IN,
                    ServiceAccountType.BUILT_IN,
                    ServiceAccountType.USER_MANAGED
                )
            )
        );
    }

    public void testAskingOnlyForUserManagedAccountsExcludesTheBuiltInOnes() {
        assertThat(infosFor(userManagedOnly()), empty());

        final ServiceAccountInfo.UserManaged account = new ServiceAccountInfo.UserManaged(
            "engineering/deploy_bot",
            List.of("deployer"),
            true,
            null
        );
        stubUserManagedAccounts(List.of(account));
        assertThat(infosFor(userManagedOnly()), contains(account));
    }

    public void testTheNameFilterIsPassedToTheAccountStore() {
        infosFor(new GetServiceAccountRequest("engineering", "deploy_bot", EnumSet.allOf(ServiceAccountType.class)));
        verify(serviceAccountService).getUserManagedAccountInfos(eq("engineering"), eq("deploy_bot"), any());
    }

    public void testAFailedAccountStoreReadFailsTheRequest() {
        stubUserManagedAccountsFailure();
        for (GetServiceAccountRequest request : List.of(bothKinds(), userManagedOnly())) {
            final PlainActionFuture<GetServiceAccountResponse> future = new PlainActionFuture<>();
            transportGetServiceAccountAction.doExecute(mock(Task.class), request, future);
            final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
            assertThat(e.getMessage(), equalTo("account store unavailable"));
        }
    }

    private static GetServiceAccountRequest builtInOnly() {
        return new GetServiceAccountRequest(null, null, EnumSet.of(ServiceAccountType.BUILT_IN));
    }

    private static GetServiceAccountRequest userManagedOnly() {
        return new GetServiceAccountRequest(null, null, EnumSet.of(ServiceAccountType.USER_MANAGED));
    }

    private static GetServiceAccountRequest bothKinds() {
        return new GetServiceAccountRequest(null, null, EnumSet.allOf(ServiceAccountType.class));
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
