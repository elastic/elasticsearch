/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.profile.ProfileService.MultiProfileSubjectResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.action.profile.ProfileHasPrivilegesRequestTests.randomValidPrivilegesToCheckRequest;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransportProfileHasPrivilegesActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(TransportProfileHasPrivilegesActionTests.class.getSimpleName());
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    public void testMultipleConcurrentCheckPrivileges() throws ExecutionException, InterruptedException {
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        NativePrivilegeStore nativePrivilegeStore = mock(NativePrivilegeStore.class);
        ProfileService profileService = mock(ProfileService.class);
        SecurityContext securityContext = mock(SecurityContext.class);

        final TransportProfileHasPrivilegesAction transportProfileHasPrivilegesAction = new TransportProfileHasPrivilegesAction(
            transportService,
            actionFilters,
            authorizationService,
            nativePrivilegeStore,
            profileService,
            securityContext,
            threadPool
        );

        final Set<String> allProfileUids = new HashSet<>(randomList(1, 100, () -> randomAlphaOfLengthBetween(4, 10)));
        final Set<String> errorProfileUids = new HashSet<>(randomSubsetOf(allProfileUids));
        final Set<String> noPrivilegesProfileUids = new HashSet<>(randomSubsetOf(allProfileUids));
        noPrivilegesProfileUids.removeAll(errorProfileUids);

        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            new ArrayList<>(allProfileUids),
            randomValidPrivilegesToCheckRequest()
        );

        doAnswer(invocation -> {
            Collection<String> uidsArg = (Collection<String>) invocation.getArguments()[0];
            Map<String, Subject> profileUidToSubject = new HashMap<>();
            for (String uid : uidsArg) {
                profileUidToSubject.put(uid, new Subject(new User("user_for_profile_" + uid), mock(Authentication.RealmRef.class)));
            }
            final ActionListener<MultiProfileSubjectResponse> listener = (ActionListener<MultiProfileSubjectResponse>) invocation
                .getArguments()[1];
            listener.onResponse(new MultiProfileSubjectResponse(profileUidToSubject, List.of()));
            return null;
        }).when(profileService).getProfileSubjects(anyCollection(), anyActionListener());

        doAnswer(invocation -> {
            final ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocation.getArguments()[2];
            listener.onResponse(List.of());
            return null;
        }).when(nativePrivilegeStore).getPrivileges(anyCollection(), any(), anyActionListener());

        doAnswer(invocation -> {
            Subject subject = (Subject) invocation.getArguments()[0];
            // run this asynchronously to test concurrency
            threadPool.generic().submit(() -> {
                ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener = (ActionListener<
                    AuthorizationEngine.PrivilegesCheckResult>) invocation.getArguments()[3];
                if (errorProfileUids.contains(subject.getUser().principal().substring("user_for_profile_".length()))) {
                    listener.onFailure(new ElasticsearchException("failed to verify privileges for " + subject));
                } else if (noPrivilegesProfileUids.contains(subject.getUser().principal().substring("user_for_profile_".length()))) {
                    listener.onResponse(new AuthorizationEngine.PrivilegesCheckResult(false, Map.of(), Map.of(), Map.of()));
                } else {
                    listener.onResponse(new AuthorizationEngine.PrivilegesCheckResult(true, Map.of(), Map.of(), Map.of()));
                }
            });
            return null;
        }).when(authorizationService)
            .checkPrivileges(any(Subject.class), eq(request.privilegesToCheck()), eq(List.of()), anyActionListener());

        final PlainActionFuture<ProfileHasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportProfileHasPrivilegesAction.doExecute(mock(Task.class), request, listener);

        ProfileHasPrivilegesResponse response = listener.get();
        assertThat(response.errorUids(), is(errorProfileUids));
        Set<String> hasPrivilegeUids = new HashSet<>(allProfileUids);
        hasPrivilegeUids.removeAll(errorProfileUids);
        hasPrivilegeUids.removeAll(noPrivilegesProfileUids);
        assertThat(response.hasPrivilegeUids(), is(hasPrivilegeUids));
    }
}
