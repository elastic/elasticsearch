/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.ResultsAndErrors;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.action.profile.ProfileHasPrivilegesRequestTests.randomValidPrivilegesToCheckRequest;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult.ALL_CHECKS_SUCCESS_NO_DETAILS;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult.SOME_CHECKS_FAILURE_NO_DETAILS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportProfileHasPrivilegesActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private AuthorizationService authorizationService;
    private NativePrivilegeStore nativePrivilegeStore;
    private ProfileService profileService;
    private TransportProfileHasPrivilegesAction transportProfileHasPrivilegesAction;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(TransportProfileHasPrivilegesActionTests.class.getSimpleName());
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        authorizationService = mock(AuthorizationService.class);
        nativePrivilegeStore = mock(NativePrivilegeStore.class);
        profileService = mock(ProfileService.class);
        transportProfileHasPrivilegesAction = new TransportProfileHasPrivilegesAction(
            transportService,
            mock(ActionFilters.class),
            authorizationService,
            nativePrivilegeStore,
            profileService,
            threadPool
        );
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    public void testMultipleConcurrentCheckPrivileges() throws Exception {

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
            final var listener = (ActionListener<ResultsAndErrors<Map.Entry<String, Subject>>>) invocation.getArguments()[1];
            listener.onResponse(new ResultsAndErrors<>(profileUidToSubject.entrySet(), Map.of()));
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
            ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener = (ActionListener<
                AuthorizationEngine.PrivilegesCheckResult>) invocation.getArguments()[3];
            // run this asynchronously to test concurrency
            threadPool.generic().submit(() -> {
                if (errorProfileUids.contains(subject.getUser().principal().substring("user_for_profile_".length()))) {
                    listener.onFailure(new ElasticsearchException("failed to verify privileges for " + subject));
                } else if (noPrivilegesProfileUids.contains(subject.getUser().principal().substring("user_for_profile_".length()))) {
                    listener.onResponse(SOME_CHECKS_FAILURE_NO_DETAILS);
                } else {
                    listener.onResponse(ALL_CHECKS_SUCCESS_NO_DETAILS);
                }
            });
            return null;
        }).when(authorizationService)
            .checkPrivileges(any(Subject.class), eq(request.privilegesToCheck()), eq(List.of()), anyActionListener());

        final PlainActionFuture<ProfileHasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportProfileHasPrivilegesAction.doExecute(mock(CancellableTask.class), request, listener);

        ProfileHasPrivilegesResponse response = listener.get();
        assertThat(response.errors().keySet(), equalTo(errorProfileUids));
        Set<String> hasPrivilegeUids = new HashSet<>(allProfileUids);
        hasPrivilegeUids.removeAll(errorProfileUids);
        hasPrivilegeUids.removeAll(noPrivilegesProfileUids);
        assertThat(response.hasPrivilegeUids(), is(hasPrivilegeUids));
    }

    @SuppressWarnings("unchecked")
    public void testNoProfileSubjectsFound() throws Exception {
        final Set<String> allProfileUids = new HashSet<>(randomList(0, 10, () -> randomAlphaOfLengthBetween(4, 10)));
        final Set<String> errorProfileUids = new HashSet<>(randomSubsetOf(allProfileUids));

        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            new ArrayList<>(allProfileUids),
            randomValidPrivilegesToCheckRequest()
        );

        doAnswer(invocation -> {
            final var listener = (ActionListener<ResultsAndErrors<Map.Entry<String, Subject>>>) invocation.getArguments()[1];
            listener.onResponse(
                new ResultsAndErrors<>(
                    List.of(),
                    errorProfileUids.stream().collect(Collectors.toMap(Function.identity(), uid -> mock(Exception.class)))
                )
            );
            return null;
        }).when(profileService).getProfileSubjects(anyCollection(), anyActionListener());

        doAnswer(invocation -> {
            final ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocation.getArguments()[2];
            listener.onFailure(new ElasticsearchException("App privileges should not be resolved when there are no subjects found"));
            return null;
        }).when(nativePrivilegeStore).getPrivileges(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener = (ActionListener<
                AuthorizationEngine.PrivilegesCheckResult>) invocation.getArguments()[4];
            listener.onFailure(new ElasticsearchException("Privileges should not be checked when there are no subjects found"));
            return null;
        }).when(authorizationService).checkPrivileges(any(), any(), any(), any());

        final PlainActionFuture<ProfileHasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportProfileHasPrivilegesAction.doExecute(mock(CancellableTask.class), request, listener);

        ProfileHasPrivilegesResponse response = listener.get();
        assertThat(response.hasPrivilegeUids(), emptyIterable());
        assertThat(response.errors().keySet(), equalTo(errorProfileUids));
    }

    public void testDLSQueryIndicesPrivilegesRequestValidation() {
        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(1, 5)];
        for (int i = 0; i < indicesPrivileges.length; i++) {
            indicesPrivileges[i] = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(randomFrom("read", "write"))
                .indices(randomAlphaOfLengthBetween(2, 8))
                .query(new BytesArray(randomAlphaOfLength(5)))
                .build();
        }

        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            randomList(1, 100, () -> randomAlphaOfLengthBetween(4, 10)),
            new AuthorizationEngine.PrivilegesToCheck(
                new String[0],
                indicesPrivileges,
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                randomBoolean()
            )
        );

        final PlainActionFuture<ProfileHasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportProfileHasPrivilegesAction.execute(mock(CancellableTask.class), request, listener);

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> listener.actionGet());
        assertThat(ile, notNullValue());
        assertThat(ile.getMessage(), containsString("may only check index privileges without any DLS query"));
    }

    @SuppressWarnings("unchecked")
    public void testCancellation() throws Exception {
        final List<String> profileUids = new ArrayList<>(new HashSet<>(randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 10))));
        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(profileUids, randomValidPrivilegesToCheckRequest());
        doAnswer(invocation -> {
            Collection<String> uidsArg = (Collection<String>) invocation.getArguments()[0];
            Map<String, Subject> profileUidToSubject = new HashMap<>();
            for (String uid : uidsArg) {
                profileUidToSubject.put(uid, new Subject(new User("user_for_profile_" + uid), mock(Authentication.RealmRef.class)));
            }
            final var listener = (ActionListener<ResultsAndErrors<Map.Entry<String, Subject>>>) invocation.getArguments()[1];
            listener.onResponse(new ResultsAndErrors<>(profileUidToSubject.entrySet(), Map.of()));
            return null;
        }).when(profileService).getProfileSubjects(anyCollection(), anyActionListener());

        doAnswer(invocation -> {
            final ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocation.getArguments()[2];
            listener.onResponse(List.of());
            return null;
        }).when(nativePrivilegeStore).getPrivileges(anyCollection(), any(), anyActionListener());

        final AtomicInteger cancelCountDown = new AtomicInteger(randomIntBetween(1, profileUids.size() + 1));
        final boolean taskActuallyCancelled = cancelCountDown.get() <= profileUids.size();
        final CancellableTask cancellableTask = new CancellableTask(0, "type", "action", "description", TaskId.EMPTY_TASK_ID, Map.of());

        if (cancelCountDown.decrementAndGet() == 0) {
            TaskCancelHelper.cancel(cancellableTask, "reason");
        }
        doAnswer(invocation -> {
            ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener = (ActionListener<
                AuthorizationEngine.PrivilegesCheckResult>) invocation.getArguments()[3];
            if (cancelCountDown.decrementAndGet() == 0) {
                TaskCancelHelper.cancel(cancellableTask, "reason");
            }
            listener.onResponse(ALL_CHECKS_SUCCESS_NO_DETAILS);
            return null;
        }).when(authorizationService)
            .checkPrivileges(any(Subject.class), eq(request.privilegesToCheck()), eq(List.of()), anyActionListener());

        final PlainActionFuture<ProfileHasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportProfileHasPrivilegesAction.doExecute(cancellableTask, request, listener);
        if (taskActuallyCancelled) {
            ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
            assertThat(e.getCause(), instanceOf(TaskCancelledException.class));
        } else {
            ProfileHasPrivilegesResponse profileHasPrivilegesResponse = listener.get();
            assertThat(profileHasPrivilegesResponse.hasPrivilegeUids(), contains(profileUids.toArray()));
        }
    }
}
