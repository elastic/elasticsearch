/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The destination of an {@code INTO} must be authorized before anything about it is reported. A caller who may not write learns
 * nothing, and the denial is the one a bulk would give for a document it creates, so it cannot tell a missing destination from an
 * existing one.
 */
public class HasPrivilegesIntoDestinationAuthorizerTests extends ESTestCase {
    private static final String USER = "alice";
    private static final String CREATE_DOCUMENT_ACTION = "indices:data/write/index:op_type/create";
    private static final String AUTO_CREATE_ACTION = "indices:admin/auto_create";

    public void testAllowAllPassesWithoutAskingAnything() {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        IntoDestinationAuthorizer.allowAll().authorize("dest", randomBoolean(), future);
        safeGet(future);
    }

    public void testExistingDestinationNeedsCreateDoc() {
        FakeChecker checker = new FakeChecker("create_doc");
        authorizeSucceeds(authorizer(checker), "dest", true);

        assertThat(checker.requests, hasSize(1));
        var privileges = checker.requests.get(0).indexPrivileges();
        assertThat(privileges.length, equalTo(1));
        assertThat(privileges[0].getIndices(), equalTo(new String[] { "dest" }));
        assertThat(List.of(privileges[0].getPrivileges()), containsInAnyOrder("create_doc"));
        assertThat(checker.requests.get(0).username(), equalTo(USER));
    }

    public void testExistingDestinationWithoutCreateDocIsForbidden() {
        // Read access does not imply write access: the caller may even hold every other privilege
        Exception e = authorize(authorizer(new FakeChecker("read", "create_index", "auto_configure")), "dest", true);
        assertForbidden(e, CREATE_DOCUMENT_ACTION);
    }

    public void testMissingDestinationAsksForCreateDocAndCreation() {
        FakeChecker checker = new FakeChecker("create_doc", "create_index");
        authorizeSucceeds(authorizer(checker), "dest", false);
        assertThat(
            List.of(checker.requests.get(0).indexPrivileges()[0].getPrivileges()),
            containsInAnyOrder("create_doc", "create_index", "auto_configure")
        );
    }

    public void testMissingDestinationPassesWithCreateIndex() {
        authorizeSucceeds(authorizer(new FakeChecker("create_doc", "create_index")), "dest", false);
    }

    public void testMissingDestinationPassesWithAutoConfigure() {
        authorizeSucceeds(authorizer(new FakeChecker("create_doc", "auto_configure")), "dest", false);
    }

    public void testMissingDestinationWithoutAnyWayToCreateItIsForbiddenOnAutoCreate() {
        assertForbidden(authorize(authorizer(new FakeChecker("create_doc")), "dest", false), AUTO_CREATE_ACTION);
    }

    public void testMissingDestinationWithoutCreateDocIsForbiddenEvenWithAutoCreate() {
        assertForbidden(authorize(authorizer(new FakeChecker("create_index", "auto_configure")), "dest", false), CREATE_DOCUMENT_ACTION);
    }

    /** A caller who cannot write must not be able to tell an existing destination from a missing one. */
    public void testDenialOfCallerWithoutCreateDocIsIdenticalForExistingAndMissingDestination() {
        FakeChecker existingChecker = new FakeChecker("read");
        FakeChecker missingChecker = new FakeChecker("read");
        Exception existing = authorize(authorizer(existingChecker), "existing-dest", true);
        Exception missing = authorize(authorizer(missingChecker), "missing-dest", false);

        assertThat(
            existing.getMessage().replace("existing-dest", "<dest>"),
            equalTo(missing.getMessage().replace("missing-dest", "<dest>"))
        );
        assertThat(((ElasticsearchSecurityException) existing).status(), equalTo(((ElasticsearchSecurityException) missing).status()));
    }

    public void testDenialNamesTheCallerAndTheDestinationTheyTyped() {
        Exception e = authorize(authorizer(new FakeChecker()), "dest", true);
        assertThat(e.getMessage(), containsString("[" + USER + "]"));
        assertThat(e.getMessage(), containsString("[dest]"));
        assertThat(e.getMessage(), not(containsString("create_index")));
    }

    public void testCheckerFailureIsPropagated() {
        IllegalStateException failure = new IllegalStateException("boom");
        assertThat(authorize(authorizer(new FakeChecker(failure)), "dest", true), equalTo(failure));
    }

    public void testMissingAuthenticationFails() {
        IntoDestinationAuthorizer authorizer = new HasPrivilegesIntoDestinationAuthorizer(
            () -> null,
            new FakeChecker("create_doc"),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        assertThat(authorize(authorizer, "dest", true), instanceOf(IllegalStateException.class));
    }

    /** Security reads a leading slash as a regular expression and a star as a wildcard, which would authorize more than one index. */
    public void testNameSecurityWouldReadAsAPatternIsRejectedWithoutAskingSecurity() {
        for (String name : List.of("/.*/", "/dest/", "dest*", "*", "-dest", "a,b", "remote:dest", "")) {
            FakeChecker checker = new FakeChecker("create_doc", "create_index", "auto_configure");
            Exception e = authorize(authorizer(checker), name, randomBoolean());
            assertThat(name, e, instanceOf(InvalidIndexNameException.class));
            assertThat(name, ((InvalidIndexNameException) e).status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(name, checker.requests, empty());
        }
    }

    /** The reply arrives on a security thread, and planning continues from the decision, so it must continue on the search pool. */
    public void testDecisionIsDeliveredOnTheExecutorWhateverThreadTheReplyArrivesOn() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            AtomicReference<Thread> replyThread = new AtomicReference<>();
            HasPrivilegesIntoDestinationAuthorizer.PrivilegeChecker onAnotherThread = (request, listener) -> {
                FakeChecker granting = new FakeChecker("create_doc");
                new Thread(() -> {
                    replyThread.set(Thread.currentThread());
                    granting.check(request, listener);
                }, "security-reply").start();
            };
            IntoDestinationAuthorizer authorizer = new HasPrivilegesIntoDestinationAuthorizer(
                () -> USER,
                onAnotherThread,
                threadPool.executor(ThreadPool.Names.SEARCH)
            );
            PlainActionFuture<Thread> decidedOn = new PlainActionFuture<>();
            authorizer.authorize("dest", true, decidedOn.map(ignored -> Thread.currentThread()));

            Thread decision = safeGet(decidedOn);
            assertThat(decision.getName(), containsString("[" + ThreadPool.Names.SEARCH + "]"));
            assertThat(decision, not(equalTo(replyThread.get())));
        } finally {
            terminate(threadPool);
        }
    }

    /** Records the requests it receives and grants exactly the privileges it was given. */
    private static final class FakeChecker implements HasPrivilegesIntoDestinationAuthorizer.PrivilegeChecker {
        final List<HasPrivilegesRequest> requests = new ArrayList<>();
        private final Set<String> granted;
        @Nullable
        private final Exception failure;

        FakeChecker(String... granted) {
            this.granted = Set.of(granted);
            this.failure = null;
        }

        FakeChecker(Exception failure) {
            this.granted = Set.of();
            this.failure = failure;
        }

        @Override
        public void check(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
            requests.add(request);
            if (failure != null) {
                listener.onFailure(failure);
                return;
            }
            var privileges = request.indexPrivileges()[0];
            ResourcePrivileges.Builder resource = ResourcePrivileges.builder(privileges.getIndices()[0]);
            for (String privilege : privileges.getPrivileges()) {
                resource.addPrivilege(privilege, granted.contains(privilege));
            }
            boolean all = granted.containsAll(List.of(privileges.getPrivileges()));
            listener.onResponse(new HasPrivilegesResponse(USER, all, Map.of(), List.of(resource.build()), Map.of()));
        }
    }

    private static IntoDestinationAuthorizer authorizer(HasPrivilegesIntoDestinationAuthorizer.PrivilegeChecker checker) {
        return new HasPrivilegesIntoDestinationAuthorizer(() -> USER, checker, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    private static Exception authorize(IntoDestinationAuthorizer authorizer, String destination, boolean exists) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer.authorize(destination, exists, future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        return (Exception) e.getCause();
    }

    private static void authorizeSucceeds(IntoDestinationAuthorizer authorizer, String destination, boolean exists) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer.authorize(destination, exists, future);
        safeGet(future);
    }

    private static void assertForbidden(Exception e, String action) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("action [" + action + "] is unauthorized for user [" + USER + "]"));
    }
}
