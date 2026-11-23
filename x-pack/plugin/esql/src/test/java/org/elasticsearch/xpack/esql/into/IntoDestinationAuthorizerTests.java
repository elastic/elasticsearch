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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The destination of an {@code INTO} must be authorized before anything about it is reported. A caller who may not write learns
 * nothing, and the denial is the one a bulk would give, so it cannot tell a missing destination from an existing one.
 */
public class IntoDestinationAuthorizerTests extends ESTestCase {
    private static final String USER = "alice";

    /** Records the requests it receives and grants exactly the privileges it was given. */
    private static final class FakeChecker implements IntoDestinationAuthorizer.PrivilegeChecker {
        final List<HasPrivilegesRequest> requests = new ArrayList<>();
        private final Set<String> granted;
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

    private static IntoDestinationAuthorizer authorizer(FakeChecker checker) {
        return new IntoDestinationAuthorizer(() -> USER, checker);
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

    public void testSecurityDisabledPassesWithoutAskingForPrivileges() {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        IntoDestinationAuthorizer.disabled().authorize("dest", randomBoolean(), future);
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
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
    }

    public void testMissingDestinationNeedsAutoCreate() {
        FakeChecker checker = new FakeChecker("create_doc");
        Exception e = authorize(authorizer(checker), "dest", false);
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(
            List.of(checker.requests.get(0).indexPrivileges()[0].getPrivileges()),
            containsInAnyOrder("create_doc", "create_index", "auto_configure")
        );
    }

    public void testMissingDestinationPassesWithCreateIndexOrAutoConfigure() {
        authorizeSucceeds(authorizer(new FakeChecker("create_doc", "create_index")), "dest", false);
        authorizeSucceeds(authorizer(new FakeChecker("create_doc", "auto_configure")), "dest", false);
    }

    public void testMissingDestinationWithoutCreateDocIsForbiddenEvenWithAutoCreate() {
        Exception e = authorize(authorizer(new FakeChecker("create_index", "auto_configure")), "dest", false);
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
    }

    /** A caller who cannot write must not be able to tell an existing destination from a missing one. */
    public void testDenialIsIdenticalForExistingAndMissingDestination() {
        Exception existing = authorize(authorizer(new FakeChecker()), "dest", true);
        Exception missing = authorize(authorizer(new FakeChecker()), "dest", false);
        assertThat(existing.getMessage(), equalTo(missing.getMessage()));
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
        IntoDestinationAuthorizer authorizer = new IntoDestinationAuthorizer(() -> null, new FakeChecker("create_doc"));
        assertThat(authorize(authorizer, "dest", true), instanceOf(IllegalStateException.class));
    }
}
