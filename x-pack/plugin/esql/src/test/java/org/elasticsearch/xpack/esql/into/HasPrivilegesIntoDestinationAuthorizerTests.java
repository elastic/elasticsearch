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
import org.elasticsearch.indices.InvalidIndexNameException;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The destination of an {@code INTO} must be authorized before anything about it is reported. A caller who may not write learns
 * nothing, and the denial is the one a bulk would give for a document it creates, so it cannot tell a missing destination from an
 * existing one.
 */
public class HasPrivilegesIntoDestinationAuthorizerTests extends ESTestCase {
    private static final String USER = "alice";
    private static final String CREATE_DOCUMENT_ACTION = "indices:data/write/index:op_type/create";
    private static final String AUTO_CREATE_ACTION = "indices:admin/auto_create";

    public void testExistingDestinationNeedsCreateDoc() {
        FakeChecker checker = new FakeChecker("create_doc");
        authorizeSucceeds(authorizer(checker), true);

        assertThat(checker.requests, hasSize(1));
        var privileges = checker.requests.getFirst().indexPrivileges();
        assertThat(privileges.length, equalTo(1));
        assertThat(privileges[0].getIndices(), equalTo(new String[] { "dest" }));
        assertThat(List.of(privileges[0].getPrivileges()), containsInAnyOrder("create_doc"));
        assertThat(checker.requests.getFirst().username(), equalTo(USER));
    }

    public void testExistingDestinationWithoutCreateDocIsForbidden() {
        assertForbidden(
            authorizeFails(authorizer(new FakeChecker("read", "create_index", "auto_configure")), "dest", true),
            CREATE_DOCUMENT_ACTION
        );
    }

    public void testMissingDestinationAsksForCreateDocAndCreation() {
        FakeChecker checker = new FakeChecker("create_doc", "create_index");
        authorizeSucceeds(authorizer(checker), false);
        assertThat(
            List.of(checker.requests.getFirst().indexPrivileges()[0].getPrivileges()),
            containsInAnyOrder("create_doc", "create_index", "auto_configure")
        );
    }

    public void testMissingDestinationPassesWithAutoConfigure() {
        authorizeSucceeds(authorizer(new FakeChecker("create_doc", "auto_configure")), false);
    }

    public void testMissingDestinationWithoutAnyWayToCreateItIsForbiddenOnAutoCreate() {
        assertForbidden(authorizeFails(authorizer(new FakeChecker("create_doc")), "dest", false), AUTO_CREATE_ACTION);
    }

    public void testMissingDestinationWithoutCreateDocIsForbiddenEvenWithAutoCreate() {
        assertForbidden(
            authorizeFails(authorizer(new FakeChecker("create_index", "auto_configure")), "dest", false),
            CREATE_DOCUMENT_ACTION
        );
    }

    public void testMissingAuthenticationFails() {
        IntoDestinationAuthorizer authorizer = new HasPrivilegesIntoDestinationAuthorizer(
            () -> null,
            new FakeChecker("create_doc"),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        assertThat(authorizeFails(authorizer, "dest", true), instanceOf(IllegalStateException.class));
    }

    /** Security reads a leading slash as a regular expression and a star as a wildcard, which would authorize more than one index. */
    public void testNameSecurityWouldReadAsAPatternIsRejectedWithoutAskingSecurity() {
        for (String name : List.of("/.*/", "/dest/", "dest*", "*", "-dest", "a,b", "remote:dest", "")) {
            FakeChecker checker = new FakeChecker("create_doc", "create_index", "auto_configure");
            Exception e = authorizeFails(authorizer(checker), name, randomBoolean());
            assertThat(name, e, instanceOf(InvalidIndexNameException.class));
            assertThat(name, ((InvalidIndexNameException) e).status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(name, checker.requests, empty());
        }
    }

    /** Records the requests it receives and grants exactly the privileges it was given. */
    private static final class FakeChecker implements HasPrivilegesIntoDestinationAuthorizer.PrivilegeChecker {
        final List<HasPrivilegesRequest> requests = new ArrayList<>();
        private final Set<String> granted;

        FakeChecker(String... granted) {
            this.granted = Set.of(granted);
        }

        @Override
        public void check(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
            requests.add(request);
            var privileges = request.indexPrivileges()[0];
            ResourcePrivileges.Builder resource = ResourcePrivileges.builder(privileges.getIndices()[0]);
            for (String privilege : privileges.getPrivileges()) {
                resource.addPrivilege(privilege, granted.contains(privilege));
            }
            boolean completeMatch = granted.containsAll(List.of(privileges.getPrivileges()));
            listener.onResponse(new HasPrivilegesResponse(USER, completeMatch, Map.of(), List.of(resource.build()), Map.of()));
        }
    }

    private static IntoDestinationAuthorizer authorizer(HasPrivilegesIntoDestinationAuthorizer.PrivilegeChecker checker) {
        return new HasPrivilegesIntoDestinationAuthorizer(() -> USER, checker, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    private static Exception authorizeFails(IntoDestinationAuthorizer authorizer, String destination, boolean exists) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer.authorize(destination, exists, future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        return (Exception) e.getCause();
    }

    private static void authorizeSucceeds(IntoDestinationAuthorizer authorizer, boolean exists) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer.authorize("dest", exists, future);
        safeGet(future);
    }

    private static void assertForbidden(Exception e, String action) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), equalTo("action [" + action + "] is unauthorized for user [" + USER + "] on indices [dest]"));
    }
}
