/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class UserPrivilegeResolverTests extends ESTestCase {

    private Client client;
    private SecurityContext securityContext;
    private UserPrivilegeResolver resolver;

    @Before
    @SuppressWarnings("unchecked")
    public void setupTest() {
        client = mock(Client.class);
        securityContext = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));
        final ApplicationActionsResolver actionsResolver = mock(ApplicationActionsResolver.class);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) args[args.length - 1];
            listener.onResponse(Set.of("role:cluster:view", "role:cluster:admin", "role:cluster:operator", "role:cluster:monitor"));
            return null;
        }).when(actionsResolver).getActions(anyString(), any(ActionListener.class));
        resolver = new UserPrivilegeResolver(client, securityContext, actionsResolver);
    }

    public void testResolveZeroAccess() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final String app = randomAlphaOfLengthBetween(3, 8);
        setupUser(username);
        setupHasPrivileges(username, app);
        final PlainActionFuture<UserPrivilegeResolver.UserPrivileges> future = new PlainActionFuture<>();
        final Function<String, Set<String>> roleMapping =
            Map.of("role:cluster:view", Set.of("viewer"), "role:cluster:admin", Set.of("admin"))::get;
        resolver.resolve(service(app, "cluster:" + randomLong(), roleMapping), future);
        final UserPrivilegeResolver.UserPrivileges privileges = future.get();
        assertThat(privileges.principal, equalTo(username));
        assertThat(privileges.hasAccess, equalTo(false));
        assertThat(privileges.roles, emptyIterable());
    }

    public void testResolveSsoWithNoRoleAccess() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final String app = randomAlphaOfLengthBetween(3, 8);
        final String resource = "cluster:" + MessageDigests.toHexString(randomByteArrayOfLength(16));
        final String viewerAction = "role:cluster:view";
        final String adminAction = "role:cluster:admin";

        setupUser(username);
        setupHasPrivileges(username, app, access(resource, viewerAction, false), access(resource, adminAction, false));

        final PlainActionFuture<UserPrivilegeResolver.UserPrivileges> future = new PlainActionFuture<>();
        final Function<String, Set<String>> roleMapping = Map.of(viewerAction, Set.of("viewer"), adminAction, Set.of("admin"))::get;
        resolver.resolve(service(app, resource, roleMapping), future);
        final UserPrivilegeResolver.UserPrivileges privileges = future.get();
        assertThat(privileges.principal, equalTo(username));
        assertThat(privileges.hasAccess, equalTo(false));
        assertThat(privileges.roles, emptyIterable());
    }

    public void testResolveSsoWithSingleRole() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final String app = randomAlphaOfLengthBetween(3, 8);
        final String resource = "cluster:" + MessageDigests.toHexString(randomByteArrayOfLength(16));
        final String viewerAction = "role:cluster:view";
        final String adminAction = "role:cluster:admin";

        setupUser(username);
        setupHasPrivileges(username, app, access(resource, viewerAction, true), access(resource, adminAction, false));

        final PlainActionFuture<UserPrivilegeResolver.UserPrivileges> future = new PlainActionFuture<>();
        final Function<String, Set<String>> roleMapping = Map.of(viewerAction, Set.of("viewer"), adminAction, Set.of("admin"))::get;
        resolver.resolve(service(app, resource, roleMapping), future);
        final UserPrivilegeResolver.UserPrivileges privileges = future.get();
        assertThat(privileges.principal, equalTo(username));
        assertThat(privileges.hasAccess, equalTo(true));
        assertThat(privileges.roles, containsInAnyOrder("viewer"));
    }

    public void testResolveSsoWithMultipleRoles() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final String app = randomAlphaOfLengthBetween(3, 8);
        final String resource = "cluster:" + MessageDigests.toHexString(randomByteArrayOfLength(16));
        final String viewerAction = "role:cluster:view";
        final String adminAction = "role:cluster:admin";
        final String operatorAction = "role:cluster:operator";
        final String monitorAction = "role:cluster:monitor";

        setupUser(username);
        setupHasPrivileges(username, app,
            access(resource, viewerAction, false),
            access(resource, adminAction, false),
            access(resource, operatorAction, true),
            access(resource, monitorAction, true)
        );

        final PlainActionFuture<UserPrivilegeResolver.UserPrivileges> future = new PlainActionFuture<>();
        Function<String, Set<String>> roleMapping = action -> {
            switch (action) {
                case viewerAction:
                    return Set.of("viewer");
                case adminAction:
                    return Set.of("admin");
                case operatorAction:
                    return Set.of("operator");
                case monitorAction:
                    return Set.of("monitor");
            }
            return Set.of();
        };
        resolver.resolve(service(app, resource, roleMapping), future);
        final UserPrivilegeResolver.UserPrivileges privileges = future.get();
        assertThat(privileges.principal, equalTo(username));
        assertThat(privileges.hasAccess, equalTo(true));
        assertThat(privileges.roles, containsInAnyOrder("operator", "monitor"));
    }

    private ServiceProviderPrivileges service(String appName, String resource, Function<String, Set<String>> roleMapping) {
        return new ServiceProviderPrivileges(appName, resource, roleMapping);
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    private HasPrivilegesResponse setupHasPrivileges(String username, String appName,
                                                     Tuple<String, Tuple<String, Boolean>>... resourceActionAccess) {
        final boolean isCompleteMatch = randomBoolean();
        final Map<String, Map<String, Boolean>> resourcePrivilegeMap = new HashMap<>(resourceActionAccess.length);
        for (Tuple<String, Tuple<String, Boolean>> t : resourceActionAccess) {
            final String resource = t.v1();
            final String action = t.v2().v1();
            final Boolean access = t.v2().v2();
            resourcePrivilegeMap.computeIfAbsent(resource, ignore -> new HashMap<>()).put(action, access);
        }
        final Collection<ResourcePrivileges> privileges = resourcePrivilegeMap.entrySet().stream()
            .map(e -> ResourcePrivileges.builder(e.getKey()).addPrivileges(e.getValue()).build())
            .collect(Collectors.toList());
        final Map<String, Collection<ResourcePrivileges>> appPrivs = Map.of(appName, privileges);
        final HasPrivilegesResponse response = new HasPrivilegesResponse(username, isCompleteMatch, Map.of(), Set.of(), appPrivs);

        Mockito.doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args.length, equalTo(3));
            ActionListener<HasPrivilegesResponse> listener = (ActionListener<HasPrivilegesResponse>) args[args.length - 1];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(HasPrivilegesAction.INSTANCE), any(HasPrivilegesRequest.class), any(ActionListener.class));
        return response;
    }

    private Tuple<String, Tuple<String, Boolean>> access(String resource, String action, boolean access) {
        return new Tuple<>(resource, new Tuple<>(action, access));
    }

    private void setupUser(String principal) {
        final User user = new User(principal, randomAlphaOfLengthBetween(6, 12));
        securityContext.setUser(user, Version.CURRENT);
    }

}
