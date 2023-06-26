/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ActionListenerUtils.anyCollection;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportHasPrivilegesActionTests extends ESTestCase {

    public void testHasPrivilegesRequestDoesNotAllowDLSRoleQueryBasedIndicesPrivileges() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext context = mock(SecurityContext.class);
        final User user = new User("user-1", "superuser");
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new Authentication.RealmRef("native", "default_native", "node1"))
            .build(false);
        when(context.getAuthentication()).thenReturn(authentication);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        final TransportHasPrivilegesAction transportHasPrivilegesAction = new TransportHasPrivilegesAction(
            mock(TransportService.class),
            new ActionFilters(Set.of()),
            mock(AuthorizationService.class),
            mock(NativePrivilegeStore.class),
            context
        );

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(1, 5)];
        for (int i = 0; i < indicesPrivileges.length; i++) {
            indicesPrivileges[i] = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(randomFrom("read", "write"))
                .indices(randomAlphaOfLengthBetween(2, 8))
                .query(new BytesArray(randomAlphaOfLength(5)))
                .build();
        }
        request.indexPrivileges(indicesPrivileges);
        request.clusterPrivileges(new String[0]);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request.username("user-1");

        final PlainActionFuture<HasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportHasPrivilegesAction.execute(mock(Task.class), request, listener);

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> listener.actionGet());
        assertThat(ile, notNullValue());
        assertThat(ile.getMessage(), containsString("may only check index privileges without any DLS query"));
    }

    public void testRequiresSameUser() {
        final SecurityContext context = mock(SecurityContext.class);

        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocation.getArgument(2);
            listener.onResponse(List.of());
            return null;
        }).when(privilegeStore).getPrivileges(any(), any(), anyActionListener());
        final AuthorizationService authorizationService = mock(AuthorizationService.class);
        final boolean isCompleteMatch = randomBoolean();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<AuthorizationEngine.PrivilegesCheckResult>) invocation.getArgument(3);
            listener.onResponse(
                new AuthorizationEngine.PrivilegesCheckResult(
                    isCompleteMatch,
                    new AuthorizationEngine.PrivilegesCheckResult.Details(Map.of(), Map.of(), Map.of())
                )
            );
            return null;
        }).when(authorizationService).checkPrivileges(any(), any(), anyCollection(), anyActionListener());
        final var action = new TransportHasPrivilegesAction(
            mock(TransportService.class),
            new ActionFilters(Set.of()),
            authorizationService,
            privilegeStore,
            context
        );

        final String username = randomAlphaOfLengthBetween(5, 10);
        final User user = new User(username);

        // Scenario 1 - regular authentication with wrong username
        final Authentication authentication1 = AuthenticationTestHelper.builder().user(user).realm().build();
        when(context.getAuthentication()).thenReturn(authentication1);
        final HasPrivilegesRequest request1 = new HasPrivilegesRequest();
        request1.clusterPrivileges("all");
        request1.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request1.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request1.username(randomValueOtherThan(username, () -> randomAlphaOfLengthBetween(5, 10)));
        final PlainActionFuture<HasPrivilegesResponse> future1 = new PlainActionFuture<>();
        action.execute(mock(Task.class), request1, future1);
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("users may only check the privileges of their own account"));
        verifyNoInteractions(privilegeStore, authorizationService);

        // Scenario 2 - cross cluster access authentication with right name on the API key but wrong name on the inner authentication
        final String requestedUsername = randomValueOtherThan(username, () -> randomAlphaOfLengthBetween(5, 10));
        final Authentication authentication2 = AuthenticationTestHelper.builder()
            .user(new User(requestedUsername))
            .apiKey()
            .build()
            .toCrossClusterAccess(AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(authentication1));
        when(context.getAuthentication()).thenReturn(authentication2);
        final HasPrivilegesRequest request2 = new HasPrivilegesRequest();
        request2.clusterPrivileges("all");
        request2.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request2.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request2.username(requestedUsername);
        final PlainActionFuture<HasPrivilegesResponse> future2 = new PlainActionFuture<>();
        action.execute(mock(Task.class), request2, future2);
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("users may only check the privileges of their own account"));
        verifyNoInteractions(privilegeStore, authorizationService);

        // Scenario 3 (success) - cross cluster access authentication with right name on the inner authentication
        final Authentication authentication3 = AuthenticationTestHelper.builder()
            .crossClusterAccess(
                randomAlphaOfLength(20),
                AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(
                    AuthenticationTestHelper.builder().user(new User(requestedUsername)).build()
                )
            )
            .build();
        when(context.getAuthentication()).thenReturn(authentication3);
        final HasPrivilegesRequest request3 = new HasPrivilegesRequest();
        request3.clusterPrivileges("all");
        request3.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request3.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request3.username(requestedUsername);

        final PlainActionFuture<HasPrivilegesResponse> future3 = new PlainActionFuture<>();
        action.execute(mock(Task.class), request3, future3);
        final HasPrivilegesResponse hasPrivilegesResponse = future3.actionGet();
        assertThat(hasPrivilegesResponse.getUsername(), equalTo(requestedUsername));
        assertThat(hasPrivilegesResponse.isCompleteMatch(), is(isCompleteMatch));
    }
}
