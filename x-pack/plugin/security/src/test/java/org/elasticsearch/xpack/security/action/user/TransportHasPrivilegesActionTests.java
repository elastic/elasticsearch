/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportHasPrivilegesActionTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool("has privileges action tests");
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testHasPrivilegesRequestDoesNotAllowDLSRoleQueryBasedIndicesPrivileges() {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final SecurityContext context = mock(SecurityContext.class);
        final User user = new User("user-1", "superuser");
        final Authentication authentication = new Authentication(
            user,
            new Authentication.RealmRef("native", "default_native", "node1"),
            null
        );
        when(context.getAuthentication()).thenReturn(authentication);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        final TransportHasPrivilegesAction transportHasPrivilegesAction = new TransportHasPrivilegesAction(
            threadPool,
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(AuthorizationService.class),
            mock(NativePrivilegeStore.class),
            context,
            xContentRegistry()
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
        request.username("user-1");

        final PlainActionFuture<HasPrivilegesResponse> listener = new PlainActionFuture<>();
        transportHasPrivilegesAction.doExecute(mock(Task.class), request, listener);

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> listener.actionGet());
        assertThat(ile, notNullValue());
        assertThat(ile.getMessage(), containsString("users may only check the index privileges without any DLS role query"));
    }
}
