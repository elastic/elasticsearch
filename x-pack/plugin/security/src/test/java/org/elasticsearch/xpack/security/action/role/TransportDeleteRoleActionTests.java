/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportDeleteRoleActionTests extends ESTestCase {

    public void testReservedRole() {
        final String roleName = randomFrom(new ArrayList<>(ReservedRolesStore.names()));
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (x) -> null,
            null,
            Collections.emptySet()
        );
        TransportDeleteRoleAction action = new TransportDeleteRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        DeleteRoleRequest request = new DeleteRoleRequest();
        request.name(roleName);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<DeleteRoleResponse>() {
            @Override
            public void onResponse(DeleteRoleResponse deleteRoleResponse) {
                responseRef.set(deleteRoleResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(instanceOf(IllegalArgumentException.class)));
        assertThat(throwableRef.get().getMessage(), containsString("is reserved and cannot be deleted"));
        verifyNoMoreInteractions(rolesStore);
    }

    public void testValidRole() {
        final String roleName = randomFrom("admin", "dept_a", "restricted");
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (x) -> null,
            null,
            Collections.emptySet()
        );
        TransportDeleteRoleAction action = new TransportDeleteRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        DeleteRoleRequest request = new DeleteRoleRequest();
        request.name(roleName);

        final boolean found = randomBoolean();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
            listener.onResponse(found);
            return null;
        }).when(rolesStore).deleteRole(eq(request), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<DeleteRoleResponse>() {
            @Override
            public void onResponse(DeleteRoleResponse deleteRoleResponse) {
                responseRef.set(deleteRoleResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().found(), is(found));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(rolesStore, times(1)).deleteRole(eq(request), anyActionListener());
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException());
        final String roleName = randomFrom("admin", "dept_a", "restricted");
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (x) -> null,
            null,
            Collections.emptySet()
        );
        TransportDeleteRoleAction action = new TransportDeleteRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        DeleteRoleRequest request = new DeleteRoleRequest();
        request.name(roleName);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
            listener.onFailure(e);
            return null;
        }).when(rolesStore).deleteRole(eq(request), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<DeleteRoleResponse>() {
            @Override
            public void onResponse(DeleteRoleResponse deleteRoleResponse) {
                responseRef.set(deleteRoleResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), is(sameInstance(e)));
        verify(rolesStore, times(1)).deleteRole(eq(request), anyActionListener());
    }
}
