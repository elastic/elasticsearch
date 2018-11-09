/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportPutRoleActionTests extends ESTestCase {

    public void testReservedRole() {
        final String roleName = randomFrom(new ArrayList<>(ReservedRolesStore.names()));
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportPutRoleAction action = new TransportPutRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        PutRoleRequest request = new PutRoleRequest();
        request.name(roleName);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutRoleResponse>() {
            @Override
            public void onResponse(PutRoleResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(instanceOf(IllegalArgumentException.class)));
        assertThat(throwableRef.get().getMessage(), containsString("is reserved and cannot be modified"));
        verifyZeroInteractions(rolesStore);
    }

    public void testValidRole() {
        final String roleName = randomFrom("admin", "dept_a", "restricted");
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportPutRoleAction action = new TransportPutRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        final boolean created = randomBoolean();
        PutRoleRequest request = new PutRoleRequest();
        request.name(roleName);

        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                assert args.length == 3;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[2];
                listener.onResponse(created);
                return null;
            }
        }).when(rolesStore).putRole(eq(request), any(RoleDescriptor.class), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutRoleResponse>() {
            @Override
            public void onResponse(PutRoleResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().isCreated(), is(created));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(rolesStore, times(1)).putRole(eq(request), any(RoleDescriptor.class), any(ActionListener.class));
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException());
        final String roleName = randomFrom("admin", "dept_a", "restricted");
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportPutRoleAction action = new TransportPutRoleAction(mock(ActionFilters.class), rolesStore, transportService);

        PutRoleRequest request = new PutRoleRequest();
        request.name(roleName);

        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                assert args.length == 3;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[2];
                listener.onFailure(e);
                return null;
            }
        }).when(rolesStore).putRole(eq(request), any(RoleDescriptor.class), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutRoleResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutRoleResponse>() {
            @Override
            public void onResponse(PutRoleResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), is(sameInstance(e)));
        verify(rolesStore, times(1)).putRole(eq(request), any(RoleDescriptor.class), any(ActionListener.class));
    }
}
