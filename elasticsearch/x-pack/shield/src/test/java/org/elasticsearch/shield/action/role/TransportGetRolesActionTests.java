/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.SecurityContext;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.permission.KibanaRole;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
import org.elasticsearch.shield.authz.store.ReservedRolesStore;
import org.elasticsearch.shield.user.KibanaUser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TransportGetRolesActionTests extends ESTestCase {

    public void testReservedRoles() {
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        SecurityContext context = mock(SecurityContext.class);
        TransportGetRolesAction action = new TransportGetRolesAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), rolesStore, mock(TransportService.class), new ReservedRolesStore(context));

        final boolean isKibanaUser = randomBoolean();
        if (isKibanaUser) {
            when(context.getUser()).thenReturn(KibanaUser.INSTANCE);
        }
        final int size = randomIntBetween(1, ReservedRolesStore.names().size());
        final List<String> names = randomSubsetOf(size, ReservedRolesStore.names());

        final List<String> expectedNames = new ArrayList<>(names);
        if (isKibanaUser == false) {
            expectedNames.remove(KibanaRole.NAME);
        }

        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<List<RoleDescriptor>> listener = (ActionListener<List<RoleDescriptor>>) args[1];
                listener.onResponse(Collections.emptyList());
                return null;
            }
        }).when(rolesStore).getRoleDescriptors(aryEq(Strings.EMPTY_ARRAY), any(ActionListener.class));

        GetRolesRequest request = new GetRolesRequest();
        request.names(names.toArray(Strings.EMPTY_ARRAY));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetRolesResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetRolesResponse>() {
            @Override
            public void onResponse(GetRolesResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        if (isKibanaUser && expectedNames.isEmpty()) {
                assertThat(responseRef.get().roles(), is(emptyArray()));
                verify(rolesStore, times(1)).getRoleDescriptors(eq(Strings.EMPTY_ARRAY), any(ActionListener.class));
        } else {
            List<String> retrievedRoleNames =
                    Arrays.asList(responseRef.get().roles()).stream().map(RoleDescriptor::getName).collect(Collectors.toList());
            assertThat(retrievedRoleNames, containsInAnyOrder(expectedNames.toArray(Strings.EMPTY_ARRAY)));
            verifyZeroInteractions(rolesStore);
        }
    }

    public void testStoreRoles() {
        final List<RoleDescriptor> storeRoleDescriptors = randomRoleDescriptors();
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        SecurityContext context = mock(SecurityContext.class);
        TransportGetRolesAction action = new TransportGetRolesAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), rolesStore, mock(TransportService.class), new ReservedRolesStore(context));

        final boolean isKibanaUser = randomBoolean();
        if (isKibanaUser) {
            when(context.getUser()).thenReturn(KibanaUser.INSTANCE);
        }

        GetRolesRequest request = new GetRolesRequest();
        request.names(storeRoleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY));

        if (request.names().length == 1) {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    String requestedName = (String) args[0];
                    ActionListener<RoleDescriptor> listener = (ActionListener<RoleDescriptor>) args[1];
                    Optional<RoleDescriptor> rd =
                            storeRoleDescriptors.stream().filter(r -> r.getName().equals(requestedName)).findFirst();
                    listener.onResponse(rd.get());
                    return null;
                }
            }).when(rolesStore).getRoleDescriptor(eq(request.names()[0]), any(ActionListener.class));
        } else {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<List<RoleDescriptor>> listener = (ActionListener<List<RoleDescriptor>>) args[1];
                    listener.onResponse(storeRoleDescriptors);
                    return null;
                }
            }).when(rolesStore).getRoleDescriptors(aryEq(request.names()), any(ActionListener.class));
        }

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetRolesResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetRolesResponse>() {
            @Override
            public void onResponse(GetRolesResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        List<String> retrievedRoleNames =
                Arrays.asList(responseRef.get().roles()).stream().map(RoleDescriptor::getName).collect(Collectors.toList());
        assertThat(retrievedRoleNames, containsInAnyOrder(request.names()));
    }

    public void testGetAllOrMix() {
        final boolean all = randomBoolean();
        final List<RoleDescriptor> storeRoleDescriptors = randomRoleDescriptors();
        final List<String> storeNames = storeRoleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toList());
        final List<String> reservedRoleNames = new ArrayList<>(ReservedRolesStore.names());

        final List<String> requestedNames = new ArrayList<>();
        List<String> specificStoreNames = new ArrayList<>();
        if (all == false) {
            requestedNames.addAll(randomSubsetOf(randomIntBetween(1, ReservedRolesStore.names().size()), ReservedRolesStore.names()));
            specificStoreNames.addAll(randomSubsetOf(randomIntBetween(1, storeNames.size()), storeNames));
            requestedNames.addAll(specificStoreNames);
        }

        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        SecurityContext context = mock(SecurityContext.class);
        TransportGetRolesAction action = new TransportGetRolesAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), rolesStore, mock(TransportService.class), new ReservedRolesStore(context));

        final boolean isKibanaUser = randomBoolean();
        final List<String> expectedNames = new ArrayList<>();
        if (all) {
            expectedNames.addAll(reservedRoleNames);
            expectedNames.addAll(storeNames);
        } else {
            expectedNames.addAll(requestedNames);
        }

        if (isKibanaUser) {
            when(context.getUser()).thenReturn(KibanaUser.INSTANCE);
        } else {
            expectedNames.remove(KibanaRole.NAME);
        }

        GetRolesRequest request = new GetRolesRequest();
        request.names(requestedNames.toArray(Strings.EMPTY_ARRAY));

        if (specificStoreNames.size() == 1) {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    String requestedName = (String) args[0];
                    ActionListener<RoleDescriptor> listener = (ActionListener<RoleDescriptor>) args[1];
                    Optional<RoleDescriptor> rd =
                            storeRoleDescriptors.stream().filter(r -> r.getName().equals(requestedName)).findFirst();
                    listener.onResponse(rd.get());
                    return null;
                }
            }).when(rolesStore).getRoleDescriptor(eq(specificStoreNames.get(0)), any(ActionListener.class));
        } else {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    String[] requestedNames = (String[]) args[0];
                    ActionListener<List<RoleDescriptor>> listener = (ActionListener<List<RoleDescriptor>>) args[1];
                    if (requestedNames.length == 0) {
                        listener.onResponse(storeRoleDescriptors);
                    } else {
                        List<String> requestedNamesList = Arrays.asList(requestedNames);
                        listener.onResponse(storeRoleDescriptors.stream()
                                .filter(r -> requestedNamesList.contains(r.getName()))
                                .collect(Collectors.toList()));
                    }
                    return null;
                }
            }).when(rolesStore).getRoleDescriptors(aryEq(specificStoreNames.toArray(Strings.EMPTY_ARRAY)), any(ActionListener.class));
        }

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetRolesResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetRolesResponse>() {
            @Override
            public void onResponse(GetRolesResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        List<String> retrievedRoleNames =
                Arrays.asList(responseRef.get().roles()).stream().map(RoleDescriptor::getName).collect(Collectors.toList());
        assertThat(retrievedRoleNames, containsInAnyOrder(expectedNames.toArray(Strings.EMPTY_ARRAY)));

        if (all) {
            verify(rolesStore, times(1)).getRoleDescriptors(aryEq(Strings.EMPTY_ARRAY), any(ActionListener.class));
        } else if (specificStoreNames.size() == 1) {
            verify(rolesStore, times(1)).getRoleDescriptor(eq(specificStoreNames.get(0)), any(ActionListener.class));
        } else {
            verify(rolesStore, times(1))
                    .getRoleDescriptors(aryEq(specificStoreNames.toArray(Strings.EMPTY_ARRAY)), any(ActionListener.class));
        }
    }

    public void testException() {
        final Throwable t = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException());
        final List<RoleDescriptor> storeRoleDescriptors = randomRoleDescriptors();
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        SecurityContext context = mock(SecurityContext.class);
        TransportGetRolesAction action = new TransportGetRolesAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), rolesStore, mock(TransportService.class), new ReservedRolesStore(context));

        GetRolesRequest request = new GetRolesRequest();
        request.names(storeRoleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY));

        if (request.names().length == 1) {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<RoleDescriptor> listener = (ActionListener<RoleDescriptor>) args[1];
                    listener.onFailure(t);
                    return null;
                }
            }).when(rolesStore).getRoleDescriptor(eq(request.names()[0]), any(ActionListener.class));
        } else {
            doAnswer(new Answer() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<List<RoleDescriptor>> listener = (ActionListener<List<RoleDescriptor>>) args[1];
                    listener.onFailure(t);
                    return null;
                }
            }).when(rolesStore).getRoleDescriptors(aryEq(request.names()), any(ActionListener.class));
        }

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetRolesResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetRolesResponse>() {
            @Override
            public void onResponse(GetRolesResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), is(t));
        assertThat(responseRef.get(), is(nullValue()));
    }

    private List<RoleDescriptor> randomRoleDescriptors() {
        int size = scaledRandomIntBetween(1, 10);
        List<RoleDescriptor> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new RoleDescriptor("role_" + i, null, null, null));
        }
        return list;
    }
}
