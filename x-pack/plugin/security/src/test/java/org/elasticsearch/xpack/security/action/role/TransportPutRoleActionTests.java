/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportPutRoleActionTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    (p, c) -> MatchAllQueryBuilder.fromXContent(p)
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HasChildQueryBuilder.NAME),
                    (p, c) -> HasChildQueryBuilder.fromXContent(p)
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HasParentQueryBuilder.NAME),
                    (p, c) -> HasParentQueryBuilder.fromXContent(p)
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(TermQueryBuilder.NAME),
                    (p, c) -> TermQueryBuilder.fromXContent(p)
                )
            )
        );
    }

    public void testReservedRole() {
        final String roleName = randomFrom(new ArrayList<>(ReservedRolesStore.names()));
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutRoleAction action = new TransportPutRoleAction(
            mock(ActionFilters.class),
            rolesStore,
            transportService,
            xContentRegistry()
        );

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
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutRoleAction action = new TransportPutRoleAction(
            mock(ActionFilters.class),
            rolesStore,
            transportService,
            xContentRegistry()
        );

        final boolean created = randomBoolean();
        PutRoleRequest request = new PutRoleRequest();
        request.name(roleName);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[2];
            listener.onResponse(created);
            return null;
        }).when(rolesStore).putRole(eq(request), any(RoleDescriptor.class), anyActionListener());

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
        verify(rolesStore, times(1)).putRole(eq(request), any(RoleDescriptor.class), anyActionListener());
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
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutRoleAction action = new TransportPutRoleAction(
            mock(ActionFilters.class),
            rolesStore,
            transportService,
            xContentRegistry()
        );

        PutRoleRequest request = new PutRoleRequest();
        request.name(roleName);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[2];
            listener.onFailure(e);
            return null;
        }).when(rolesStore).putRole(eq(request), any(RoleDescriptor.class), anyActionListener());

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
        verify(rolesStore, times(1)).putRole(eq(request), any(RoleDescriptor.class), anyActionListener());
    }

    public void testCreationOfRoleWithMalformedQueryJsonFails() {
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutRoleAction action = new TransportPutRoleAction(
            mock(ActionFilters.class),
            rolesStore,
            transportService,
            xContentRegistry()
        );
        PutRoleRequest request = new PutRoleRequest();
        request.name("test");
        String[] malformedQueryJson = new String[] {
            "{ \"match_all\": { \"unknown_field\": \"\" } }",
            "{ malformed JSON }",
            "{ \"unknown\": {\"\"} }",
            "{}" };
        BytesReference query = new BytesArray(randomFrom(malformedQueryJson));
        request.addIndex(new String[] { "idx1" }, new String[] { "read" }, null, null, query, randomBoolean());

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
        Throwable t = throwableRef.get();
        assertThat(t, instanceOf(ElasticsearchParseException.class));
        assertThat(
            t.getMessage(),
            containsString(
                "failed to parse field 'query' for indices ["
                    + Strings.arrayToCommaDelimitedString(new String[] { "idx1" })
                    + "] at index privilege [0] of role descriptor"
            )
        );
    }

    public void testCreationOfRoleWithUnsupportedQueryFails() throws Exception {
        NativeRolesStore rolesStore = mock(NativeRolesStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutRoleAction action = new TransportPutRoleAction(
            mock(ActionFilters.class),
            rolesStore,
            transportService,
            xContentRegistry()
        );
        PutRoleRequest request = new PutRoleRequest();
        request.name("test");
        String hasChildQuery = "{ \"has_child\": { \"type\": \"child\", \"query\": { \"match_all\": {} } } }";
        String hasParentQuery = "{ \"has_parent\": { \"parent_type\": \"parent\", \"query\": { \"match_all\": {} } } }";
        BytesReference query = new BytesArray(randomFrom(hasChildQuery, hasParentQuery));
        request.addIndex(new String[] { "idx1" }, new String[] { "read" }, null, null, query, randomBoolean());

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
        Throwable t = throwableRef.get();
        assertThat(t, instanceOf(ElasticsearchParseException.class));
        assertThat(
            t.getMessage(),
            containsString(
                "failed to parse field 'query' for indices ["
                    + Strings.arrayToCommaDelimitedString(new String[] { "idx1" })
                    + "] at index privilege [0] of role descriptor"
            )
        );
    }
}
