/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse.IndexPrivileges;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collections;
import java.util.LinkedHashMap;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportHasPrivilegesActionTests extends ESTestCase {

    private User user;
    private Role role;
    private TransportHasPrivilegesAction action;

    @Before
    public void setup() {
        final Settings settings = Settings.builder().build();
        user = new User(randomAlphaOfLengthBetween(4, 12));
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());

        final Authentication authentication = mock(Authentication.class);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        when(authentication.getUser()).thenReturn(user);

        AuthorizationService authorizationService = mock(AuthorizationService.class);
        Mockito.doAnswer(invocationOnMock -> {
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[1];
            listener.onResponse(role);
            return null;
        }).when(authorizationService).roles(eq(user), any(ActionListener.class));

        action = new TransportHasPrivilegesAction(settings, threadPool, transportService, mock(ActionFilters.class), authorizationService);
    }

    /**
     * This tests that action names in the request are considered "matched" by the relevant named privilege
     * (in this case that {@link DeleteAction} and {@link IndexAction} are satisfied by {@link IndexPrivilege#WRITE}).
     */
    public void testNamedIndexPrivilegesMatchApplicableActions() throws Exception {
        role = Role.builder("test1").cluster(ClusterPrivilege.ALL).add(IndexPrivilege.WRITE, "academy").build();

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(ClusterHealthAction.NAME);
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices("academy")
                .privileges(DeleteAction.NAME, IndexAction.NAME)
                .build());
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.isCompleteMatch(), is(true));

        assertThat(response.getClusterPrivileges().size(), equalTo(1));
        assertThat(response.getClusterPrivileges().get(ClusterHealthAction.NAME), equalTo(true));

        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        final IndexPrivileges result = response.getIndexPrivileges().get(0);
        assertThat(result.getIndex(), equalTo("academy"));
        assertThat(result.getPrivileges().size(), equalTo(2));
        assertThat(result.getPrivileges().get(DeleteAction.NAME), equalTo(true));
        assertThat(result.getPrivileges().get(IndexAction.NAME), equalTo(true));
    }

    /**
     * This tests that the action responds correctly when the user/role has some, but not all
     * of the privileges being checked.
     */
    public void testMatchSubsetOfPrivileges() throws Exception {
        role = Role.builder("test2")
                .cluster(ClusterPrivilege.MONITOR)
                .add(IndexPrivilege.INDEX, "academy")
                .add(IndexPrivilege.WRITE, "initiative")
                .build();

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges("monitor", "manage");
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices("academy", "initiative", "school")
                .privileges("delete", "index", "manage")
                .build());
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getClusterPrivileges().size(), equalTo(2));
        assertThat(response.getClusterPrivileges().get("monitor"), equalTo(true));
        assertThat(response.getClusterPrivileges().get("manage"), equalTo(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(3));

        final IndexPrivileges academy = response.getIndexPrivileges().get(0);
        final IndexPrivileges initiative = response.getIndexPrivileges().get(1);
        final IndexPrivileges school = response.getIndexPrivileges().get(2);

        assertThat(academy.getIndex(), equalTo("academy"));
        assertThat(academy.getPrivileges().size(), equalTo(3));
        assertThat(academy.getPrivileges().get("index"), equalTo(true)); // explicit
        assertThat(academy.getPrivileges().get("delete"), equalTo(false));
        assertThat(academy.getPrivileges().get("manage"), equalTo(false));

        assertThat(initiative.getIndex(), equalTo("initiative"));
        assertThat(initiative.getPrivileges().size(), equalTo(3));
        assertThat(initiative.getPrivileges().get("index"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("delete"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("manage"), equalTo(false));

        assertThat(school.getIndex(), equalTo("school"));
        assertThat(school.getPrivileges().size(), equalTo(3));
        assertThat(school.getPrivileges().get("index"), equalTo(false));
        assertThat(school.getPrivileges().get("delete"), equalTo(false));
        assertThat(school.getPrivileges().get("manage"), equalTo(false));
    }

    /**
     * This tests that the action responds correctly when the user/role has none
     * of the privileges being checked.
     */
    public void testMatchNothing() throws Exception {
        role = Role.builder("test3")
                .cluster(ClusterPrivilege.MONITOR)
                .build();

        final HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices("academy")
                .privileges("read", "write")
                .build(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        final IndexPrivileges result = response.getIndexPrivileges().get(0);
        assertThat(result.getIndex(), equalTo("academy"));
        assertThat(result.getPrivileges().size(), equalTo(2));
        assertThat(result.getPrivileges().get("read"), equalTo(false));
        assertThat(result.getPrivileges().get("write"), equalTo(false));
    }

    /**
     * Wildcards in the request are treated as
     * <em>does the user have ___ privilege on every possible index that matches this pattern?</em>
     * Or, expressed differently,
     * <em>does the user have ___ privilege on a wildcard that covers (is a superset of) this pattern?</em>
     */
    public void testWildcardHandling() throws Exception {
        role = Role.builder("test3")
                .add(IndexPrivilege.ALL, "logstash-*", "foo?")
                .add(IndexPrivilege.READ, "abc*")
                .add(IndexPrivilege.WRITE, "*xyz")
                .build();

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(Strings.EMPTY_ARRAY);
        request.indexPrivileges(
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("logstash-2016-*")
                        .privileges("write") // Yes, because (ALL,"logstash-*")
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("logstash-*")
                        .privileges("read") // Yes, because (ALL,"logstash-*")
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("log*")
                        .privileges("manage") // No, because "log*" includes indices that "logstash-*" does not
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("foo*", "foo?")
                        .privileges("read") // Yes, "foo?", but not "foo*", because "foo*" > "foo?"
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("abcd*")
                        .privileges("read", "write") // read = Yes, because (READ, "abc*"), write = No
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("abc*xyz")
                        .privileges("read", "write", "manage") // read = Yes ( READ "abc*"), write = Yes (WRITE, "*xyz"), manage = No
                        .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("a*xyz")
                        .privileges("read", "write", "manage") // read = No, write = Yes (WRITE, "*xyz"), manage = No
                        .build()
        );
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(8));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                new IndexPrivileges("logstash-2016-*", Collections.singletonMap("write", true)),
                new IndexPrivileges("logstash-*", Collections.singletonMap("read", true)),
                new IndexPrivileges("log*", Collections.singletonMap("manage", false)),
                new IndexPrivileges("foo?", Collections.singletonMap("read", true)),
                new IndexPrivileges("foo*", Collections.singletonMap("read", false)),
                new IndexPrivileges("abcd*", mapBuilder().put("read", true).put("write", false).map()),
                new IndexPrivileges("abc*xyz", mapBuilder().put("read", true).put("write", true).put("manage", false).map()),
                new IndexPrivileges("a*xyz", mapBuilder().put("read", false).put("write", true).put("manage", false).map())
        ));
    }

    public void testCheckingIndexPermissionsDefinedOnDifferentPatterns() throws Exception {
        role = Role.builder("test-write")
                .add(IndexPrivilege.INDEX, "apache-*")
                .add(IndexPrivilege.DELETE, "apache-2016-*")
                .build();

        final HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices("apache-2016-12", "apache-2017-01")
                .privileges("index", "delete")
                .build(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                new IndexPrivileges("apache-2016-12",
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).put("delete", true).map()),
                new IndexPrivileges("apache-2017-01",
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).put("delete", false).map()
                )
        ));
    }

    public void testIsCompleteMatch() throws Exception {
        role = Role.builder("test-write")
                .cluster(ClusterPrivilege.MONITOR)
                .add(IndexPrivilege.READ, "read-*")
                .add(IndexPrivilege.ALL, "all-*")
                .build();

        assertThat(hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), "monitor").isCompleteMatch(), is(true));
        assertThat(hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), "manage").isCompleteMatch(), is(false));
        assertThat(hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), "monitor").isCompleteMatch(), is(false));
        assertThat(hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), "manage").isCompleteMatch(), is(false));
    }

    private RoleDescriptor.IndicesPrivileges indexPrivileges(String priv, String... indices) {
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(priv)
                .build();
    }

    private HasPrivilegesResponse hasPrivileges(RoleDescriptor.IndicesPrivileges indicesPrivileges, String... clusterPrivileges)
            throws Exception {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(clusterPrivileges);
        request.indexPrivileges(indicesPrivileges);
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);
        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        return response;
    }

    private static MapBuilder<String, Boolean> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }

}
