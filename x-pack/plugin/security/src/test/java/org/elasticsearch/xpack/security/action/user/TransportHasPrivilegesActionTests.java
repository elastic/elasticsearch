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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestLogging("org.elasticsearch.xpack.security.action.user.TransportHasPrivilegesAction:TRACE," +
        "org.elasticsearch.xpack.core.security.authz.permission.ApplicationPermission:DEBUG")
public class TransportHasPrivilegesActionTests extends ESTestCase {

    private User user;
    private Role role;
    private TransportHasPrivilegesAction action;
    private List<ApplicationPrivilegeDescriptor> applicationPrivileges;

    @Before
    public void setup() {
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

        applicationPrivileges = new ArrayList<>();
        NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        Mockito.doAnswer(inv -> {
            assertThat(inv.getArguments(), arrayWithSize(3));
            ActionListener<List<ApplicationPrivilegeDescriptor>> listener
                = (ActionListener<List<ApplicationPrivilegeDescriptor>>) inv.getArguments()[2];
            logger.info("Privileges for ({}) are {}", Arrays.toString(inv.getArguments()), applicationPrivileges);
            listener.onResponse(applicationPrivileges);
            return null;
        }).when(privilegeStore).getPrivileges(any(Collection.class), any(Collection.class), any(ActionListener.class));

        action = new TransportHasPrivilegesAction(threadPool, transportService, mock(ActionFilters.class), authorizationService,
            privilegeStore);
    }

    /**
     * This tests that action names in the request are considered "matched" by the relevant named privilege
     * (in this case that {@link DeleteAction} and {@link IndexAction} are satisfied by {@link IndexPrivilege#WRITE}).
     */
    public void testNamedIndexPrivilegesMatchApplicableActions() throws Exception {
        role = Role.builder("test1")
            .cluster(Collections.singleton("all"), Collections.emptyList())
            .add(IndexPrivilege.WRITE, "academy")
            .build();

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(ClusterHealthAction.NAME);
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices("academy")
                .privileges(DeleteAction.NAME, IndexAction.NAME)
                .build());
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.getUsername(), is(user.principal()));
        assertThat(response.isCompleteMatch(), is(true));

        assertThat(response.getClusterPrivileges().size(), equalTo(1));
        assertThat(response.getClusterPrivileges().get(ClusterHealthAction.NAME), equalTo(true));

        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        final ResourcePrivileges result = response.getIndexPrivileges().iterator().next();
        assertThat(result.getResource(), equalTo("academy"));
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
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.getUsername(), is(user.principal()));
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getClusterPrivileges().size(), equalTo(2));
        assertThat(response.getClusterPrivileges().get("monitor"), equalTo(true));
        assertThat(response.getClusterPrivileges().get("manage"), equalTo(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(3));

        final Iterator<ResourcePrivileges> indexPrivilegesIterator = response.getIndexPrivileges().iterator();
        final ResourcePrivileges academy = indexPrivilegesIterator.next();
        final ResourcePrivileges initiative = indexPrivilegesIterator.next();
        final ResourcePrivileges school = indexPrivilegesIterator.next();

        assertThat(academy.getResource(), equalTo("academy"));
        assertThat(academy.getPrivileges().size(), equalTo(3));
        assertThat(academy.getPrivileges().get("index"), equalTo(true)); // explicit
        assertThat(academy.getPrivileges().get("delete"), equalTo(false));
        assertThat(academy.getPrivileges().get("manage"), equalTo(false));

        assertThat(initiative.getResource(), equalTo("initiative"));
        assertThat(initiative.getPrivileges().size(), equalTo(3));
        assertThat(initiative.getPrivileges().get("index"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("delete"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("manage"), equalTo(false));

        assertThat(school.getResource(), equalTo("school"));
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
        assertThat(response.getUsername(), is(user.principal()));
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        final ResourcePrivileges result = response.getIndexPrivileges().iterator().next();
        assertThat(result.getResource(), equalTo("academy"));
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
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege("kibana", "read",
                "data:read/*", "action:login", "action:view/dashboard");
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege("kibana", "write",
                "data:write/*", "action:login", "action:view/dashboard");
        final ApplicationPrivilege kibanaAdmin = defineApplicationPrivilege("kibana", "admin",
                "action:login", "action:manage/*");
        final ApplicationPrivilege kibanaViewSpace = defineApplicationPrivilege("kibana", "view-space",
                "action:login", "space:view/*");
        role = Role.builder("test3")
                .add(IndexPrivilege.ALL, "logstash-*", "foo?")
                .add(IndexPrivilege.READ, "abc*")
                .add(IndexPrivilege.WRITE, "*xyz")
                .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
                .addApplicationPrivilege(kibanaViewSpace, newHashSet("space/engineering/*", "space/builds"))
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

        request.applicationPrivileges(
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .resources("*")
                        .application("kibana")
                        .privileges(Sets.union(kibanaRead.name(), kibanaWrite.name())) // read = Yes, write = No
                        .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .resources("space/engineering/project-*", "space/*") // project-* = Yes, space/* = Not
                        .application("kibana")
                        .privileges("space:view/dashboard")
                        .build()
        );

        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture();
        action.doExecute(mock(Task.class), request, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.getUsername(), is(user.principal()));
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(8));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                new ResourcePrivileges("logstash-2016-*", Collections.singletonMap("write", true)),
                new ResourcePrivileges("logstash-*", Collections.singletonMap("read", true)),
                new ResourcePrivileges("log*", Collections.singletonMap("manage", false)),
                new ResourcePrivileges("foo?", Collections.singletonMap("read", true)),
                new ResourcePrivileges("foo*", Collections.singletonMap("read", false)),
                new ResourcePrivileges("abcd*", mapBuilder().put("read", true).put("write", false).map()),
                new ResourcePrivileges("abc*xyz", mapBuilder().put("read", true).put("write", true).put("manage", false).map()),
                new ResourcePrivileges("a*xyz", mapBuilder().put("read", false).put("write", true).put("manage", false).map())
        ));
        assertThat(response.getApplicationPrivileges().entrySet(), Matchers.iterableWithSize(1));
        final Set<ResourcePrivileges> kibanaPrivileges = response.getApplicationPrivileges().get("kibana");
        assertThat(kibanaPrivileges, Matchers.iterableWithSize(3));
        assertThat(Strings.collectionToCommaDelimitedString(kibanaPrivileges), kibanaPrivileges, containsInAnyOrder(
                new ResourcePrivileges("*", mapBuilder().put("read", true).put("write", false).map()),
                new ResourcePrivileges("space/engineering/project-*", Collections.singletonMap("space:view/dashboard", true)),
                new ResourcePrivileges("space/*", Collections.singletonMap("space:view/dashboard", false))
        ));
    }

    private ApplicationPrivilege defineApplicationPrivilege(String app, String name, String ... actions) {
        this.applicationPrivileges.add(new ApplicationPrivilegeDescriptor(app, name, newHashSet(actions), emptyMap()));
        return new ApplicationPrivilege(app, name, actions);
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
                new ResourcePrivileges("apache-2016-12",
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).put("delete", true).map()),
                new ResourcePrivileges("apache-2017-01",
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).put("delete", false).map()
                )
        ));
    }

    public void testCheckingApplicationPrivilegesOnDifferentApplicationsAndResources() throws Exception {
        final ApplicationPrivilege app1Read = defineApplicationPrivilege("app1", "read", "data:read/*");
        final ApplicationPrivilege app1Write = defineApplicationPrivilege("app1", "write", "data:write/*");
        final ApplicationPrivilege app1All = defineApplicationPrivilege("app1", "all", "*");
        final ApplicationPrivilege app2Read = defineApplicationPrivilege("app2", "read", "data:read/*");
        final ApplicationPrivilege app2Write = defineApplicationPrivilege("app2", "write", "data:write/*");
        final ApplicationPrivilege app2All = defineApplicationPrivilege("app2", "all", "*");

        role = Role.builder("test-role")
                .addApplicationPrivilege(app1Read, Collections.singleton("foo/*"))
                .addApplicationPrivilege(app1All, Collections.singleton("foo/bar/baz"))
                .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
                .addApplicationPrivilege(app2Write, Collections.singleton("*/bar/*"))
                .build();

        final HasPrivilegesResponse response = hasPrivileges(new RoleDescriptor.IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[]{
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("app1")
                                .resources("foo/1", "foo/bar/2", "foo/bar/baz", "baz/bar/foo")
                                .privileges("read", "write", "all")
                                .build(),
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("app2")
                                .resources("foo/1", "foo/bar/2", "foo/bar/baz", "baz/bar/foo")
                                .privileges("read", "write", "all")
                                .build()
                }, Strings.EMPTY_ARRAY);

        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.emptyIterable());
        assertThat(response.getApplicationPrivileges().entrySet(), Matchers.iterableWithSize(2));
        final Set<ResourcePrivileges> app1 = response.getApplicationPrivileges().get("app1");
        assertThat(app1, Matchers.iterableWithSize(4));
        assertThat(Strings.collectionToCommaDelimitedString(app1), app1, containsInAnyOrder(
                new ResourcePrivileges("foo/1", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", true).put("write", false).put("all", false).map()),
                new ResourcePrivileges("foo/bar/2", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", true).put("write", false).put("all", false).map()),
                new ResourcePrivileges("foo/bar/baz", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", true).put("write", true).put("all", true).map()),
                new ResourcePrivileges("baz/bar/foo", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", false).put("write", false).put("all", false).map())
        ));
        final Set<ResourcePrivileges> app2 = response.getApplicationPrivileges().get("app2");
        assertThat(app2, Matchers.iterableWithSize(4));
        assertThat(Strings.collectionToCommaDelimitedString(app2), app2, containsInAnyOrder(
                new ResourcePrivileges("foo/1", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", false).put("write", false).put("all", false).map()),
                new ResourcePrivileges("foo/bar/2", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", true).put("write", true).put("all", false).map()),
                new ResourcePrivileges("foo/bar/baz", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", true).put("write", true).put("all", false).map()),
                new ResourcePrivileges("baz/bar/foo", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("read", false).put("write", true).put("all", false).map())
        ));
    }

    public void testCheckingApplicationPrivilegesWithComplexNames() throws Exception {
        final String appName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 10);
        final String action1 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 5);
        final String action2 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(6, 9);

        final ApplicationPrivilege priv1 = defineApplicationPrivilege(appName, action1, "DATA:read/*", "ACTION:" + action1);
        final ApplicationPrivilege priv2 = defineApplicationPrivilege(appName, action2, "DATA:read/*", "ACTION:" + action2);

        role = Role.builder("test-write")
            .addApplicationPrivilege(priv1, Collections.singleton("user/*/name"))
            .build();

        final HasPrivilegesResponse response = hasPrivileges(
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[]{
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(appName)
                    .resources("user/hawkeye/name")
                    .privileges("DATA:read/user/*", "ACTION:" + action1, "ACTION:" + action2, action1, action2)
                    .build()
            },
            "monitor");
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getApplicationPrivileges().keySet(), containsInAnyOrder(appName));
        assertThat(response.getApplicationPrivileges().get(appName), iterableWithSize(1));
        assertThat(response.getApplicationPrivileges().get(appName), containsInAnyOrder(
            new ResourcePrivileges("user/hawkeye/name", MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("DATA:read/user/*", true)
                .put("ACTION:" + action1, true)
                .put("ACTION:" + action2, false)
                .put(action1, true)
                .put(action2, false)
                .map())
        ));
    }

    public void testIsCompleteMatch() throws Exception {
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege("kibana", "read", "data:read/*");
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege("kibana", "write", "data:write/*");
        role = Role.builder("test-write")
                .cluster(ClusterPrivilege.MONITOR)
                .add(IndexPrivilege.READ, "read-*")
                .add(IndexPrivilege.ALL, "all-*")
                .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
                .build();

        assertThat(hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), "monitor").isCompleteMatch(), is(true));
        assertThat(hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), "manage").isCompleteMatch(), is(false));
        assertThat(hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), "monitor").isCompleteMatch(), is(false));
        assertThat(hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), "manage").isCompleteMatch(), is(false));
        assertThat(hasPrivileges(
                new RoleDescriptor.IndicesPrivileges[]{
                        RoleDescriptor.IndicesPrivileges.builder()
                                .indices("read-a")
                                .privileges("read")
                                .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                                .indices("all-b")
                                .privileges("read", "write")
                                .build()
                },
                new RoleDescriptor.ApplicationResourcePrivileges[]{
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("kibana")
                                .resources("*")
                                .privileges("read")
                                .build()
                },
                "monitor").isCompleteMatch(), is(true));
        assertThat(hasPrivileges(
                new RoleDescriptor.IndicesPrivileges[]{indexPrivileges("read", "read-123", "read-456", "all-999")},
                new RoleDescriptor.ApplicationResourcePrivileges[]{
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("kibana").resources("*").privileges("read").build(),
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("kibana").resources("*").privileges("write").build()
                },
                "monitor").isCompleteMatch(), is(false));
    }

    private RoleDescriptor.IndicesPrivileges indexPrivileges(String priv, String... indices) {
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(priv)
                .build();
    }

    private HasPrivilegesResponse hasPrivileges(RoleDescriptor.IndicesPrivileges indicesPrivileges, String... clusterPrivileges)
            throws Exception {
        return hasPrivileges(
                new RoleDescriptor.IndicesPrivileges[]{indicesPrivileges},
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                clusterPrivileges
        );
    }

    private HasPrivilegesResponse hasPrivileges(RoleDescriptor.IndicesPrivileges[] indicesPrivileges,
                                                RoleDescriptor.ApplicationResourcePrivileges[] appPrivileges,
                                                String... clusterPrivileges) throws Exception {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(clusterPrivileges);
        request.indexPrivileges(indicesPrivileges);
        request.applicationPrivileges(appPrivileges);
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
