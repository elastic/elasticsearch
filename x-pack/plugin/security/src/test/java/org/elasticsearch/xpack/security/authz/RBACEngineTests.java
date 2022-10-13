/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.ApplicationPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.elasticsearch.xpack.security.authz.AuthorizedIndicesTests.getRequestInfo;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RBACEngineTests extends ESTestCase {

    private RBACEngine engine;
    private CompositeRolesStore rolesStore;

    @Before
    public void createEngine() {
        final LoadAuthorizedIndicesTimeChecker.Factory timerFactory = mock(LoadAuthorizedIndicesTimeChecker.Factory.class);
        when(timerFactory.newTimer(any())).thenReturn(LoadAuthorizedIndicesTimeChecker.NO_OP_CONSUMER);
        rolesStore = mock(CompositeRolesStore.class);
        engine = new RBACEngine(Settings.EMPTY, rolesStore, timerFactory);
    }

    public void testResolveAuthorizationInfoForEmptyRolesWithAuthentication() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Tuple<Role, Role>>) invocation.getArgument(1);
            listener.onResponse(new Tuple<>(Role.EMPTY, Role.EMPTY));
            return null;
        }).when(rolesStore).getRoles(any(), anyActionListener());

        final PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
        engine.resolveAuthorizationInfo(
            new AuthorizationEngine.RequestInfo(
                AuthenticationTestHelper.builder().build(),
                mock(TransportRequest.class),
                randomAlphaOfLengthBetween(20, 30),
                null
            ),
            future
        );

        final AuthorizationInfo authorizationInfo = future.actionGet();
        assertThat((String[]) authorizationInfo.asMap().get("user.roles"), emptyArray());
        assertThat((String[]) authorizationInfo.getAuthenticatedUserAuthorizationInfo().asMap().get("user.roles"), emptyArray());
    }

    public void testResolveAuthorizationInfoForEmptyRoleWithSubject() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Role>) invocation.getArgument(1);
            listener.onResponse(Role.EMPTY);
            return null;
        }).when(rolesStore).getRole(any(), anyActionListener());

        final PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
        engine.resolveAuthorizationInfo(AuthenticationTestHelper.builder().build().getEffectiveSubject(), future);

        final AuthorizationInfo authorizationInfo = future.actionGet();
        assertThat((String[]) authorizationInfo.asMap().get("user.roles"), emptyArray());
        assertThat((String[]) authorizationInfo.getAuthenticatedUserAuthorizationInfo().asMap().get("user.roles"), emptyArray());
    }

    public void testSameUserPermission() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest
            ? new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request()
            : new HasPrivilegesRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : HasPrivilegesAction.NAME;
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) : randomAlphaOfLengthBetween(4, 12),
            randomAlphaOfLengthBetween(3, 8)
        );
        final Authentication authentication = AuthenticationTestHelper.builder().realm().realmRef(authenticatedBy).user(user).build(false);

        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User authUser = new User("admin", "bar");
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest
            ? new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request()
            : new HasPrivilegesRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : HasPrivilegesAction.NAME;

        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(4, 12),
            randomAlphaOfLengthBetween(3, 8)
        );

        final Authentication.RealmRef lookedUpBy = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) : randomAlphaOfLengthBetween(4, 12),
            randomAlphaOfLengthBetween(3, 8)
        );

        final Authentication authentication = Authentication.newRealmAuthentication(authUser, authenticatedBy).runAs(user, lookedUpBy);

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));

        // this should still fail since the username is still different
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));

        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest) request).username("joe");
        } else {
            ((HasPrivilegesRequest) request).username("joe");
        }
        assertTrue(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionForAuthenticateRequest() {
        assertTrue(
            RBACEngine.checkSameUserPermissions(
                AuthenticateAction.NAME,
                AuthenticateRequest.INSTANCE,
                AuthenticationTestHelper.builder().build()
            )
        );
    }

    public void testSameUserPermissionDoesNotAllowOtherActions() {
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(
            PutUserAction.NAME,
            DeleteUserAction.NAME,
            ClusterHealthAction.NAME,
            ClusterStateAction.NAME,
            ClusterStatsAction.NAME,
            GetLicenseAction.NAME
        );
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));
        verifyNoMoreInteractions(request);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final User authUser = new User("admin", "bar");
        final String username = "joe";
        final User user = new User(username);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest
            ? new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request()
            : new HasPrivilegesRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;

        final Authentication.RealmRef authenticatedBy = AuthenticationTestHelper.randomRealmRef(false);
        final Authentication.RealmRef lookedUpBy = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) : randomAlphaOfLengthBetween(4, 12),
            randomAlphaOfLengthBetween(3, 8)
        );

        final Authentication authentication = Authentication.newRealmAuthentication(authUser, authenticatedBy).runAs(user, lookedUpBy);
        assertTrue(RBACEngine.checkSameUserPermissions(action, request, authentication));

        final Authentication authentication2 = Authentication.newRealmAuthentication(authUser, authenticatedBy);
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication2));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final Authentication authentication = AuthenticationTestHelper.builder()
            .realm()
            .realmRef(
                new Authentication.RealmRef(
                    randomAlphaOfLengthBetween(3, 8),
                    randomFrom(
                        LdapRealmSettings.LDAP_TYPE,
                        FileRealmSettings.TYPE,
                        LdapRealmSettings.AD_TYPE,
                        PkiRealmSettings.TYPE,
                        randomAlphaOfLengthBetween(4, 12)
                    ),
                    randomAlphaOfLengthBetween(3, 8)
                )
            )
            .build(false);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final String action = ChangePasswordAction.NAME;

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForApiKey() {
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build(false);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final String action = ChangePasswordAction.NAME;

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForAccessToken() {
        final Authentication authentication = AuthenticationTestHelper.builder().realm().build(false).token();
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final String action = ChangePasswordAction.NAME;

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final Authentication authentication = AuthenticationTestHelper.builder()
            .realm()
            .runAs()
            .realmRef(
                new Authentication.RealmRef(
                    randomAlphaOfLengthBetween(3, 8),
                    randomFrom(
                        LdapRealmSettings.LDAP_TYPE,
                        FileRealmSettings.TYPE,
                        LdapRealmSettings.AD_TYPE,
                        PkiRealmSettings.TYPE,
                        randomAlphaOfLengthBetween(4, 12)
                    ),
                    randomAlphaOfLengthBetween(3, 8)
                )
            )
            .build();
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final String action = ChangePasswordAction.NAME;

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(RBACEngine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionAllowsSelfApiKeyInfoRetrievalWhenAuthenticatedByApiKey() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(user, apiKeyId);
        final TransportRequest request = GetApiKeyRequest.builder().apiKeyId(apiKeyId).build();
        assertTrue(RBACEngine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    public void testSameUserPermissionDeniesSelfApiKeyInfoRetrievalWithLimitedByWhenAuthenticatedByApiKey() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(user, apiKeyId);
        final TransportRequest request = GetApiKeyRequest.builder().apiKeyId(apiKeyId).withLimitedBy(true).build();
        assertFalse(RBACEngine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    public void testSameUserPermissionDeniesApiKeyInfoRetrievalWhenAuthenticatedByADifferentApiKey() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final TransportRequest request = GetApiKeyRequest.builder().apiKeyId(apiKeyId).ownedByAuthenticatedUser(false).build();
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(user, randomAlphaOfLength(8));
        assertFalse(RBACEngine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    public void testSameUserPermissionDeniesApiKeyInfoRetrievalWhenLookedupByIsPresent() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final TransportRequest request = GetApiKeyRequest.builder().apiKeyId(apiKeyId).ownedByAuthenticatedUser(false).build();
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(new User("not-joe"), apiKeyId)
            .runAs(user, new Authentication.RealmRef("name", "type", randomAlphaOfLengthBetween(3, 8)));
        assertFalse(RBACEngine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    /**
     * This tests that action names in the request are considered "matched" by the relevant named privilege
     * (in this case that {@link DeleteAction} and {@link IndexAction} are satisfied by {@link IndexPrivilege#WRITE}).
     */
    public void testNamedIndexPrivilegesMatchApplicableActions() throws Exception {
        Role role = Role.builder(RESTRICTED_INDICES, "test1")
            .cluster(Collections.singleton("all"), Collections.emptyList())
            .add(IndexPrivilege.WRITE, "academy")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final PrivilegesCheckResult result = hasPrivileges(
            IndicesPrivileges.builder().indices("academy").privileges(DeleteAction.NAME, IndexAction.NAME).build(),
            authzInfo,
            List.of(),
            new String[] { ClusterHealthAction.NAME }
        );

        assertThat(result, notNullValue());
        assertThat(result.allChecksSuccess(), is(true));

        assertThat(result.getDetails().cluster(), aMapWithSize(1));
        assertThat(result.getDetails().cluster().get(ClusterHealthAction.NAME), equalTo(true));

        assertThat(result.getDetails().index().values(), Matchers.iterableWithSize(1));
        final ResourcePrivileges resourcePrivileges = result.getDetails().index().values().iterator().next();
        assertThat(resourcePrivileges.getResource(), equalTo("academy"));
        assertThat(resourcePrivileges.getPrivileges(), aMapWithSize(2));
        assertThat(resourcePrivileges.getPrivileges().get(DeleteAction.NAME), equalTo(true));
        assertThat(resourcePrivileges.getPrivileges().get(IndexAction.NAME), equalTo(true));
    }

    /**
     * This tests that the action responds correctly when the user/role has some, but not all
     * of the privileges being checked.
     */
    public void testMatchSubsetOfPrivileges() throws Exception {
        Role role = Role.builder(RESTRICTED_INDICES, "test2")
            .cluster(Set.of("monitor"), Set.of())
            .add(IndexPrivilege.INDEX, "academy")
            .add(IndexPrivilege.WRITE, "initiative")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder().indices("academy", "initiative", "school").privileges("delete", "index", "manage").build(),
            authzInfo,
            List.of(),
            new String[] { "monitor", "manage" }
        );

        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().cluster(), aMapWithSize(2));
        assertThat(response.getDetails().cluster().get("monitor"), equalTo(true));
        assertThat(response.getDetails().cluster().get("manage"), equalTo(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(3));

        final ResourcePrivileges academy = response.getDetails().index().get("academy");
        final ResourcePrivileges initiative = response.getDetails().index().get("initiative");
        final ResourcePrivileges school = response.getDetails().index().get("school");

        assertThat(academy.getResource(), equalTo("academy"));
        assertThat(academy.getPrivileges(), aMapWithSize(3));
        assertThat(academy.getPrivileges().get("index"), equalTo(true)); // explicit
        assertThat(academy.getPrivileges().get("delete"), equalTo(false));
        assertThat(academy.getPrivileges().get("manage"), equalTo(false));

        assertThat(initiative.getResource(), equalTo("initiative"));
        assertThat(initiative.getPrivileges(), aMapWithSize(3));
        assertThat(initiative.getPrivileges().get("index"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("delete"), equalTo(true)); // implied by write
        assertThat(initiative.getPrivileges().get("manage"), equalTo(false));

        assertThat(school.getResource(), equalTo("school"));
        assertThat(school.getPrivileges(), aMapWithSize(3));
        assertThat(school.getPrivileges().get("index"), equalTo(false));
        assertThat(school.getPrivileges().get("delete"), equalTo(false));
        assertThat(school.getPrivileges().get("manage"), equalTo(false));
    }

    /**
     * This tests that the action responds correctly when the user/role has none
     * of the privileges being checked.
     */
    public void testMatchNothing() throws Exception {
        Role role = Role.builder(RESTRICTED_INDICES, "test3").cluster(Set.of("monitor"), Set.of()).build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder().indices("academy").privileges("read", "write").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        final ResourcePrivileges result = response.getDetails().index().values().iterator().next();
        assertThat(result.getResource(), equalTo("academy"));
        assertThat(result.getPrivileges(), aMapWithSize(2));
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
        List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege(
            privs,
            "kibana",
            "read",
            "data:read/*",
            "action:login",
            "action:view/dashboard"
        );
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege(
            privs,
            "kibana",
            "write",
            "data:write/*",
            "action:login",
            "action:view/dashboard"
        );
        final ApplicationPrivilege kibanaAdmin = defineApplicationPrivilege(privs, "kibana", "admin", "action:login", "action:manage/*");
        final ApplicationPrivilege kibanaViewSpace = defineApplicationPrivilege(
            privs,
            "kibana",
            "view-space",
            "action:login",
            "space:view/*"
        );
        Role role = Role.builder(RESTRICTED_INDICES, "test3")
            .add(IndexPrivilege.ALL, "logstash-*", "foo?")
            .add(IndexPrivilege.READ, "abc*")
            .add(IndexPrivilege.WRITE, "*xyz")
            .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
            .addApplicationPrivilege(kibanaViewSpace, newHashSet("space/engineering/*", "space/builds"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final PrivilegesCheckResult response = hasPrivileges(
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("logstash-2016-*")
                    .privileges("write") // Yes, because (ALL,"logstash-*")
                    .build(),
                IndicesPrivileges.builder()
                    .indices("logstash-*")
                    .privileges("read") // Yes, because (ALL,"logstash-*")
                    .build(),
                IndicesPrivileges.builder()
                    .indices("log*")
                    .privileges("manage") // No, because "log*" includes indices that "logstash-*" does not
                    .build(),
                IndicesPrivileges.builder()
                    .indices("foo*", "foo?")
                    .privileges("read") // Yes, "foo?", but not "foo*", because "foo*" > "foo?"
                    .build(),
                IndicesPrivileges.builder()
                    .indices("abcd*")
                    .privileges("read", "write") // read = Yes, because (READ, "abc*"), write = No
                    .build(),
                IndicesPrivileges.builder()
                    .indices("abc*xyz")
                    .privileges("read", "write", "manage") // read = Yes ( READ "abc*"), write = Yes (WRITE, "*xyz"), manage = No
                    .build(),
                IndicesPrivileges.builder()
                    .indices("a*xyz")
                    .privileges("read", "write", "manage") // read = No, write = Yes (WRITE, "*xyz"), manage = No
                    .build() },
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder()
                    .resources("*")
                    .application("kibana")
                    .privileges(Sets.union(kibanaRead.name(), kibanaWrite.name())) // read = Yes, write = No
                    .build(),
                ApplicationResourcePrivileges.builder()
                    .resources("space/engineering/project-*", "space/*") // project-* = Yes, space/* = Not
                    .application("kibana")
                    .privileges("space:view/dashboard")
                    .build() },
            authzInfo,
            privs,
            new String[0]
        );

        assertThat(response, notNullValue());
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(8));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder("logstash-2016-*").addPrivileges(Collections.singletonMap("write", true)).build(),
                ResourcePrivileges.builder("logstash-*").addPrivileges(Collections.singletonMap("read", true)).build(),
                ResourcePrivileges.builder("log*").addPrivileges(Collections.singletonMap("manage", false)).build(),
                ResourcePrivileges.builder("foo?").addPrivileges(Collections.singletonMap("read", true)).build(),
                ResourcePrivileges.builder("foo*").addPrivileges(Collections.singletonMap("read", false)).build(),
                ResourcePrivileges.builder("abcd*").addPrivileges(mapBuilder().put("read", true).put("write", false).map()).build(),
                ResourcePrivileges.builder("abc*xyz")
                    .addPrivileges(mapBuilder().put("read", true).put("write", true).put("manage", false).map())
                    .build(),
                ResourcePrivileges.builder("a*xyz")
                    .addPrivileges(mapBuilder().put("read", false).put("write", true).put("manage", false).map())
                    .build()
            )
        );
        assertThat(response.getDetails().application().entrySet(), Matchers.iterableWithSize(1));
        final Collection<ResourcePrivileges> kibanaPrivileges = response.getDetails().application().get("kibana");
        assertThat(kibanaPrivileges, Matchers.iterableWithSize(3));
        assertThat(
            Strings.collectionToCommaDelimitedString(kibanaPrivileges),
            kibanaPrivileges,
            containsInAnyOrder(
                ResourcePrivileges.builder("*").addPrivileges(mapBuilder().put("read", true).put("write", false).map()).build(),
                ResourcePrivileges.builder("space/engineering/project-*")
                    .addPrivileges(Collections.singletonMap("space:view/dashboard", true))
                    .build(),
                ResourcePrivileges.builder("space/*").addPrivileges(Collections.singletonMap("space:view/dashboard", false)).build()
            )
        );
    }

    public void testCheckingIndexPermissionsDefinedOnDifferentPatterns() throws Exception {
        final RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(
            Role.builder(RESTRICTED_INDICES, "test-multiple")
                .add(IndexPrivilege.CREATE_DOC, "*")
                .add(IndexPrivilege.INDEX, "apache-*", "unrelated", "something_else*")
                .add(IndexPrivilege.DELETE, "apache-2016-*", ".security*")
                .build(),
            null
        );

        List<String> indices = new ArrayList<>(3);
        indices.add("apache-2016-12");
        indices.add("apache-2017-01");
        indices.add("other");
        Collections.shuffle(indices, random());
        List<String> privileges = new ArrayList<>(3);
        privileges.add("create_doc");
        privileges.add("index");
        privileges.add("delete");
        Collections.shuffle(privileges, random());
        PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder().indices(indices).privileges(privileges).allowRestrictedIndices(randomBoolean()).build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(3));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder("apache-2016-12")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new TreeMap<String, Boolean>())
                            .put("create_doc", true)
                            .put("index", true)
                            .put("delete", true)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("apache-2017-01")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new TreeMap<String, Boolean>())
                            .put("create_doc", true)
                            .put("index", true)
                            .put("delete", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("other")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new TreeMap<String, Boolean>())
                            .put("create_doc", true)
                            .put("index", false)
                            .put("delete", false)
                            .map()
                    )
                    .build()
            )
        );

        indices = new ArrayList<>(2);
        indices.add("apache-2016-12");
        indices.add("apache-2017-01");
        Collections.shuffle(indices, random());
        privileges = new ArrayList<>(3);
        privileges.add("create");
        privileges.add("create_doc");
        privileges.add("index");
        Collections.shuffle(privileges, random());
        response = hasPrivileges(
            IndicesPrivileges.builder().indices(indices).privileges(privileges).allowRestrictedIndices(randomBoolean()).build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(true));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder("apache-2016-12")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new TreeMap<String, Boolean>())
                            .put("create_doc", true)
                            .put("create", true)
                            .put("index", true)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("apache-2017-01")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new TreeMap<String, Boolean>())
                            .put("create_doc", true)
                            .put("create", true)
                            .put("index", true)
                            .map()
                    )
                    .build()
            )
        );
    }

    public void testCheckRestrictedIndexPatternPermission() throws Exception {
        final String patternPrefix = XPackPlugin.ASYNC_RESULTS_INDEX.substring(
            0,
            randomIntBetween(2, XPackPlugin.ASYNC_RESULTS_INDEX.length() - 2)
        );
        Role role = Role.builder(RESTRICTED_INDICES, "role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, false, patternPrefix + "*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        String prePatternPrefix = patternPrefix.substring(0, randomIntBetween(1, patternPrefix.length() - 1)) + "*";
        PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder().indices(prePatternPrefix).allowRestrictedIndices(randomBoolean()).privileges("index").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(prePatternPrefix)
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).map())
                    .build()
            )
        );

        String matchesPatternPrefix = XPackPlugin.ASYNC_RESULTS_INDEX.substring(0, patternPrefix.length() + 1);
        response = hasPrivileges(
            IndicesPrivileges.builder().indices(matchesPatternPrefix + "*").allowRestrictedIndices(false).privileges("index").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(true));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).map())
                    .build()
            )
        );
        response = hasPrivileges(
            IndicesPrivileges.builder().indices(matchesPatternPrefix + "*").allowRestrictedIndices(true).privileges("index").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).map())
                    .build()
            )
        );
        response = hasPrivileges(
            IndicesPrivileges.builder().indices(matchesPatternPrefix).allowRestrictedIndices(randomBoolean()).privileges("index").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(true));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix)
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).map())
                    .build()
            )
        );

        final String restrictedIndexMatchingWildcard = XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2);
        response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard + "*")
                .allowRestrictedIndices(true)
                .privileges("index")
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*")
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).map())
                    .build()
            )
        );
        response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard + "*")
                .allowRestrictedIndices(false)
                .privileges("index")
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*")
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).map())
                    .build()
            )
        );
        response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard)
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard)
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).map())
                    .build()
            )
        );

        role = Role.builder(RESTRICTED_INDICES, "role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, true, patternPrefix + "*")
            .build();
        authzInfo = new RBACAuthorizationInfo(role, null);
        response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(matchesPatternPrefix + "*")
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(true));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(1));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).map())
                    .build()
            )
        );
    }

    public void testCheckExplicitRestrictedIndexPermissions() throws Exception {
        final boolean restrictedIndexPermission = randomBoolean();
        final boolean restrictedMonitorPermission = randomBoolean();
        Role role = Role.builder(RESTRICTED_INDICES, "role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, restrictedIndexPermission, ".sec*")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, restrictedMonitorPermission, ".security*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        String explicitRestrictedIndex = randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(new String[] { ".secret-non-restricted", explicitRestrictedIndex })
                .privileges("index", "monitor")
                .allowRestrictedIndices(false) // explicit false for test
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".secret-non-restricted") // matches ".sec*" but not ".security*"
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("index", restrictedIndexPermission)
                            .put("monitor", restrictedMonitorPermission)
                            .map()
                    )
                    .build()
            )
        );

        explicitRestrictedIndex = randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        response = hasPrivileges(
            IndicesPrivileges.builder()
                .indices(new String[] { ".secret-non-restricted", explicitRestrictedIndex })
                .privileges("index", "monitor")
                .allowRestrictedIndices(true) // explicit true for test
                .build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".secret-non-restricted") // matches ".sec*" but not ".security*"
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("index", restrictedIndexPermission)
                            .put("monitor", restrictedMonitorPermission)
                            .map()
                    )
                    .build()
            )
        );
    }

    public void testCheckRestrictedIndexWildcardPermissions() throws Exception {
        Role role = Role.builder(RESTRICTED_INDICES, "role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, false, ".sec*")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, true, ".security*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        PrivilegesCheckResult response = hasPrivileges(
            IndicesPrivileges.builder().indices(".sec*", ".security*").privileges("index", "monitor").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".sec*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(".security*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", true).map()
                    )
                    .build()
            )
        );

        response = hasPrivileges(
            IndicesPrivileges.builder().indices(".sec*", ".security*").privileges("index", "monitor").allowRestrictedIndices(true).build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".sec*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(".security*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", false).put("monitor", true).map()
                    )
                    .build()
            )
        );

        role = Role.builder(RESTRICTED_INDICES, "role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, true, ".sec*")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, false, ".security*")
            .build();
        authzInfo = new RBACAuthorizationInfo(role, null);

        response = hasPrivileges(
            IndicesPrivileges.builder().indices(".sec*", ".security*").privileges("index", "monitor").build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".sec*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(".security*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", true).map()
                    )
                    .build()
            )
        );

        response = hasPrivileges(
            IndicesPrivileges.builder().indices(".sec*", ".security*").privileges("index", "monitor").allowRestrictedIndices(true).build(),
            authzInfo,
            Collections.emptyList(),
            Strings.EMPTY_ARRAY
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.iterableWithSize(2));
        assertThat(
            response.getDetails().index().values(),
            containsInAnyOrder(
                ResourcePrivileges.builder(".sec*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build(),
                ResourcePrivileges.builder(".security*")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>()).put("index", true).put("monitor", false).map()
                    )
                    .build()
            )
        );
    }

    public void testCheckingApplicationPrivilegesOnDifferentApplicationsAndResources() throws Exception {
        List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege app1Read = defineApplicationPrivilege(privs, "app1", "read", "data:read/*");
        final ApplicationPrivilege app1Write = defineApplicationPrivilege(privs, "app1", "write", "data:write/*");
        final ApplicationPrivilege app1All = defineApplicationPrivilege(privs, "app1", "all", "*");
        final ApplicationPrivilege app2Read = defineApplicationPrivilege(privs, "app2", "read", "data:read/*");
        final ApplicationPrivilege app2Write = defineApplicationPrivilege(privs, "app2", "write", "data:write/*");
        final ApplicationPrivilege app2All = defineApplicationPrivilege(privs, "app2", "all", "*");

        Role role = Role.builder(RESTRICTED_INDICES, "test-role")
            .addApplicationPrivilege(app1Read, Collections.singleton("foo/*"))
            .addApplicationPrivilege(app1All, Collections.singleton("foo/bar/baz"))
            .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
            .addApplicationPrivilege(app2Write, Collections.singleton("*/bar/*"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        List<String> resources = new ArrayList<>();
        resources.add("foo/1");
        resources.add("foo/bar/2");
        resources.add("foo/bar/baz");
        resources.add("baz/bar/foo");
        Collections.shuffle(resources, random());
        List<String> privileges = new ArrayList<>();
        privileges.add("read");
        privileges.add("write");
        privileges.add("all");
        Collections.shuffle(privileges, random());

        final PrivilegesCheckResult response = hasPrivileges(
            new IndicesPrivileges[0],
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder().application("app1").resources(resources).privileges(privileges).build(),
                ApplicationResourcePrivileges.builder().application("app2").resources(resources).privileges(privileges).build() },
            authzInfo,
            privs,
            Strings.EMPTY_ARRAY
        );

        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().index().values(), Matchers.emptyIterable());
        assertThat(response.getDetails().application().entrySet(), Matchers.iterableWithSize(2));
        final Collection<ResourcePrivileges> app1 = response.getDetails().application().get("app1");
        assertThat(app1, Matchers.iterableWithSize(4));
        assertThat(
            Strings.collectionToCommaDelimitedString(app1),
            app1,
            containsInAnyOrder(
                ResourcePrivileges.builder("foo/1")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", true)
                            .put("write", false)
                            .put("all", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("foo/bar/2")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", true)
                            .put("write", false)
                            .put("all", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("foo/bar/baz")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", true)
                            .put("write", true)
                            .put("all", true)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("baz/bar/foo")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", false)
                            .put("write", false)
                            .put("all", false)
                            .map()
                    )
                    .build()
            )
        );
        final Collection<ResourcePrivileges> app2 = response.getDetails().application().get("app2");
        assertThat(app2, Matchers.iterableWithSize(4));
        assertThat(
            Strings.collectionToCommaDelimitedString(app2),
            app2,
            containsInAnyOrder(
                ResourcePrivileges.builder("foo/1")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", false)
                            .put("write", false)
                            .put("all", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("foo/bar/2")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", true)
                            .put("write", true)
                            .put("all", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("foo/bar/baz")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", true)
                            .put("write", true)
                            .put("all", false)
                            .map()
                    )
                    .build(),
                ResourcePrivileges.builder("baz/bar/foo")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("read", false)
                            .put("write", true)
                            .put("all", false)
                            .map()
                    )
                    .build()
            )
        );
    }

    public void testCheckingApplicationPrivilegesWithComplexNames() throws Exception {
        final String appName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 10);
        final String action1 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 5);
        final String action2 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(6, 9);

        final List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege priv1 = defineApplicationPrivilege(privs, appName, action1, "DATA:read/*", "ACTION:" + action1);
        final ApplicationPrivilege priv2 = defineApplicationPrivilege(privs, appName, action2, "DATA:read/*", "ACTION:" + action2);

        Role role = Role.builder(RESTRICTED_INDICES, "test-write")
            .addApplicationPrivilege(priv1, Collections.singleton("user/*/name"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final PrivilegesCheckResult response = hasPrivileges(
            new IndicesPrivileges[0],
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder()
                    .application(appName)
                    .resources("user/hawkeye/name")
                    .privileges("DATA:read/user/*", "ACTION:" + action1, "ACTION:" + action2, action1, action2)
                    .build() },
            authzInfo,
            privs,
            "monitor"
        );
        assertThat(response.allChecksSuccess(), is(false));
        assertThat(response.getDetails().application().keySet(), containsInAnyOrder(appName));
        assertThat(response.getDetails().application().get(appName), iterableWithSize(1));
        assertThat(
            response.getDetails().application().get(appName),
            containsInAnyOrder(
                ResourcePrivileges.builder("user/hawkeye/name")
                    .addPrivileges(
                        MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                            .put("DATA:read/user/*", true)
                            .put("ACTION:" + action1, true)
                            .put("ACTION:" + action2, false)
                            .put(action1, true)
                            .put(action2, false)
                            .map()
                    )
                    .build()
            )
        );
    }

    public void testCheckPrivilegesWithCache() throws Exception {
        final List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final String appName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 10);
        final String privilegeName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 10);
        final String action1 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 5);
        final ApplicationPrivilege priv1 = defineApplicationPrivilege(privs, appName, privilegeName, "DATA:read/*", "ACTION:" + action1);
        SimpleRole role = spy(
            Role.builder(RESTRICTED_INDICES, "test-write").addApplicationPrivilege(priv1, Collections.singleton("user/*/name")).build()
        );
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        // 1st check privileges
        final PrivilegesToCheck privilegesToCheck1 = new PrivilegesToCheck(
            new String[0],
            new IndicesPrivileges[0],
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder()
                    .application(appName)
                    .resources("user/hawkeye/name")
                    .privileges("DATA:read/user/*", "ACTION:" + action1)
                    .build() },
            randomBoolean()
        );

        final PlainActionFuture<PrivilegesCheckResult> future1 = new PlainActionFuture<>();
        engine.checkPrivileges(authzInfo, privilegesToCheck1, privs, future1);
        final PrivilegesCheckResult privilegesCheckResult1 = future1.actionGet();

        // Result should be cached
        verify(role).cacheHasPrivileges(any(), eq(privilegesToCheck1), eq(privilegesCheckResult1));

        // Stall the check so that we are sure cache is used
        final RuntimeException stallCheckException = new RuntimeException("you shall not pass");
        doThrow(stallCheckException).when(role).checkApplicationResourcePrivileges(anyString(), any(), any(), any(), any());
        Mockito.clearInvocations(role);

        final PlainActionFuture<PrivilegesCheckResult> future2 = new PlainActionFuture<>();
        engine.checkPrivileges(authzInfo, privilegesToCheck1, privs, future2);
        final PrivilegesCheckResult privilegesCheckResult2 = future2.actionGet();

        assertThat(privilegesCheckResult2, is(privilegesCheckResult1));
        // Cached result won't be cached again
        verify(role, never()).cacheHasPrivileges(any(), any(), any());

        // Test a new check does not go through cache (and hence will be stalled by the exception)
        final PrivilegesToCheck privilegesToCheck2 = new PrivilegesToCheck(
            new String[0],
            new IndicesPrivileges[0],
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder()
                    .application(appName)
                    .resources("user/hawkeye/name")
                    .privileges("DATA:read/user/*")
                    .build() },
            randomBoolean()
        );
        final RuntimeException e1 = expectThrows(
            RuntimeException.class,
            () -> engine.checkPrivileges(authzInfo, privilegesToCheck2, privs, new PlainActionFuture<>())
        );
        assertThat(e1, is(stallCheckException));
    }

    public void testIsCompleteMatch() throws Exception {
        final List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege(privs, "kibana", "read", "data:read/*");
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege(privs, "kibana", "write", "data:write/*");
        Role role = Role.builder(RESTRICTED_INDICES, "test-write")
            .cluster(Set.of("monitor"), Set.of())
            .add(IndexPrivilege.READ, "read-*")
            .add(IndexPrivilege.ALL, "all-*")
            .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        assertThat(
            hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), authzInfo, privs, "monitor").allChecksSuccess(),
            is(true)
        );
        assertThat(
            hasPrivileges(indexPrivileges("read", "read-123", "read-456", "all-999"), authzInfo, privs, "manage").allChecksSuccess(),
            is(false)
        );
        assertThat(
            hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), authzInfo, privs, "monitor").allChecksSuccess(),
            is(false)
        );
        assertThat(
            hasPrivileges(indexPrivileges("write", "read-123", "read-456", "all-999"), authzInfo, privs, "manage").allChecksSuccess(),
            is(false)
        );
        assertThat(
            hasPrivileges(
                new IndicesPrivileges[] {
                    IndicesPrivileges.builder().indices("read-a").privileges("read").build(),
                    IndicesPrivileges.builder().indices("all-b").privileges("read", "write").build() },
                new ApplicationResourcePrivileges[] {
                    ApplicationResourcePrivileges.builder().application("kibana").resources("*").privileges("read").build() },
                authzInfo,
                privs,
                "monitor"
            ).allChecksSuccess(),
            is(true)
        );
        assertThat(
            hasPrivileges(
                new IndicesPrivileges[] { indexPrivileges("read", "read-123", "read-456", "all-999") },
                new ApplicationResourcePrivileges[] {
                    ApplicationResourcePrivileges.builder().application("kibana").resources("*").privileges("read").build(),
                    ApplicationResourcePrivileges.builder().application("kibana").resources("*").privileges("write").build() },
                authzInfo,
                privs,
                "monitor"
            ).allChecksSuccess(),
            is(false)
        );
    }

    public void testBuildUserPrivilegeResponse() {
        final ManageApplicationPrivileges manageApplicationPrivileges = new ManageApplicationPrivileges(Sets.newHashSet("app01", "app02"));
        final BytesArray query = new BytesArray("""
            {"term":{"public":true}}""");
        final Role role = Role.builder(RESTRICTED_INDICES, "test", "role")
            .cluster(Sets.newHashSet("monitor", "manage_watcher"), Collections.singleton(manageApplicationPrivileges))
            .add(IndexPrivilege.get(Sets.newHashSet("read", "write")), "index-1")
            .add(IndexPrivilege.ALL, "index-2", "index-3")
            .add(
                new FieldPermissions(new FieldPermissionsDefinition(new String[] { "public.*" }, new String[0])),
                Collections.singleton(query),
                IndexPrivilege.READ,
                randomBoolean(),
                "index-4",
                "index-5"
            )
            .addApplicationPrivilege(new ApplicationPrivilege("app01", "read", "data:read"), Collections.singleton("*"))
            .runAs(new Privilege(Sets.newHashSet("user01", "user02"), "user01", "user02"))
            .build();

        final GetUserPrivilegesResponse response = RBACEngine.buildUserPrivilegesResponseObject(role);

        assertThat(response.getClusterPrivileges(), containsInAnyOrder("monitor", "manage_watcher"));
        assertThat(response.getConditionalClusterPrivileges(), containsInAnyOrder(manageApplicationPrivileges));

        assertThat(response.getIndexPrivileges(), iterableWithSize(3));
        final GetUserPrivilegesResponse.Indices index1 = findIndexPrivilege(response.getIndexPrivileges(), "index-1");
        assertThat(index1.getIndices(), containsInAnyOrder("index-1"));
        assertThat(index1.getPrivileges(), containsInAnyOrder("read", "write"));
        assertThat(index1.getFieldSecurity(), emptyIterable());
        assertThat(index1.getQueries(), emptyIterable());
        final GetUserPrivilegesResponse.Indices index2 = findIndexPrivilege(response.getIndexPrivileges(), "index-2");
        assertThat(index2.getIndices(), containsInAnyOrder("index-2", "index-3"));
        assertThat(index2.getPrivileges(), containsInAnyOrder("all"));
        assertThat(index2.getFieldSecurity(), emptyIterable());
        assertThat(index2.getQueries(), emptyIterable());
        final GetUserPrivilegesResponse.Indices index4 = findIndexPrivilege(response.getIndexPrivileges(), "index-4");
        assertThat(index4.getIndices(), containsInAnyOrder("index-4", "index-5"));
        assertThat(index4.getPrivileges(), containsInAnyOrder("read"));
        assertThat(
            index4.getFieldSecurity(),
            containsInAnyOrder(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "public.*" }, new String[0]))
        );
        assertThat(index4.getQueries(), containsInAnyOrder(query));

        assertThat(
            response.getApplicationPrivileges(),
            containsInAnyOrder(ApplicationResourcePrivileges.builder().application("app01").privileges("read").resources("*").build())
        );

        assertThat(response.getRunAs(), containsInAnyOrder("user01", "user02"));
    }

    public void testBackingIndicesAreIncludedForAuthorizedDataStreams() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = AuthenticationTestHelper.builder().user(user).build();
        Role role = Role.builder(RESTRICTED_INDICES, "test1")
            .cluster(Collections.singleton("all"), Collections.emptyList())
            .add(IndexPrivilege.READ, dataStreamName)
            .build();

        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        List<IndexMetadata> backingIndices = new ArrayList<>();
        int numBackingIndices = randomIntBetween(1, 3);
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices.add(DataStreamTestHelper.createBackingIndex(dataStreamName, k + 1).build());
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        IndexAbstraction.DataStream iads = new IndexAbstraction.DataStream(ds);
        lookup.put(ds.getName(), iads);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.ConcreteIndex(im, iads));
        }

        SearchRequest request = new SearchRequest("*");
        Set<String> authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(request, SearchAction.NAME),
            lookup
        );
        // The authorized indices is the lazily loading set implementation
        assertThat(authorizedIndices, instanceOf(RBACEngine.AuthorizedIndicesSet.class));
        assertThat(authorizedIndices, hasItem(dataStreamName));
        assertThat(
            authorizedIndices,
            hasItems(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY))
        );
    }

    public void testExplicitMappingUpdatesAreNotGrantedWithIngestPrivileges() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = AuthenticationTestHelper.builder().user(user).build();
        Role role = Role.builder(RESTRICTED_INDICES, "test1")
            .cluster(Collections.emptySet(), Collections.emptyList())
            .add(IndexPrivilege.CREATE, "my_*")
            .add(IndexPrivilege.WRITE, "my_data*")
            .build();

        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        List<IndexMetadata> backingIndices = new ArrayList<>();
        int numBackingIndices = randomIntBetween(1, 3);
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices.add(DataStreamTestHelper.createBackingIndex(dataStreamName, k + 1).build());
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        IndexAbstraction.DataStream iads = new IndexAbstraction.DataStream(ds);
        lookup.put(ds.getName(), iads);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.ConcreteIndex(im, iads));
        }

        PutMappingRequest request = new PutMappingRequest("*");
        request.source("{ \"properties\": { \"message\": { \"type\": \"text\" } } }", XContentType.JSON);
        Set<String> authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(request, PutMappingAction.NAME),
            lookup
        );
        // The authorized indices is the lazily loading set implementation
        assertThat(authorizedIndices, instanceOf(RBACEngine.AuthorizedIndicesSet.class));
        assertThat(authorizedIndices.isEmpty(), is(true));
    }

    public void testNoInfiniteRecursionForRBACAuthorizationInfoHashCode() {
        final Role role = Role.builder(RESTRICTED_INDICES, "role").build();
        // No assertion is needed, the test is successful as long as hashCode calls do not throw error
        new RBACAuthorizationInfo(role, Role.builder(RESTRICTED_INDICES, "authenticated_role").build()).hashCode();
        new RBACAuthorizationInfo(role, null).hashCode();
    }

    @SuppressWarnings("unchecked")
    public void testLazinessForAuthorizedIndicesSet() {
        final Set<String> authorizedNames = Set.of("foo", "bar", "baz");
        final HashSet<String> allNames = new HashSet<>(authorizedNames);
        allNames.addAll(Set.of("buzz", "fiz"));

        final Supplier<Set<String>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(authorizedNames);
        final Predicate<String> predicate = mock(Predicate.class);
        doAnswer(invocation -> {
            final String name = (String) invocation.getArguments()[0];
            return authorizedNames.contains(name);
        }).when(predicate).test(anyString());
        final RBACEngine.AuthorizedIndicesSet authorizedIndicesSet = new RBACEngine.AuthorizedIndicesSet(supplier, predicate);

        // Check with contains or containsAll do not trigger loading
        final String name1 = randomFrom(allNames);
        final String name2 = randomValueOtherThan(name1, () -> randomFrom(allNames));
        final boolean containsAll = randomBoolean();
        if (containsAll) {
            assertThat(authorizedIndicesSet.containsAll(Set.of(name1, name2)), equalTo(authorizedNames.containsAll(Set.of(name1, name2))));
        } else {
            assertThat(authorizedIndicesSet.contains(name1), equalTo(authorizedNames.contains(name1)));
        }
        verify(supplier, never()).get();
        verify(predicate, atLeastOnce()).test(anyString());

        // Iterating through the set triggers loading
        Mockito.clearInvocations(predicate);
        final Set<String> collectedNames = new HashSet<>();
        for (String name : authorizedIndicesSet) {
            collectedNames.add(name);
        }
        verify(supplier).get();
        assertThat(collectedNames, equalTo(authorizedNames));

        // Check with contains and containsAll again now uses the loaded set not the predicate anymore
        Mockito.clearInvocations(supplier);
        if (containsAll) {
            assertThat(authorizedIndicesSet.containsAll(Set.of(name1, name2)), equalTo(authorizedNames.containsAll(Set.of(name1, name2))));
        } else {
            assertThat(authorizedIndicesSet.contains(name1), equalTo(authorizedNames.contains(name1)));
        }
        verify(predicate, never()).test(anyString());
        // It also does not load twice
        verify(supplier, never()).get();
    }

    public void testGetUserPrivilegesThrowsIaeForUnsupportedOperation() {
        final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
        final Role role = mock(Role.class);
        when(authorizationInfo.getRole()).thenReturn(role);
        when(role.cluster()).thenReturn(ClusterPermission.NONE);
        when(role.indices()).thenReturn(IndicesPermission.NONE);
        when(role.application()).thenReturn(ApplicationPermission.NONE);
        when(role.runAs()).thenReturn(RunAsPermission.NONE);

        final UnsupportedOperationException unsupportedOperationException = new UnsupportedOperationException();
        switch (randomIntBetween(0, 3)) {
            case 0 -> when(role.cluster()).thenThrow(unsupportedOperationException);
            case 1 -> when(role.indices()).thenThrow(unsupportedOperationException);
            case 2 -> when(role.application()).thenThrow(unsupportedOperationException);
            case 3 -> when(role.runAs()).thenThrow(unsupportedOperationException);
            default -> throw new IllegalStateException("unknown case number");
        }

        final PlainActionFuture<GetUserPrivilegesResponse> future = new PlainActionFuture<>();
        engine.getUserPrivileges(authorizationInfo, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);

        assertThat(
            e.getMessage(),
            equalTo(
                "Cannot retrieve privileges for API keys with assigned role descriptors. "
                    + "Please use the Get API key information API https://ela.st/es-api-get-api-key"
            )
        );
        assertThat(e.getCause(), sameInstance(unsupportedOperationException));
    }

    private GetUserPrivilegesResponse.Indices findIndexPrivilege(Set<GetUserPrivilegesResponse.Indices> indices, String name) {
        return indices.stream().filter(i -> i.getIndices().contains(name)).findFirst().get();
    }

    private IndicesPrivileges indexPrivileges(String priv, String... indices) {
        return IndicesPrivileges.builder().indices(indices).privileges(priv).build();
    }

    private ApplicationPrivilege defineApplicationPrivilege(
        List<ApplicationPrivilegeDescriptor> privs,
        String app,
        String name,
        String... actions
    ) {
        privs.add(new ApplicationPrivilegeDescriptor(app, name, newHashSet(actions), emptyMap()));
        return new ApplicationPrivilege(app, name, actions);
    }

    private PrivilegesCheckResult hasPrivileges(
        IndicesPrivileges indicesPrivileges,
        AuthorizationInfo authorizationInfo,
        List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
        String... clusterPrivileges
    ) throws Exception {
        return hasPrivileges(
            new IndicesPrivileges[] { indicesPrivileges },
            new ApplicationResourcePrivileges[0],
            authorizationInfo,
            applicationPrivilegeDescriptors,
            clusterPrivileges
        );
    }

    private PrivilegesCheckResult hasPrivileges(
        IndicesPrivileges[] indicesPrivileges,
        ApplicationResourcePrivileges[] appPrivileges,
        AuthorizationInfo authorizationInfo,
        List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
        String... clusterPrivileges
    ) throws Exception {
        final PlainActionFuture<PrivilegesCheckResult> future = new PlainActionFuture<>();
        final PlainActionFuture<PrivilegesCheckResult> future2 = new PlainActionFuture<>();
        final PrivilegesToCheck privilegesToCheck = new PrivilegesToCheck(
            clusterPrivileges,
            indicesPrivileges,
            appPrivileges,
            randomBoolean()
        );
        engine.checkPrivileges(authorizationInfo, privilegesToCheck, applicationPrivilegeDescriptors, future);
        // flip the "runDetailedCheck" flag
        engine.checkPrivileges(
            authorizationInfo,
            new PrivilegesToCheck(
                privilegesToCheck.cluster(),
                privilegesToCheck.index(),
                privilegesToCheck.application(),
                false == privilegesToCheck.runDetailedCheck()
            ),
            applicationPrivilegeDescriptors,
            future2
        );

        final PrivilegesCheckResult privilegesCheckResult = future.get();
        assertThat(privilegesCheckResult, notNullValue());
        final PrivilegesCheckResult privilegesCheckResult2 = future2.get();
        assertThat(privilegesCheckResult2, notNullValue());

        // same result independent of the "runDetailedCheck" flag
        assertThat(privilegesCheckResult.allChecksSuccess(), is(privilegesCheckResult2.allChecksSuccess()));

        if (privilegesToCheck.runDetailedCheck()) {
            assertThat(privilegesCheckResult.getDetails(), notNullValue());
            assertThat(privilegesCheckResult2.getDetails(), nullValue());
            return privilegesCheckResult;
        } else {
            assertThat(privilegesCheckResult.getDetails(), nullValue());
            assertThat(privilegesCheckResult2.getDetails(), notNullValue());
            return privilegesCheckResult2;
        }
    }

    private static MapBuilder<String, Boolean> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }
}
