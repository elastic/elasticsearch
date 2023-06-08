/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
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
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
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
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AsyncSupplier;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizedIndices;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.ParentActionAuthorization;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.permission.ApplicationPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Map.entry;
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
        engine = new RBACEngine(Settings.EMPTY, rolesStore, new FieldPermissionsCache(Settings.EMPTY), timerFactory);
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
            new RequestInfo(
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

    public void testSameUserPermissionForCrossClusterAccess() {
        final CrossClusterAccessSubjectInfo ccaSubjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build().toCrossClusterAccess(ccaSubjectInfo);

        // HasPrivileges is allowed
        final HasPrivilegesRequest hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(ccaSubjectInfo.getAuthentication().getEffectiveSubject().getUser().principal());
        assertTrue(RBACEngine.checkSameUserPermissions(HasPrivilegesAction.NAME, hasPrivilegesRequest, authentication));

        // Other actions, e.g. GetUserPrivilegesAction, are not allowed even if they are allowed when performing within a single cluster
        final GetUserPrivilegesRequest getUserPrivilegesRequest = new GetUserPrivilegesRequestBuilder(mock(ElasticsearchClient.class))
            .username(ccaSubjectInfo.getAuthentication().getEffectiveSubject().getUser().principal())
            .request();
        assertFalse(RBACEngine.checkSameUserPermissions(GetUserPrivilegesAction.NAME, getUserPrivilegesRequest, authentication));
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
                    .addPrivileges(Map.of("create_doc", true, "index", true, "delete", true))
                    .build(),
                ResourcePrivileges.builder("apache-2017-01")
                    .addPrivileges(Map.of("create_doc", true, "index", true, "delete", false))
                    .build(),
                ResourcePrivileges.builder("other").addPrivileges(Map.of("create_doc", true, "index", false, "delete", false)).build()
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
                    .addPrivileges(Map.of("create_doc", true, "create", true, "index", true))
                    .build(),
                ResourcePrivileges.builder("apache-2017-01")
                    .addPrivileges(Map.of("create_doc", true, "create", true, "index", true))
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
            containsInAnyOrder(ResourcePrivileges.builder(prePatternPrefix).addPrivileges(Map.of("index", false)).build())
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
            containsInAnyOrder(ResourcePrivileges.builder(matchesPatternPrefix + "*").addPrivileges(Map.of("index", true)).build())
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
            containsInAnyOrder(ResourcePrivileges.builder(matchesPatternPrefix + "*").addPrivileges(Map.of("index", false)).build())
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
            containsInAnyOrder(ResourcePrivileges.builder(matchesPatternPrefix).addPrivileges(Map.of("index", true)).build())
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
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*").addPrivileges(Map.of("index", false)).build()
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
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*").addPrivileges(Map.of("index", false)).build()
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
            containsInAnyOrder(ResourcePrivileges.builder(restrictedIndexMatchingWildcard).addPrivileges(Map.of("index", false)).build())
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
            containsInAnyOrder(ResourcePrivileges.builder(matchesPatternPrefix + "*").addPrivileges(Map.of("index", true)).build())
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
                    .addPrivileges(Map.of("index", true, "monitor", false))
                    .build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(Map.of("index", restrictedIndexPermission, "monitor", restrictedMonitorPermission))
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
                    .addPrivileges(Map.of("index", true, "monitor", false))
                    .build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(Map.of("index", restrictedIndexPermission, "monitor", restrictedMonitorPermission))
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
                ResourcePrivileges.builder(".sec*").addPrivileges(Map.of("index", true, "monitor", false)).build(),
                ResourcePrivileges.builder(".security*").addPrivileges(Map.of("index", true, "monitor", true)).build()
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
                ResourcePrivileges.builder(".sec*").addPrivileges(Map.of("index", false, "monitor", false)).build(),
                ResourcePrivileges.builder(".security*").addPrivileges(Map.of("index", false, "monitor", true)).build()
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
                ResourcePrivileges.builder(".sec*").addPrivileges(Map.of("index", true, "monitor", false)).build(),
                ResourcePrivileges.builder(".security*").addPrivileges(Map.of("index", true, "monitor", true)).build()
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
                ResourcePrivileges.builder(".sec*").addPrivileges(Map.of("index", true, "monitor", false)).build(),
                ResourcePrivileges.builder(".security*").addPrivileges(Map.of("index", true, "monitor", false)).build()
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
                ResourcePrivileges.builder("foo/1").addPrivileges(Map.of("read", true, "write", false, "all", false)).build(),
                ResourcePrivileges.builder("foo/bar/2").addPrivileges(Map.of("read", true, "write", false, "all", false)).build(),
                ResourcePrivileges.builder("foo/bar/baz").addPrivileges(Map.of("read", true, "write", true, "all", true)).build(),
                ResourcePrivileges.builder("baz/bar/foo").addPrivileges(Map.of("read", false, "write", false, "all", false)).build()
            )
        );
        final Collection<ResourcePrivileges> app2 = response.getDetails().application().get("app2");
        assertThat(app2, Matchers.iterableWithSize(4));
        assertThat(
            Strings.collectionToCommaDelimitedString(app2),
            app2,
            containsInAnyOrder(
                ResourcePrivileges.builder("foo/1").addPrivileges(Map.of("read", false, "write", false, "all", false)).build(),
                ResourcePrivileges.builder("foo/bar/2").addPrivileges(Map.of("read", true, "write", true, "all", false)).build(),
                ResourcePrivileges.builder("foo/bar/baz").addPrivileges(Map.of("read", true, "write", true, "all", false)).build(),
                ResourcePrivileges.builder("baz/bar/foo").addPrivileges(Map.of("read", false, "write", true, "all", false)).build()
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
                        Map.ofEntries(
                            entry("DATA:read/user/*", true),
                            entry("ACTION:" + action1, true),
                            entry("ACTION:" + action2, false),
                            entry(action1, true),
                            entry(action2, false)
                        )
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
            .addApplicationPrivilege(ApplicationPrivilegeTests.createPrivilege("app01", "read", "data:read"), Collections.singleton("*"))
            .runAs(new Privilege(Sets.newHashSet("user01", "user02"), "user01", "user02"))
            .addRemoteGroup(Set.of("remote-1"), FieldPermissions.DEFAULT, null, IndexPrivilege.READ, false, "remote-index-1")
            .addRemoteGroup(
                Set.of("remote-2", "remote-3"),
                new FieldPermissions(new FieldPermissionsDefinition(new String[] { "public.*" }, new String[0])),
                Collections.singleton(query),
                IndexPrivilege.READ,
                randomBoolean(),
                "remote-index-2",
                "remote-index-3"
            )
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

        assertThat(response.getRemoteIndexPrivileges(), iterableWithSize(2));
        final GetUserPrivilegesResponse.RemoteIndices remoteIndex1 = findRemoteIndexPrivilege(
            response.getRemoteIndexPrivileges(),
            "remote-1"
        );
        assertThat(remoteIndex1.remoteClusters(), containsInAnyOrder("remote-1"));
        assertThat(remoteIndex1.indices().getIndices(), containsInAnyOrder("remote-index-1"));
        assertThat(remoteIndex1.indices().getPrivileges(), containsInAnyOrder("read"));
        assertThat(remoteIndex1.indices().getFieldSecurity(), emptyIterable());
        assertThat(remoteIndex1.indices().getQueries(), emptyIterable());
        final GetUserPrivilegesResponse.RemoteIndices remoteIndex2 = findRemoteIndexPrivilege(
            response.getRemoteIndexPrivileges(),
            "remote-2"
        );
        assertThat(remoteIndex2.remoteClusters(), containsInAnyOrder("remote-2", "remote-3"));
        assertThat(remoteIndex2.indices().getIndices(), containsInAnyOrder("remote-index-2", "remote-index-3"));
        assertThat(remoteIndex2.indices().getPrivileges(), containsInAnyOrder("read"));
        assertThat(
            remoteIndex2.indices().getFieldSecurity(),
            containsInAnyOrder(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "public.*" }, new String[0]))
        );
        assertThat(remoteIndex2.indices().getQueries(), containsInAnyOrder(query));
    }

    public void testBackingIndicesAreIncludedForAuthorizedDataStreams() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
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
        lookup.put(ds.getName(), ds);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.ConcreteIndex(im, ds));
        }

        SearchRequest request = new SearchRequest("*");
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(request, SearchAction.NAME),
            lookup,
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get(), hasItem(dataStreamName));
        assertThat(authorizedIndices.check(dataStreamName), is(true));
        assertThat(
            authorizedIndices.all().get(),
            hasItems(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY))
        );
        for (String index : backingIndices.stream().map(im -> im.getIndex().getName()).toList()) {
            assertThat(authorizedIndices.check(index), is(true));
        }
    }

    public void testExplicitMappingUpdatesAreNotGrantedWithIngestPrivileges() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
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
        lookup.put(ds.getName(), ds);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.ConcreteIndex(im, ds));
        }

        PutMappingRequest request = new PutMappingRequest("*");
        request.source("{ \"properties\": { \"message\": { \"type\": \"text\" } } }", XContentType.JSON);
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(request, PutMappingAction.NAME),
            lookup,
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get().isEmpty(), is(true));
    }

    public void testNoInfiniteRecursionForRBACAuthorizationInfoHashCode() {
        final Role role = Role.builder(RESTRICTED_INDICES, "role").build();
        // No assertion is needed, the test is successful as long as hashCode calls do not throw error
        new RBACAuthorizationInfo(role, Role.builder(RESTRICTED_INDICES, "authenticated_role").build()).hashCode();
        new RBACAuthorizationInfo(role, null).hashCode();
    }

    public void testGetUserPrivilegesThrowsIaeForUnsupportedOperation() {
        final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
        final Role role = mock(Role.class);
        when(authorizationInfo.getRole()).thenReturn(role);
        when(role.cluster()).thenReturn(ClusterPermission.NONE);
        when(role.indices()).thenReturn(IndicesPermission.NONE);
        when(role.application()).thenReturn(ApplicationPermission.NONE);
        when(role.runAs()).thenReturn(RunAsPermission.NONE);
        when(role.remoteIndices()).thenReturn(RemoteIndicesPermission.NONE);

        final UnsupportedOperationException unsupportedOperationException = new UnsupportedOperationException();
        switch (randomIntBetween(0, 4)) {
            case 0 -> when(role.cluster()).thenThrow(unsupportedOperationException);
            case 1 -> when(role.indices()).thenThrow(unsupportedOperationException);
            case 2 -> when(role.application()).thenThrow(unsupportedOperationException);
            case 3 -> when(role.runAs()).thenThrow(unsupportedOperationException);
            case 4 -> when(role.remoteIndices()).thenThrow(unsupportedOperationException);
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

    public void testGetRoleDescriptorsIntersectionForRemoteCluster() throws ExecutionException, InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final RemoteIndicesPermission.Builder remoteIndicesBuilder = RemoteIndicesPermission.builder();
        final String concreteClusterAlias = randomAlphaOfLength(10);
        final int numGroups = randomIntBetween(1, 3);
        final List<IndicesPrivileges> expectedIndicesPrivileges = new ArrayList<>();
        for (int i = 0; i < numGroups; i++) {
            final String[] indexNames = Objects.requireNonNull(generateRandomStringArray(3, 10, false, false));
            final boolean allowRestrictedIndices = randomBoolean();
            final boolean hasFls = randomBoolean();
            final FieldPermissionsDefinition.FieldGrantExcludeGroup group = hasFls
                ? randomFieldGrantExcludeGroup()
                : new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null);
            final BytesReference query = randomBoolean() ? randomDlsQuery() : null;
            final IndicesPrivileges.Builder builder = IndicesPrivileges.builder()
                .indices(Arrays.stream(indexNames).sorted().collect(Collectors.toList()))
                .privileges("read")
                .allowRestrictedIndices(allowRestrictedIndices)
                .query(query);
            if (hasFls) {
                builder.grantedFields(group.getGrantedFields());
                builder.deniedFields(group.getExcludedFields());
            }
            expectedIndicesPrivileges.add(builder.build());
            remoteIndicesBuilder.addGroup(
                Set.of(randomFrom(concreteClusterAlias, "*")),
                IndexPrivilege.READ,
                new FieldPermissions(new FieldPermissionsDefinition(Set.of(group))),
                query == null ? null : Set.of(query),
                allowRestrictedIndices,
                indexNames
            );
        }
        final String mismatchedConcreteClusterAlias = randomValueOtherThan(concreteClusterAlias, () -> randomAlphaOfLength(10));
        // Add some groups that don't match the alias
        final int numMismatchedGroups = randomIntBetween(0, 3);
        for (int i = 0; i < numMismatchedGroups; i++) {
            remoteIndicesBuilder.addGroup(
                Set.of(mismatchedConcreteClusterAlias),
                IndexPrivilege.READ,
                new FieldPermissions(
                    new FieldPermissionsDefinition(Set.of(new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null)))
                ),
                null,
                randomBoolean(),
                generateRandomStringArray(3, 10, false, false)
            );
        }

        final Role role = createSimpleRoleWithRemoteIndices(remoteIndicesBuilder.build());
        final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
        when(authorizationInfo.getRole()).thenReturn(role);

        final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
        engine.getRoleDescriptorsIntersectionForRemoteCluster(concreteClusterAlias, authorizationInfo, future);
        final RoleDescriptorsIntersection actual = future.get();

        assertThat(
            actual,
            equalTo(
                new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                expectedIndicesPrivileges.stream().sorted().toArray(RoleDescriptor.IndicesPrivileges[]::new),
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        )
                    )
                )
            )
        );
    }

    public void testGetRoleDescriptorsIntersectionForRemoteClusterHasDeterministicOrderForIndicesPrivileges() throws ExecutionException,
        InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final RemoteIndicesPermission.Builder remoteIndicesBuilder = RemoteIndicesPermission.builder();
        final String concreteClusterAlias = randomAlphaOfLength(10);
        final int numGroups = randomIntBetween(2, 5);
        for (int i = 0; i < numGroups; i++) {
            remoteIndicesBuilder.addGroup(
                Set.copyOf(randomNonEmptySubsetOf(List.of(concreteClusterAlias, "*"))),
                IndexPrivilege.get(Set.copyOf(randomSubsetOf(randomIntBetween(1, 4), IndexPrivilege.names()))),
                new FieldPermissions(
                    new FieldPermissionsDefinition(
                        Set.of(
                            randomBoolean()
                                ? randomFieldGrantExcludeGroup()
                                : new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null)
                        )
                    )
                ),
                randomBoolean() ? Set.of(randomDlsQuery()) : null,
                randomBoolean(),
                generateRandomStringArray(3, 10, false, false)
            );
        }
        final RemoteIndicesPermission permissions = remoteIndicesBuilder.build();
        List<RemoteIndicesPermission.RemoteIndicesGroup> remoteIndicesGroups = permissions.remoteIndicesGroups();
        final Role role1 = createSimpleRoleWithRemoteIndices(permissions);
        final RBACAuthorizationInfo authorizationInfo1 = mock(RBACAuthorizationInfo.class);
        when(authorizationInfo1.getRole()).thenReturn(role1);
        final PlainActionFuture<RoleDescriptorsIntersection> future1 = new PlainActionFuture<>();
        engine.getRoleDescriptorsIntersectionForRemoteCluster(concreteClusterAlias, authorizationInfo1, future1);
        final RoleDescriptorsIntersection actual1 = future1.get();

        // Randomize the order of both remote indices groups and each of the indices permissions groups each group holds
        final RemoteIndicesPermission shuffledPermissions = new RemoteIndicesPermission(
            shuffledList(
                remoteIndicesGroups.stream()
                    .map(
                        group -> new RemoteIndicesPermission.RemoteIndicesGroup(
                            group.remoteClusterAliases(),
                            shuffledList(group.indicesPermissionGroups())
                        )
                    )
                    .toList()
            )
        );
        final Role role2 = createSimpleRoleWithRemoteIndices(shuffledPermissions);
        final RBACAuthorizationInfo authorizationInfo2 = mock(RBACAuthorizationInfo.class);
        when(authorizationInfo2.getRole()).thenReturn(role2);
        final PlainActionFuture<RoleDescriptorsIntersection> future2 = new PlainActionFuture<>();
        engine.getRoleDescriptorsIntersectionForRemoteCluster(concreteClusterAlias, authorizationInfo2, future2);
        final RoleDescriptorsIntersection actual2 = future2.get();

        assertThat(actual1, equalTo(actual2));
        assertThat(actual1.roleDescriptorsList().iterator().next().iterator().next().getIndicesPrivileges().length, equalTo(numGroups));
    }

    public void testGetRoleDescriptorsIntersectionForRemoteClusterWithoutMatchingGroups() throws ExecutionException, InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final String concreteClusterAlias = randomAlphaOfLength(10);
        final Role role = createSimpleRoleWithRemoteIndices(
            RemoteIndicesPermission.builder()
                .addGroup(
                    Set.of(concreteClusterAlias),
                    IndexPrivilege.READ,
                    new FieldPermissions(new FieldPermissionsDefinition(null, null)),
                    null,
                    randomBoolean(),
                    generateRandomStringArray(3, 10, false, false)
                )
                .build()
        );
        final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
        when(authorizationInfo.getRole()).thenReturn(role);

        final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
        engine.getRoleDescriptorsIntersectionForRemoteCluster(
            randomValueOtherThan(concreteClusterAlias, () -> randomAlphaOfLength(10)),
            authorizationInfo,
            future
        );
        final RoleDescriptorsIntersection actual = future.get();
        assertThat(actual, equalTo(RoleDescriptorsIntersection.EMPTY));
    }

    public void testGetRoleDescriptorsIntersectionForRemoteClusterWithoutRemoteIndicesPermissions() throws ExecutionException,
        InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final String concreteClusterAlias = randomAlphaOfLength(10);
        final Role role = createSimpleRoleWithRemoteIndices(RemoteIndicesPermission.NONE);
        final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
        when(authorizationInfo.getRole()).thenReturn(role);

        final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
        engine.getRoleDescriptorsIntersectionForRemoteCluster(
            randomValueOtherThan(concreteClusterAlias, () -> randomAlphaOfLength(10)),
            authorizationInfo,
            future
        );
        final RoleDescriptorsIntersection actual = future.get();
        assertThat(actual, equalTo(RoleDescriptorsIntersection.EMPTY));
    }

    public void testGetRoleDescriptorsForRemoteClusterForReservedRoles() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        // superuser
        {
            final SimpleRole role = Role.buildFromRoleDescriptor(
                reservedRolesStore.roleDescriptor("superuser"),
                fieldPermissionsCache,
                RESTRICTED_INDICES
            );
            final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
            when(authorizationInfo.getRole()).thenReturn(role);
            final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
            engine.getRoleDescriptorsIntersectionForRemoteCluster(randomAlphaOfLengthBetween(5, 20), authorizationInfo, future);
            assertThat(
                future.actionGet(),
                equalTo(
                    new RoleDescriptorsIntersection(
                        new RoleDescriptor(
                            Role.REMOTE_USER_ROLE_NAME,
                            null,
                            new IndicesPrivileges[] {
                                IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(false).build(),
                                IndicesPrivileges.builder()
                                    .indices("*")
                                    .privileges("monitor", "read", "read_cross_cluster", "view_index_metadata")
                                    .allowRestrictedIndices(true)
                                    .build() },
                            null,
                            null,
                            null,
                            null,
                            null
                        )
                    )
                )
            );
        }

        // kibana_system
        {
            final SimpleRole role = Role.buildFromRoleDescriptor(
                reservedRolesStore.roleDescriptor("kibana_system"),
                fieldPermissionsCache,
                RESTRICTED_INDICES
            );
            final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
            when(authorizationInfo.getRole()).thenReturn(role);
            final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
            engine.getRoleDescriptorsIntersectionForRemoteCluster(randomAlphaOfLengthBetween(5, 20), authorizationInfo, future);
            assertThat(
                future.actionGet(),
                equalTo(
                    new RoleDescriptorsIntersection(
                        new RoleDescriptor(
                            Role.REMOTE_USER_ROLE_NAME,
                            null,
                            new IndicesPrivileges[] {
                                IndicesPrivileges.builder().indices(".monitoring-*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder().indices("apm-*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder().indices("logs-apm.*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder().indices("metrics-apm.*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder().indices("traces-apm-*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder().indices("traces-apm.*").privileges("read", "read_cross_cluster").build() },
                            null,
                            null,
                            null,
                            null,
                            null
                        )
                    )
                )
            );
        }

        // monitoring_user
        {
            final SimpleRole role = Role.buildFromRoleDescriptor(
                reservedRolesStore.roleDescriptor("monitoring_user"),
                fieldPermissionsCache,
                RESTRICTED_INDICES
            );
            final RBACAuthorizationInfo authorizationInfo = mock(RBACAuthorizationInfo.class);
            when(authorizationInfo.getRole()).thenReturn(role);
            final PlainActionFuture<RoleDescriptorsIntersection> future = new PlainActionFuture<>();
            engine.getRoleDescriptorsIntersectionForRemoteCluster(randomAlphaOfLengthBetween(5, 20), authorizationInfo, future);
            assertThat(
                future.actionGet(),
                equalTo(
                    new RoleDescriptorsIntersection(
                        new RoleDescriptor(
                            Role.REMOTE_USER_ROLE_NAME,
                            null,
                            new IndicesPrivileges[] {
                                IndicesPrivileges.builder().indices(".monitoring-*").privileges("read", "read_cross_cluster").build(),
                                IndicesPrivileges.builder()
                                    .indices("/metrics-(beats|elasticsearch|enterprisesearch|kibana|logstash).*/")
                                    .privileges("read", "read_cross_cluster")
                                    .build(),
                                IndicesPrivileges.builder().indices("metricbeat-*").privileges("read", "read_cross_cluster").build() },
                            null,
                            null,
                            null,
                            null,
                            null
                        )
                    )
                )
            );
        }
    }

    public void testChildSearchActionAuthorizationIsSkipped() {
        final String[] indices = { "test-index" };
        final Role role = Mockito.spy(Role.builder(RESTRICTED_INDICES, "test-role").add(IndexPrivilege.READ, indices).build());

        final String action = randomFrom(PreAuthorizationUtils.CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.get(SearchAction.NAME));
        final ParentActionAuthorization parentAuthorization = new ParentActionAuthorization(SearchAction.NAME);

        authorizeIndicesAction(indices, role, action, parentAuthorization, new ActionListener<IndexAuthorizationResult>() {
            @Override
            public void onResponse(IndexAuthorizationResult indexAuthorizationResult) {
                assertTrue(indexAuthorizationResult.isGranted());
                // Child authorization should be skipped since we passed parent authorization.
                Mockito.verify(role, never()).checkIndicesAction(action);
                Mockito.verify(role, never()).authorize(eq(action), any(), any(), any());
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    public void testChildSearchActionIsAuthorizedWithoutSkipping() {
        final String[] indices = { "test-index" };
        final Role role = Mockito.spy(Role.builder(RESTRICTED_INDICES, "test-role").add(IndexPrivilege.READ, indices).build());

        final String action = randomFrom(PreAuthorizationUtils.CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.get(SearchAction.NAME));
        final ParentActionAuthorization parentAuthorization = null;

        authorizeIndicesAction(indices, role, action, parentAuthorization, new ActionListener<IndexAuthorizationResult>() {
            @Override
            public void onResponse(IndexAuthorizationResult indexAuthorizationResult) {
                assertTrue(indexAuthorizationResult.isGranted());
                // Child action should have been authorized normally since we did not pass parent authorization
                Mockito.verify(role, atLeastOnce()).authorize(eq(action), any(), any(), any());
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    public void testChildSearchActionAuthorizationIsNotSkippedWhenRoleHasDLS() {
        final String[] indices = { "test-index" };
        final BytesArray query = new BytesArray("""
            {"term":{"foo":bar}}""");
        final Role role = Mockito.spy(
            Role.builder(RESTRICTED_INDICES, "test-role")
                .add(
                    new FieldPermissions(new FieldPermissionsDefinition(new String[] { "foo" }, new String[0])),
                    Set.of(query),
                    IndexPrivilege.READ,
                    randomBoolean(),
                    indices
                )
                .build()
        );

        final String action = randomFrom(PreAuthorizationUtils.CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.get(SearchAction.NAME));
        final ParentActionAuthorization parentAuthorization = new ParentActionAuthorization(SearchAction.NAME);

        authorizeIndicesAction(indices, role, action, parentAuthorization, new ActionListener<IndexAuthorizationResult>() {
            @Override
            public void onResponse(IndexAuthorizationResult indexAuthorizationResult) {
                assertTrue(indexAuthorizationResult.isGranted());
                // Child action authorization should not be skipped, even though the parent authorization was present
                Mockito.verify(role, atLeastOnce()).authorize(eq(action), any(), any(), any());
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    public void testRandomChildSearchActionAuthorizionIsNotSkipped() {
        final String[] indices = { "test-index" };
        final Role role = Mockito.spy(Role.builder(RESTRICTED_INDICES, "test-role").add(IndexPrivilege.READ, indices).build());

        final String action = SearchAction.NAME + "[" + randomAlphaOfLength(3) + "]";
        final ParentActionAuthorization parentAuthorization = new ParentActionAuthorization(SearchAction.NAME);

        authorizeIndicesAction(indices, role, action, parentAuthorization, new ActionListener<IndexAuthorizationResult>() {
            @Override
            public void onResponse(IndexAuthorizationResult indexAuthorizationResult) {
                assertTrue(indexAuthorizationResult.isGranted());
                Mockito.verify(role, atLeastOnce()).authorize(eq(action), any(), any(), any());
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    private void authorizeIndicesAction(
        final String[] indices,
        final Role role,
        final String action,
        final ParentActionAuthorization parentAuthorization,
        final ActionListener<IndexAuthorizationResult> listener
    ) {

        final RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);
        final ResolvedIndices resolvedIndices = new ResolvedIndices(List.of(indices), List.of());
        final TransportRequest searchRequest = new SearchRequest(indices);
        final RequestInfo requestInfo = createRequestInfo(searchRequest, action, parentAuthorization);
        final AsyncSupplier<ResolvedIndices> indicesAsyncSupplier = s -> s.onResponse(resolvedIndices);

        final Map<String, IndexAbstraction> aliasOrIndexLookup = Stream.of(indices)
            .collect(
                Collectors.toMap(
                    i -> i,
                    v -> new IndexAbstraction.ConcreteIndex(IndexMetadata.builder(v).settings(indexSettings(Version.CURRENT, 1, 0)).build())
                )
            );

        engine.authorizeIndexAction(requestInfo, authzInfo, indicesAsyncSupplier, aliasOrIndexLookup, listener);
    }

    private static RequestInfo createRequestInfo(TransportRequest request, String action, ParentActionAuthorization parentAuthorization) {
        final Authentication.RealmRef realm = new Authentication.RealmRef(
            randomAlphaOfLength(6),
            randomAlphaOfLength(4),
            "node0" + randomIntBetween(1, 9)
        );
        return new RequestInfo(
            AuthenticationTestHelper.builder().user(new User(randomAlphaOfLength(8))).realmRef(realm).build(false),
            request,
            action,
            null,
            parentAuthorization
        );
    }

    private GetUserPrivilegesResponse.Indices findIndexPrivilege(Set<GetUserPrivilegesResponse.Indices> indices, String name) {
        return indices.stream().filter(i -> i.getIndices().contains(name)).findFirst().get();
    }

    private GetUserPrivilegesResponse.RemoteIndices findRemoteIndexPrivilege(
        Set<GetUserPrivilegesResponse.RemoteIndices> remoteIndices,
        String remoteClusterAlias
    ) {
        return remoteIndices.stream().filter(i -> i.remoteClusters().contains(remoteClusterAlias)).findFirst().get();
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
        return ApplicationPrivilegeTests.createPrivilege(app, name, actions);
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

    private BytesArray randomDlsQuery() {
        return new BytesArray(
            "{ \"term\": { \"" + randomAlphaOfLengthBetween(3, 24) + "\" : \"" + randomAlphaOfLengthBetween(3, 24) + "\" }"
        );
    }

    private FieldPermissionsDefinition.FieldGrantExcludeGroup randomFieldGrantExcludeGroup() {
        return new FieldPermissionsDefinition.FieldGrantExcludeGroup(generateRandomStringArray(3, 10, false, false), new String[] {});
    }

    private Role createSimpleRoleWithRemoteIndices(final RemoteIndicesPermission remoteIndicesPermission) {
        final String[] roleNames = generateRandomStringArray(3, 10, false, false);
        Role.Builder roleBuilder = Role.builder(new RestrictedIndices(Automatons.EMPTY), roleNames);
        remoteIndicesPermission.remoteIndicesGroups().forEach(group -> {
            group.indicesPermissionGroups()
                .forEach(
                    p -> roleBuilder.addRemoteGroup(
                        group.remoteClusterAliases(),
                        p.getFieldPermissions(),
                        p.getQuery(),
                        p.privilege(),
                        p.allowRestrictedIndices(),
                        p.indices()
                    )
                );
        });
        return roleBuilder.build();
    }
}
