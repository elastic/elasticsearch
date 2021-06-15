/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.security.authz.AuthorizedIndicesTests.getRequestInfo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RBACEngineTests extends ESTestCase {

    private RBACEngine engine;

    @Before
    public void createEngine() {
        engine = new RBACEngine(Settings.EMPTY, mock(CompositeRolesStore.class));
    }

    public void testSameUserPermission() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.REALM);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        final String authenticationType = changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
            randomAlphaOfLengthBetween(4, 12);
        when(authenticatedBy.getType()).thenReturn(authenticationType);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.REALM);

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(user);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        // this should still fail since the username is still different
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));

        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest) request).username("joe");
        } else {
            ((AuthenticateRequest) request).username("joe");
        }
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowOtherActions() {
        final User user = mock(User.class);
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(PutUserAction.NAME, DeleteUserAction.NAME, ClusterHealthAction.NAME, ClusterStateAction.NAME,
            ClusterStatsAction.NAME, GetLicenseAction.NAME);
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final boolean runAs = randomBoolean();
        when(authentication.getUser()).thenReturn(user);
        when(user.authenticatedUser()).thenReturn(runAs ? new User("authUser") : user);
        when(user.isRunAs()).thenReturn(runAs);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(randomAlphaOfLengthBetween(4, 12));

        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verifyZeroInteractions(user, request, authentication);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final User authUser = new User("admin", new String[]{"bar"});
        final String username = "joe";
        final User user = new User(username, null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.REALM);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(authUser);
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.REALM);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verify(authentication).getAuthenticationType();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForApiKey() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.API_KEY);
        when(authenticatedBy.getType()).thenReturn(ApiKeyService.API_KEY_REALM_TYPE);

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verify(authentication).getAuthenticationType();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForAccessToken() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.TOKEN);
        when(authenticatedBy.getType()).thenReturn(NativeRealmSettings.TYPE);

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verify(authentication).getAuthenticationType();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getAuthenticationType()).thenReturn(Authentication.AuthenticationType.REALM);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authentication).getLookedUpBy();
        verify(authentication, times(2)).getUser();
        verify(lookedUpBy).getType();
        verify(authentication).getAuthenticationType();
        verifyNoMoreInteractions(authentication, lookedUpBy, authenticatedBy);
    }

    public void testSameUserPermissionAllowsSelfApiKeyInfoRetrievalWhenAuthenticatedByApiKey() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final TransportRequest request = GetApiKeyRequest.usingApiKeyId(apiKeyId, false);
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getAuthenticationType()).thenReturn(AuthenticationType.API_KEY);
        when(authentication.getMetadata()).thenReturn(Map.of(ApiKeyService.API_KEY_ID_KEY, apiKeyId));

        assertTrue(engine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    public void testSameUserPermissionDeniesApiKeyInfoRetrievalWhenAuthenticatedByADifferentApiKey() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final TransportRequest request = GetApiKeyRequest.usingApiKeyId(apiKeyId, false);
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(ApiKeyService.API_KEY_REALM_TYPE);
        when(authentication.getMetadata()).thenReturn(Map.of(ApiKeyService.API_KEY_ID_KEY, randomAlphaOfLengthBetween(4, 7)));

        assertFalse(engine.checkSameUserPermissions(GetApiKeyAction.NAME, request, authentication));
    }

    public void testSameUserPermissionDeniesApiKeyInfoRetrievalWhenLookedupByIsPresent() {
        final User user = new User("joe");
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final TransportRequest request = GetApiKeyRequest.usingApiKeyId(apiKeyId, false);
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final Authentication.RealmRef lookedupBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedupBy);
        when(authentication.getAuthenticationType()).thenReturn(AuthenticationType.API_KEY);
        when(authentication.getMetadata()).thenReturn(Map.of(ApiKeyService.API_KEY_ID_KEY, randomAlphaOfLengthBetween(4, 7)));

        final AssertionError assertionError = expectThrows(AssertionError.class, () -> engine.checkSameUserPermissions(GetApiKeyAction.NAME,
            request, authentication));
        assertNotNull(assertionError);
        assertThat(assertionError.getLocalizedMessage(), is("runAs not supported for api key authentication"));
    }

    /**
     * This tests that action names in the request are considered "matched" by the relevant named privilege
     * (in this case that {@link DeleteAction} and {@link IndexAction} are satisfied by {@link IndexPrivilege#WRITE}).
     */
    public void testNamedIndexPrivilegesMatchApplicableActions() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test1")
            .cluster(Collections.singleton("all"), Collections.emptyList())
            .add(IndexPrivilege.WRITE, "academy")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(ClusterHealthAction.NAME);
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
            .indices("academy")
            .privileges(DeleteAction.NAME, IndexAction.NAME)
            .build());
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);

        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture<>();
        engine.checkPrivileges(authentication, authzInfo, request, Collections.emptyList(), future);

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
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test2")
            .cluster(Set.of("monitor"), Set.of())
            .add(IndexPrivilege.INDEX, "academy")
            .add(IndexPrivilege.WRITE, "initiative")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges("monitor", "manage");
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
            .indices("academy", "initiative", "school")
            .privileges("delete", "index", "manage")
            .build());
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture<>();
        engine.checkPrivileges(authentication, authzInfo, request, Collections.emptyList(), future);

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
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test3")
            .cluster(Set.of("monitor"), Set.of())
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
            .indices("academy")
            .privileges("read", "write")
            .build(),
            authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
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
        List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege(privs, "kibana", "read",
            "data:read/*", "action:login", "action:view/dashboard");
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege(privs, "kibana", "write",
            "data:write/*", "action:login", "action:view/dashboard");
        final ApplicationPrivilege kibanaAdmin = defineApplicationPrivilege(privs, "kibana", "admin",
            "action:login", "action:manage/*");
        final ApplicationPrivilege kibanaViewSpace = defineApplicationPrivilege(privs, "kibana", "view-space",
            "action:login", "space:view/*");
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test3")
            .add(IndexPrivilege.ALL, "logstash-*", "foo?")
            .add(IndexPrivilege.READ, "abc*")
            .add(IndexPrivilege.WRITE, "*xyz")
            .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
            .addApplicationPrivilege(kibanaViewSpace, newHashSet("space/engineering/*", "space/builds"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

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

        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture<>();
        engine.checkPrivileges(authentication, authzInfo, request, privs, future);

        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.getUsername(), is(user.principal()));
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(8));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder("logstash-2016-*").addPrivileges(Collections.singletonMap("write", true)).build(),
            ResourcePrivileges.builder("logstash-*").addPrivileges(Collections.singletonMap("read", true)).build(),
            ResourcePrivileges.builder("log*").addPrivileges(Collections.singletonMap("manage", false)).build(),
            ResourcePrivileges.builder("foo?").addPrivileges(Collections.singletonMap("read", true)).build(),
            ResourcePrivileges.builder("foo*").addPrivileges(Collections.singletonMap("read", false)).build(),
            ResourcePrivileges.builder("abcd*").addPrivileges(mapBuilder().put("read", true).put("write", false).map()).build(),
            ResourcePrivileges.builder("abc*xyz")
                .addPrivileges(mapBuilder().put("read", true).put("write", true).put("manage", false).map()).build(),
            ResourcePrivileges.builder("a*xyz")
                .addPrivileges(mapBuilder().put("read", false).put("write", true).put("manage", false).map()).build()
        ));
        assertThat(response.getApplicationPrivileges().entrySet(), Matchers.iterableWithSize(1));
        final Set<ResourcePrivileges> kibanaPrivileges = response.getApplicationPrivileges().get("kibana");
        assertThat(kibanaPrivileges, Matchers.iterableWithSize(3));
        assertThat(Strings.collectionToCommaDelimitedString(kibanaPrivileges), kibanaPrivileges, containsInAnyOrder(
            ResourcePrivileges.builder("*").addPrivileges(mapBuilder().put("read", true).put("write", false).map()).build(),
            ResourcePrivileges.builder("space/engineering/project-*")
                .addPrivileges(Collections.singletonMap("space:view/dashboard", true)).build(),
            ResourcePrivileges.builder("space/*").addPrivileges(Collections.singletonMap("space:view/dashboard", false)).build()
        ));
    }

    public void testCheckingIndexPermissionsDefinedOnDifferentPatterns() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test-write")
            .add(IndexPrivilege.INDEX, "apache-*")
            .add(IndexPrivilege.DELETE, "apache-2016-*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
            .indices("apache-2016-12", "apache-2017-01")
            .privileges("index", "delete")
            .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder("apache-2016-12")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("delete", true).map()).build(),
            ResourcePrivileges.builder("apache-2017-01")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("delete", false).map()).build()
        ));
    }

    public void testCheckRestrictedIndexPatternPermission() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        final String patternPrefix = RestrictedIndicesNames.ASYNC_SEARCH_PREFIX.substring(0,
                randomIntBetween(2, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX.length() - 2));
        Role role = Role.builder("role")
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, false, patternPrefix + "*")
                .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        String prePatternPrefix = patternPrefix.substring(0, randomIntBetween(1, patternPrefix.length() - 1)) + "*";
        HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(prePatternPrefix)
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(prePatternPrefix)
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", false).map()).build()));

        String matchesPatternPrefix = RestrictedIndicesNames.ASYNC_SEARCH_PREFIX.substring(0, patternPrefix.length() + 1);
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(matchesPatternPrefix + "*")
                .allowRestrictedIndices(false)
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(true));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).map()).build()));
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(matchesPatternPrefix + "*")
                .allowRestrictedIndices(true)
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", false).map()).build()));
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(matchesPatternPrefix)
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(true));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix)
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).map()).build()));

        final String restrictedIndexMatchingWildcard = RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2);
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard + "*")
                .allowRestrictedIndices(true)
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*")
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", false).map()).build()));
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard + "*")
                .allowRestrictedIndices(false)
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard + "*")
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", false).map()).build()));
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(restrictedIndexMatchingWildcard)
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(restrictedIndexMatchingWildcard)
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", false).map()).build()));

        role = Role.builder("role")
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, true, patternPrefix + "*")
                .build();
        authzInfo = new RBACAuthorizationInfo(role, null);
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(matchesPatternPrefix + "*")
                .allowRestrictedIndices(randomBoolean())
                .privileges("index")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(true));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(1));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(matchesPatternPrefix + "*")
                        .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                                .put("index", true).map()).build()));
    }

    public void testCheckExplicitRestrictedIndexPermissions() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        final boolean restrictedIndexPermission = randomBoolean();
        final boolean restrictedMonitorPermission = randomBoolean();
        Role role = Role.builder("role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, restrictedIndexPermission, ".sec*")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, restrictedMonitorPermission, ".security*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        String explicitRestrictedIndex = randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES);
        HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(new String[] {".secret-non-restricted", explicitRestrictedIndex})
                .privileges("index", "monitor")
                .allowRestrictedIndices(false) // explicit false for test
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(".secret-non-restricted") // matches ".sec*" but not ".security*"
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("index", true).put("monitor", false).map()).build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("index", restrictedIndexPermission).put("monitor", restrictedMonitorPermission).map()).build()));

        explicitRestrictedIndex = randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES);
        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(new String[] {".secret-non-restricted", explicitRestrictedIndex})
                .privileges("index", "monitor")
                .allowRestrictedIndices(true) // explicit true for test
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
                ResourcePrivileges.builder(".secret-non-restricted") // matches ".sec*" but not ".security*"
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("index", true).put("monitor", false).map()).build(),
                ResourcePrivileges.builder(explicitRestrictedIndex) // matches both ".sec*" and ".security*"
                    .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                        .put("index", restrictedIndexPermission).put("monitor", restrictedMonitorPermission).map()).build()));
    }

    public void testCheckRestrictedIndexWildcardPermissions() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("role")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, false, ".sec*")
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, true, ".security*")
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        HasPrivilegesResponse response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
            .indices(".sec*", ".security*")
            .privileges("index", "monitor")
            .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder(".sec*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", false).map()).build(),
            ResourcePrivileges.builder(".security*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", true).map()).build()
        ));

        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(".sec*", ".security*")
                .privileges("index", "monitor")
                .allowRestrictedIndices(true)
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder(".sec*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", false).put("monitor", false).map()).build(),
            ResourcePrivileges.builder(".security*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", false).put("monitor", true).map()).build()
        ));

        role = Role.builder("role")
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.INDEX, true, ".sec*")
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.MONITOR, false, ".security*")
                .build();
        authzInfo = new RBACAuthorizationInfo(role, null);

        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(".sec*", ".security*")
                .privileges("index", "monitor")
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder(".sec*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", false).map()).build(),
            ResourcePrivileges.builder(".security*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", true).map()).build()
        ));

        response = hasPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                .indices(".sec*", ".security*")
                .privileges("index", "monitor")
                .allowRestrictedIndices(true)
                .build(), authentication, authzInfo, Collections.emptyList(), Strings.EMPTY_ARRAY);
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.iterableWithSize(2));
        assertThat(response.getIndexPrivileges(), containsInAnyOrder(
            ResourcePrivileges.builder(".sec*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", false).map()).build(),
            ResourcePrivileges.builder(".security*")
                .addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                    .put("index", true).put("monitor", false).map()).build()
        ));
    }

    public void testCheckingApplicationPrivilegesOnDifferentApplicationsAndResources() throws Exception {
        List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege app1Read = defineApplicationPrivilege(privs, "app1", "read", "data:read/*");
        final ApplicationPrivilege app1Write = defineApplicationPrivilege(privs, "app1", "write", "data:write/*");
        final ApplicationPrivilege app1All = defineApplicationPrivilege(privs, "app1", "all", "*");
        final ApplicationPrivilege app2Read = defineApplicationPrivilege(privs, "app2", "read", "data:read/*");
        final ApplicationPrivilege app2Write = defineApplicationPrivilege(privs, "app2", "write", "data:write/*");
        final ApplicationPrivilege app2All = defineApplicationPrivilege(privs, "app2", "all", "*");

        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test-role")
            .addApplicationPrivilege(app1Read, Collections.singleton("foo/*"))
            .addApplicationPrivilege(app1All, Collections.singleton("foo/bar/baz"))
            .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
            .addApplicationPrivilege(app2Write, Collections.singleton("*/bar/*"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

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
            }, authentication, authzInfo, privs, Strings.EMPTY_ARRAY);

        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getIndexPrivileges(), Matchers.emptyIterable());
        assertThat(response.getApplicationPrivileges().entrySet(), Matchers.iterableWithSize(2));
        final Set<ResourcePrivileges> app1 = response.getApplicationPrivileges().get("app1");
        assertThat(app1, Matchers.iterableWithSize(4));
        assertThat(Strings.collectionToCommaDelimitedString(app1), app1, containsInAnyOrder(
            ResourcePrivileges.builder("foo/1").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", true).put("write", false).put("all", false).map()).build(),
            ResourcePrivileges.builder("foo/bar/2").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", true).put("write", false).put("all", false).map()).build(),
            ResourcePrivileges.builder("foo/bar/baz").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", true).put("write", true).put("all", true).map()).build(),
            ResourcePrivileges.builder("baz/bar/foo").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", false).put("write", false).put("all", false).map()).build()
        ));
        final Set<ResourcePrivileges> app2 = response.getApplicationPrivileges().get("app2");
        assertThat(app2, Matchers.iterableWithSize(4));
        assertThat(Strings.collectionToCommaDelimitedString(app2), app2, containsInAnyOrder(
            ResourcePrivileges.builder("foo/1").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", false).put("write", false).put("all", false).map()).build(),
            ResourcePrivileges.builder("foo/bar/2").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", true).put("write", true).put("all", false).map()).build(),
            ResourcePrivileges.builder("foo/bar/baz").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", true).put("write", true).put("all", false).map()).build(),
            ResourcePrivileges.builder("baz/bar/foo").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("read", false).put("write", true).put("all", false).map()).build()
        ));
    }

    public void testCheckingApplicationPrivilegesWithComplexNames() throws Exception {
        final String appName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 10);
        final String action1 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 5);
        final String action2 = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(6, 9);

        final List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege priv1 = defineApplicationPrivilege(privs, appName, action1, "DATA:read/*", "ACTION:" + action1);
        final ApplicationPrivilege priv2 = defineApplicationPrivilege(privs, appName, action2, "DATA:read/*", "ACTION:" + action2);

        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test-write")
            .addApplicationPrivilege(priv1, Collections.singleton("user/*/name"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        final HasPrivilegesResponse response = hasPrivileges(
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[]{
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(appName)
                    .resources("user/hawkeye/name")
                    .privileges("DATA:read/user/*", "ACTION:" + action1, "ACTION:" + action2, action1, action2)
                    .build()
            }, authentication, authzInfo, privs, "monitor");
        assertThat(response.isCompleteMatch(), is(false));
        assertThat(response.getApplicationPrivileges().keySet(), containsInAnyOrder(appName));
        assertThat(response.getApplicationPrivileges().get(appName), iterableWithSize(1));
        assertThat(response.getApplicationPrivileges().get(appName), containsInAnyOrder(
            ResourcePrivileges.builder("user/hawkeye/name").addPrivileges(MapBuilder.newMapBuilder(new LinkedHashMap<String, Boolean>())
                .put("DATA:read/user/*", true)
                .put("ACTION:" + action1, true)
                .put("ACTION:" + action2, false)
                .put(action1, true)
                .put(action2, false)
                .map()).build()
        ));
    }

    public void testIsCompleteMatch() throws Exception {
        final List<ApplicationPrivilegeDescriptor> privs = new ArrayList<>();
        final ApplicationPrivilege kibanaRead = defineApplicationPrivilege(privs, "kibana", "read", "data:read/*");
        final ApplicationPrivilege kibanaWrite = defineApplicationPrivilege(privs, "kibana", "write", "data:write/*");
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test-write")
            .cluster(Set.of("monitor"), Set.of())
            .add(IndexPrivilege.READ, "read-*")
            .add(IndexPrivilege.ALL, "all-*")
            .addApplicationPrivilege(kibanaRead, Collections.singleton("*"))
            .build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);


        assertThat(hasPrivileges(
            indexPrivileges("read", "read-123", "read-456", "all-999"), authentication, authzInfo, privs, "monitor").isCompleteMatch(),
            is(true));
        assertThat(hasPrivileges(
            indexPrivileges("read", "read-123", "read-456", "all-999"), authentication, authzInfo, privs, "manage").isCompleteMatch(),
            is(false));
        assertThat(hasPrivileges(
            indexPrivileges("write", "read-123", "read-456", "all-999"), authentication, authzInfo, privs, "monitor").isCompleteMatch(),
            is(false));
        assertThat(hasPrivileges(
            indexPrivileges("write", "read-123", "read-456", "all-999"), authentication, authzInfo, privs, "manage").isCompleteMatch(),
            is(false));
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
            }, authentication, authzInfo, privs, "monitor").isCompleteMatch(), is(true));
        assertThat(hasPrivileges(
            new RoleDescriptor.IndicesPrivileges[]{indexPrivileges("read", "read-123", "read-456", "all-999")},
            new RoleDescriptor.ApplicationResourcePrivileges[]{
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana").resources("*").privileges("read").build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana").resources("*").privileges("write").build()
            }, authentication, authzInfo, privs, "monitor").isCompleteMatch(), is(false));
    }

    public void testBuildUserPrivilegeResponse() {
        final ManageApplicationPrivileges manageApplicationPrivileges = new ManageApplicationPrivileges(Sets.newHashSet("app01", "app02"));
        final BytesArray query = new BytesArray("{\"term\":{\"public\":true}}");
        final Role role = Role.builder("test", "role")
            .cluster(Sets.newHashSet("monitor", "manage_watcher"), Collections.singleton(manageApplicationPrivileges))
            .add(IndexPrivilege.get(Sets.newHashSet("read", "write")), "index-1")
            .add(IndexPrivilege.ALL, "index-2", "index-3")
            .add(
                new FieldPermissions(new FieldPermissionsDefinition(new String[]{ "public.*" }, new String[0])),
                Collections.singleton(query),
                IndexPrivilege.READ, randomBoolean(), "index-4", "index-5")
            .addApplicationPrivilege(new ApplicationPrivilege("app01", "read", "data:read"), Collections.singleton("*"))
            .runAs(new Privilege(Sets.newHashSet("user01", "user02"), "user01", "user02"))
            .build();

        final GetUserPrivilegesResponse response = engine.buildUserPrivilegesResponseObject(role);

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
        assertThat(index4.getFieldSecurity(), containsInAnyOrder(
            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[]{ "public.*" }, new String[0])));
        assertThat(index4.getQueries(), containsInAnyOrder(query));

        assertThat(response.getApplicationPrivileges(), containsInAnyOrder(
            RoleDescriptor.ApplicationResourcePrivileges.builder().application("app01").privileges("read").resources("*").build())
        );

        assertThat(response.getRunAs(), containsInAnyOrder("user01", "user02"));
    }

    public void testBackingIndicesAreIncludedForAuthorizedDataStreams() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test1")
            .cluster(Collections.singleton("all"), Collections.emptyList())
            .add(IndexPrivilege.READ, dataStreamName)
            .build();

        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        List<IndexMetadata> backingIndices = new ArrayList<>();
        int numBackingIndices = randomIntBetween(1, 3);
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices.add(DataStreamTestHelper.createBackingIndex(dataStreamName, k + 1).build());
        }
        DataStream ds = new DataStream(dataStreamName, null,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()));
        IndexAbstraction.DataStream iads = new IndexAbstraction.DataStream(ds, backingIndices);
        lookup.put(ds.getName(), iads);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.Index(im, iads));
        }

        SearchRequest request = new SearchRequest("*");
        Set<String> authorizedIndices =
            RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(request, SearchAction.NAME), lookup);
        assertThat(authorizedIndices, hasItem(dataStreamName));
        assertThat(authorizedIndices, hasItems(backingIndices.stream()
            .map(im -> im.getIndex().getName()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY)));
    }

    public void testExplicitMappingUpdatesAreNotGrantedWithIngestPrivileges() {
        final String dataStreamName = "my_data_stream";
        User user = new User(randomAlphaOfLengthBetween(4, 12));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        Role role = Role.builder("test1")
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
        DataStream ds = new DataStream(dataStreamName, null,
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()));
        IndexAbstraction.DataStream iads = new IndexAbstraction.DataStream(ds, backingIndices);
        lookup.put(ds.getName(), iads);
        for (IndexMetadata im : backingIndices) {
            lookup.put(im.getIndex().getName(), new IndexAbstraction.Index(im, iads));
        }

        PutMappingRequest request = new PutMappingRequest("*");
        request.source("{ \"properties\": { \"message\": { \"type\": \"text\" } } }",
                XContentType.JSON
        );
        Set<String> authorizedIndices =
                RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(request, PutMappingAction.NAME), lookup);
        assertThat(authorizedIndices.isEmpty(), is(true));
    }

    private GetUserPrivilegesResponse.Indices findIndexPrivilege(Set<GetUserPrivilegesResponse.Indices> indices, String name) {
        return indices.stream().filter(i -> i.getIndices().contains(name)).findFirst().get();
    }

    private RoleDescriptor.IndicesPrivileges indexPrivileges(String priv, String... indices) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(indices)
            .privileges(priv)
            .build();
    }

    private ApplicationPrivilege defineApplicationPrivilege(List<ApplicationPrivilegeDescriptor> privs, String app, String name,
                                                            String ... actions) {
        privs.add(new ApplicationPrivilegeDescriptor(app, name, newHashSet(actions), emptyMap()));
        return new ApplicationPrivilege(app, name, actions);
    }

    private HasPrivilegesResponse hasPrivileges(RoleDescriptor.IndicesPrivileges indicesPrivileges, Authentication authentication,
                                                AuthorizationInfo authorizationInfo,
                                                List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                                                String... clusterPrivileges) throws Exception {
        return hasPrivileges(
            new RoleDescriptor.IndicesPrivileges[]{indicesPrivileges},
            new RoleDescriptor.ApplicationResourcePrivileges[0],
            authentication, authorizationInfo, applicationPrivilegeDescriptors,
            clusterPrivileges
        );
    }

    private HasPrivilegesResponse hasPrivileges(RoleDescriptor.IndicesPrivileges[] indicesPrivileges,
                                                RoleDescriptor.ApplicationResourcePrivileges[] appPrivileges,
                                                Authentication authentication,
                                                AuthorizationInfo authorizationInfo,
                                                List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                                                String... clusterPrivileges) throws Exception {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(authentication.getUser().principal());
        request.clusterPrivileges(clusterPrivileges);
        request.indexPrivileges(indicesPrivileges);
        request.applicationPrivileges(appPrivileges);
        final PlainActionFuture<HasPrivilegesResponse> future = new PlainActionFuture<>();
        engine.checkPrivileges(authentication, authorizationInfo, request, applicationPrivilegeDescriptors, future);
        final HasPrivilegesResponse response = future.get();
        assertThat(response, notNullValue());
        return response;
    }

    private static MapBuilder<String, Boolean> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }
}
