/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.CreateApiKeyRequest;
import org.elasticsearch.client.security.CreateApiKeyRequestTests;
import org.elasticsearch.client.security.CreateServiceAccountTokenRequest;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DeletePrivilegesRequest;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteServiceAccountTokenRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetPrivilegesRequest;
import org.elasticsearch.client.security.GetRoleMappingsRequest;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetServiceAccountCredentialsRequest;
import org.elasticsearch.client.security.GetServiceAccountsRequest;
import org.elasticsearch.client.security.GetUsersRequest;
import org.elasticsearch.client.security.GrantApiKeyRequest;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.QueryApiKeyRequest;
import org.elasticsearch.client.security.QueryApiKeyRequestTests;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.client.security.user.privileges.Role.IndexPrivilegeName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.RequestConvertersTests.assertToXContentBody;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityRequestConvertersTests extends ESTestCase {

    public void testPutUser() throws IOException {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final char[] password = randomBoolean() ? randomAlphaOfLengthBetween(8, 12).toCharArray() : null;
        final List<String> roles = Arrays.asList(generateRandomStringArray(randomIntBetween(2, 8), randomIntBetween(8, 16), false, true));
        final String email = randomBoolean() ? null : randomAlphaOfLengthBetween(12, 24);
        final String fullName = randomBoolean() ? null : randomAlphaOfLengthBetween(7, 14);
        final boolean enabled = randomBoolean();
        final Map<String, Object> metadata = new HashMap<>();
        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(0, 10); i++) {
                metadata.put(String.valueOf(i), randomAlphaOfLengthBetween(1, 12));
            }
        }
        final User user = new User(username, roles, metadata, fullName, email);

        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);

        PutUserRequest putUserRequest = new PutUserRequest(user, password, enabled, refreshPolicy);
        Request request = SecurityRequestConverters.putUser(putUserRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/" + putUserRequest.getUser().getUsername(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(putUserRequest, request.getEntity());
    }

    public void testDeleteUser() {
        final String name = randomAlphaOfLengthBetween(4, 12);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        DeleteUserRequest deleteUserRequest = new DeleteUserRequest(name, refreshPolicy);
        Request request = SecurityRequestConverters.deleteUser(deleteUserRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/" + name, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testGetUsers() {
        final String[] users = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        GetUsersRequest getUsersRequest = new GetUsersRequest(users);
        Request request = SecurityRequestConverters.getUsers(getUsersRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        if (users.length == 0) {
            assertEquals("/_security/user", request.getEndpoint());
        } else {
            assertEquals("/_security/user/" + Strings.collectionToCommaDelimitedString(getUsersRequest.getUsernames()),
                request.getEndpoint());
        }
        assertNull(request.getEntity());
        assertEquals(Collections.emptyMap(), request.getParameters());
    }

    public void testPutRoleMapping() throws IOException {
        final String username = randomAlphaOfLengthBetween(4, 7);
        final String rolename = randomAlphaOfLengthBetween(4, 7);
        final String roleMappingName = randomAlphaOfLengthBetween(4, 7);
        final String groupname = "cn="+randomAlphaOfLengthBetween(4, 7)+",dc=example,dc=com";
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams;
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }

        final RoleMapperExpression rules = AnyRoleMapperExpression.builder()
                .addExpression(FieldRoleMapperExpression.ofUsername(username))
                .addExpression(FieldRoleMapperExpression.ofGroups(groupname))
                .build();
        final PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(roleMappingName, true,
            Collections.singletonList(rolename), Collections.emptyList(), rules, null, refreshPolicy);

        final Request request = SecurityRequestConverters.putRoleMapping(putRoleMappingRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/role_mapping/" + roleMappingName, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(putRoleMappingRequest, request.getEntity());
    }

    public void testGetRoleMappings() throws IOException {
        int noOfRoleMappingNames = randomIntBetween(0, 2);
        final String[] roleMappingNames =
                randomArray(noOfRoleMappingNames, noOfRoleMappingNames, String[]::new, () -> randomAlphaOfLength(5));
        final GetRoleMappingsRequest getRoleMappingsRequest = new GetRoleMappingsRequest(roleMappingNames);

        final Request request = SecurityRequestConverters.getRoleMappings(getRoleMappingsRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        if (noOfRoleMappingNames == 0) {
            assertEquals("/_security/role_mapping", request.getEndpoint());
        } else {
            assertEquals("/_security/role_mapping/" +
                    Strings.collectionToCommaDelimitedString(getRoleMappingsRequest.getRoleMappingNames()), request.getEndpoint());
        }
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertNull(request.getEntity());
    }

    public void testEnableUser() {
        final String username = randomAlphaOfLengthBetween(1, 12);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        EnableUserRequest enableUserRequest = new EnableUserRequest(username, refreshPolicy);
        Request request = SecurityRequestConverters.enableUser(enableUserRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/" + username + "/_enable", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testDisableUser() {
        final String username = randomAlphaOfLengthBetween(1, 12);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        DisableUserRequest disableUserRequest = new DisableUserRequest(username, refreshPolicy);
        Request request = SecurityRequestConverters.disableUser(disableUserRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/" + username + "/_disable", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    private static Map<String, String> getExpectedParamsFromRefreshPolicy(RefreshPolicy refreshPolicy) {
        if (refreshPolicy != RefreshPolicy.NONE) {
            return Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            return Collections.emptyMap();
        }
    }

    public void testChangePassword() throws IOException {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final char[] password = randomAlphaOfLengthBetween(8, 12).toCharArray();
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest(username, password, refreshPolicy);
        Request request = SecurityRequestConverters.changePassword(changePasswordRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/" + changePasswordRequest.getUsername() + "/_password", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(changePasswordRequest, request.getEntity());
    }

    public void testSelfChangePassword() throws IOException {
        final char[] password = randomAlphaOfLengthBetween(8, 12).toCharArray();
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest(null, password, refreshPolicy);
        Request request = SecurityRequestConverters.changePassword(changePasswordRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/user/_password", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(changePasswordRequest, request.getEntity());
    }

    public void testDeleteRoleMapping() throws IOException {
        final String roleMappingName = randomAlphaOfLengthBetween(4, 7);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams;
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }
        final DeleteRoleMappingRequest deleteRoleMappingRequest = new DeleteRoleMappingRequest(roleMappingName, refreshPolicy);

        final Request request = SecurityRequestConverters.deleteRoleMapping(deleteRoleMappingRequest);

        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_security/role_mapping/" + roleMappingName, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testGetRoles() {
        final String[] roles = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        final GetRolesRequest getRolesRequest = new GetRolesRequest(roles);
        final Request request = SecurityRequestConverters.getRoles(getRolesRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        if (roles.length == 0) {
            assertEquals("/_security/role", request.getEndpoint());
        } else {
            assertEquals("/_security/role/" + Strings.collectionToCommaDelimitedString(getRolesRequest.getRoleNames()),
                request.getEndpoint());
        }
        assertNull(request.getEntity());
        assertEquals(Collections.emptyMap(), request.getParameters());
    }

    public void testDeleteRole() {
        final String name = randomAlphaOfLengthBetween(1, 12);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest(name, refreshPolicy);
        Request request = SecurityRequestConverters.deleteRole(deleteRoleRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_security/role/" + name, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testCreateTokenWithPasswordGrant() throws Exception {
        final String username = randomAlphaOfLengthBetween(1, 12);
        final String password = randomAlphaOfLengthBetween(8, 12);
        CreateTokenRequest createTokenRequest = CreateTokenRequest.passwordGrant(username, password.toCharArray());
        Request request = SecurityRequestConverters.createToken(createTokenRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/oauth2/token", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(createTokenRequest, request.getEntity());
    }

    public void testCreateTokenWithRefreshTokenGrant() throws Exception {
        final String refreshToken = randomAlphaOfLengthBetween(8, 24);
        CreateTokenRequest createTokenRequest = CreateTokenRequest.refreshTokenGrant(refreshToken);
        Request request = SecurityRequestConverters.createToken(createTokenRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/oauth2/token", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(createTokenRequest, request.getEntity());
    }

    public void testCreateTokenWithClientCredentialsGrant() throws Exception {
        CreateTokenRequest createTokenRequest = CreateTokenRequest.clientCredentialsGrant();
        Request request = SecurityRequestConverters.createToken(createTokenRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/oauth2/token", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(createTokenRequest, request.getEntity());
    }

    public void testDelegatePkiAuthentication() throws Exception {
        X509Certificate mockCertificate = mock(X509Certificate.class);
        when(mockCertificate.getEncoded()).thenReturn(new byte[0]);
        DelegatePkiAuthenticationRequest delegatePkiAuthenticationRequest = new DelegatePkiAuthenticationRequest(
                Arrays.asList(mockCertificate));
        Request request = SecurityRequestConverters.delegatePkiAuthentication(delegatePkiAuthenticationRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/delegate_pki", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(delegatePkiAuthenticationRequest, request.getEntity());
    }

    public void testGetApplicationPrivilege() throws Exception {
        final String application = randomAlphaOfLength(6);
        final String privilege = randomAlphaOfLength(4);
        GetPrivilegesRequest getPrivilegesRequest = new GetPrivilegesRequest(application, privilege);
        Request request = SecurityRequestConverters.getPrivileges(getPrivilegesRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege/" + application + "/" + privilege, request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertNull(request.getEntity());
    }

    public void testGetAllPrivilegesForApplication() throws Exception {
        final String application = randomAlphaOfLength(6);
        GetPrivilegesRequest getPrivilegesRequest = GetPrivilegesRequest.getApplicationPrivileges(application);
        Request request = SecurityRequestConverters.getPrivileges(getPrivilegesRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege/" + application, request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertNull(request.getEntity());
    }

    public void testGetMultipleApplicationPrivileges() throws Exception {
        final String application = randomAlphaOfLength(6);
        final int numberOfPrivileges = randomIntBetween(1, 5);
        final String[] privilegeNames =
            randomArray(numberOfPrivileges, numberOfPrivileges, String[]::new, () -> randomAlphaOfLength(5));
        GetPrivilegesRequest getPrivilegesRequest = new GetPrivilegesRequest(application, privilegeNames);
        Request request = SecurityRequestConverters.getPrivileges(getPrivilegesRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege/" + application + "/" + Strings.arrayToCommaDelimitedString(privilegeNames),
            request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertNull(request.getEntity());
    }

    public void testGetAllApplicationPrivileges() throws Exception {
        GetPrivilegesRequest getPrivilegesRequest = GetPrivilegesRequest.getAllPrivileges();
        Request request = SecurityRequestConverters.getPrivileges(getPrivilegesRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege", request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertNull(request.getEntity());
    }

    public void testPutPrivileges() throws Exception {
        int noOfApplicationPrivileges = randomIntBetween(2, 4);
        final List<ApplicationPrivilege> privileges = new ArrayList<>();
        for (int count = 0; count < noOfApplicationPrivileges; count++) {
            final String[] actions = generateRandomStringArray(3, 5, false, false);
            privileges.add(ApplicationPrivilege.builder()
                    .application(randomAlphaOfLength(4))
                    .privilege(randomAlphaOfLengthBetween(3, 5))
                    .metadata(Collections.singletonMap("k1", "v1"))
                    .actions(actions == null ? Collections.emptyList() : List.of(actions))
                    .build());
        }
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, refreshPolicy);
        final Request request = SecurityRequestConverters.putPrivileges(putPrivilegesRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(putPrivilegesRequest, request.getEntity());
    }

    public void testDeletePrivileges() {
        final String application = randomAlphaOfLengthBetween(1, 12);
        final List<String> privileges = randomSubsetOf(randomIntBetween(1, 3), "read", "write", "all");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams = getExpectedParamsFromRefreshPolicy(refreshPolicy);
        DeletePrivilegesRequest deletePrivilegesRequest =
            new DeletePrivilegesRequest(application, privileges.toArray(Strings.EMPTY_ARRAY), refreshPolicy);
        Request request = SecurityRequestConverters.deletePrivileges(deletePrivilegesRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_security/privilege/" + application + "/" + Strings.collectionToCommaDelimitedString(privileges),
            request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testPutRole() throws IOException {
        final String roleName = randomAlphaOfLengthBetween(4, 7);
        final List<String> clusterPrivileges = randomSubsetOf(3, Role.ClusterPrivilegeName.ALL_ARRAY);
        final Map<String, Object> metadata = Collections.singletonMap(randomAlphaOfLengthBetween(4, 7), randomAlphaOfLengthBetween(4, 7));
        final String[] runAsPrivilege = randomArray(3, String[]::new, () -> randomAlphaOfLength(5));
        final List<String> applicationPrivilegeNames = Arrays.asList(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(5)));
        final List<String> applicationResouceNames = Arrays.asList(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(5)));
        final ApplicationResourcePrivileges applicationResourcePrivilege = new ApplicationResourcePrivileges(
                randomAlphaOfLengthBetween(4, 7), applicationPrivilegeNames, applicationResouceNames);
        final List<String> indicesName = Arrays.asList(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(5)));
        final List<String> indicesPrivilegeName = Arrays.asList(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(5)));
        final List<String> indicesPrivilegeGrantedName = Arrays.asList(randomArray(3, String[]::new, () -> randomAlphaOfLength(5)));
        final List<String> indicesPrivilegeDeniedName = Arrays.asList(randomArray(3, String[]::new, () -> randomAlphaOfLength(5)));
        final String indicesPrivilegeQuery = randomAlphaOfLengthBetween(0, 7);
        final IndicesPrivileges indicesPrivilege = IndicesPrivileges.builder().indices(indicesName).privileges(indicesPrivilegeName)
                .allowRestrictedIndices(randomBoolean()).grantedFields(indicesPrivilegeGrantedName).deniedFields(indicesPrivilegeDeniedName)
                .query(indicesPrivilegeQuery).build();
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams;
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }
        final Role role = Role.builder().name(roleName).clusterPrivileges(clusterPrivileges).indicesPrivileges(indicesPrivilege)
                .applicationResourcePrivileges(applicationResourcePrivilege).runAsPrivilege(runAsPrivilege).metadata(metadata).build();
        final PutRoleRequest putRoleRequest = new PutRoleRequest(role, refreshPolicy);
        final Request request = SecurityRequestConverters.putRole(putRoleRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_security/role/" + roleName, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(putRoleRequest, request.getEntity());
    }

    public void testCreateApiKey() throws IOException {
        final CreateApiKeyRequest createApiKeyRequest = buildCreateApiKeyRequest();

        final Map<String, String> expectedParams;
        final RefreshPolicy refreshPolicy = createApiKeyRequest.getRefreshPolicy();
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }

        final Request request = SecurityRequestConverters.createApiKey(createApiKeyRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/api_key", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(createApiKeyRequest, request.getEntity());
    }

    private CreateApiKeyRequest buildCreateApiKeyRequest() {
        final String name = randomAlphaOfLengthBetween(4, 7);
        final List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = randomBoolean() ? null : TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, Object> metadata = CreateApiKeyRequestTests.randomMetadata();
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, metadata);
        return createApiKeyRequest;
    }

    public void testGrantApiKey() throws IOException {
        final CreateApiKeyRequest createApiKeyRequest = buildCreateApiKeyRequest();
        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest(randomBoolean()
            ? GrantApiKeyRequest.Grant.accessTokenGrant(randomAlphaOfLength(24))
            : GrantApiKeyRequest.Grant.passwordGrant(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(14, 18).toCharArray()),
            createApiKeyRequest);
        final Map<String, String> expectedParams;
        final RefreshPolicy refreshPolicy = createApiKeyRequest.getRefreshPolicy();
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }

        final Request request = SecurityRequestConverters.grantApiKey(grantApiKeyRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_security/api_key/grant", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(grantApiKeyRequest, request.getEntity());
    }

    public void testGetApiKey() throws IOException {
        String realmName = randomAlphaOfLength(5);
        String userName = randomAlphaOfLength(7);
        final GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName(realmName, userName);
        final Request request = SecurityRequestConverters.getApiKey(getApiKeyRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/api_key", request.getEndpoint());
        Map<String, String> expectedMapOfParameters = new HashMap<>();
        expectedMapOfParameters.put("realm_name", realmName);
        expectedMapOfParameters.put("username", userName);
        expectedMapOfParameters.put("owner", Boolean.FALSE.toString());
        assertThat(request.getParameters(), equalTo(expectedMapOfParameters));
    }

    public void testInvalidateApiKey() throws IOException {
        String realmName = randomAlphaOfLength(5);
        String userName = randomAlphaOfLength(7);
        final InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmName, userName);
        final Request request = SecurityRequestConverters.invalidateApiKey(invalidateApiKeyRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_security/api_key", request.getEndpoint());
        assertToXContentBody(invalidateApiKeyRequest, request.getEntity());
    }

    public void testQueryApiKey() throws IOException {
        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryApiKeyRequestTests.randomQueryBuilder(),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            QueryApiKeyRequestTests.randomFieldSortBuilders(),
            QueryApiKeyRequestTests.randomSearchAfterBuilder());
        final Request request = SecurityRequestConverters.queryApiKey(queryApiKeyRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/_query/api_key", request.getEndpoint());
        assertToXContentBody(queryApiKeyRequest, request.getEntity());
    }

    public void testGetServiceAccounts() throws IOException {
        final String namespace = randomBoolean() ? randomAlphaOfLengthBetween(3, 8) : null;
        final String serviceName = namespace == null ? null : randomAlphaOfLengthBetween(3, 8);
        final GetServiceAccountsRequest getServiceAccountsRequest = new GetServiceAccountsRequest(namespace, serviceName);
        final Request request = SecurityRequestConverters.getServiceAccounts(getServiceAccountsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        if (namespace == null) {
            assertEquals("/_security/service", request.getEndpoint());
        } else if (serviceName == null) {
            assertEquals("/_security/service/" + namespace, request.getEndpoint());
        } else {
            assertEquals("/_security/service/" + namespace + "/" + serviceName, request.getEndpoint());
        }
    }

    public void testCreateServiceAccountToken() throws IOException {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomBoolean() ? randomAlphaOfLengthBetween(3, 8) : null;
        final RefreshPolicy refreshPolicy = randomBoolean() ? randomFrom(RefreshPolicy.values()) : null;
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
            new CreateServiceAccountTokenRequest(namespace, serviceName, tokenName, refreshPolicy);
        final Request request = SecurityRequestConverters.createServiceAccountToken(createServiceAccountTokenRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        final String url =
            "/_security/service/" + namespace + "/" + serviceName + "/credential/token" + (tokenName == null ? "" : "/" + tokenName);
        assertEquals(url, request.getEndpoint());
        if (refreshPolicy != null && refreshPolicy != RefreshPolicy.NONE) {
            assertEquals(refreshPolicy.getValue(), request.getParameters().get("refresh"));
        }
    }

    public void testDeleteServiceAccountToken() throws IOException {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final RefreshPolicy refreshPolicy = randomBoolean() ? randomFrom(RefreshPolicy.values()) : null;
        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest =
            new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName, refreshPolicy);
        final Request request = SecurityRequestConverters.deleteServiceAccountToken(deleteServiceAccountTokenRequest);
        assertEquals("/_security/service/" + namespace + "/" + serviceName + "/credential/token/" + tokenName, request.getEndpoint());
        if (refreshPolicy != null && refreshPolicy != RefreshPolicy.NONE) {
            assertEquals(refreshPolicy.getValue(), request.getParameters().get("refresh"));
        }
    }

    public void testGetServiceAccountCredentials() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final GetServiceAccountCredentialsRequest getServiceAccountCredentialsRequest =
            new GetServiceAccountCredentialsRequest(namespace, serviceName);
        final Request request = SecurityRequestConverters.getServiceAccountCredentials(getServiceAccountCredentialsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_security/service/" + namespace + "/" + serviceName + "/credential", request.getEndpoint());
    }
 }
