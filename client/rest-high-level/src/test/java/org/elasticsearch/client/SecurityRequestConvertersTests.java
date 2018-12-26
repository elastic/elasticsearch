/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.DeletePrivilegesRequest;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.GetPrivilegesRequest;
import org.elasticsearch.client.security.GetRoleMappingsRequest;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetUsersRequest;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.RequestConvertersTests.assertToXContentBody;

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
        final PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(roleMappingName, true, Collections.singletonList(
                rolename), rules, null, refreshPolicy);

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

    public void testGetAllApplicationPrivileges() throws Exception {
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

    public void testGetAllPrivileges() throws Exception {
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
            privileges.add(ApplicationPrivilege.builder()
                    .application(randomAlphaOfLength(4))
                    .privilege(randomAlphaOfLengthBetween(3, 5))
                    .actions(Sets.newHashSet(generateRandomStringArray(3, 5, false, false)))
                    .metadata(Collections.singletonMap("k1", "v1"))
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
                .grantedFields(indicesPrivilegeGrantedName).deniedFields(indicesPrivilegeDeniedName).query(indicesPrivilegeQuery).build();
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
}
