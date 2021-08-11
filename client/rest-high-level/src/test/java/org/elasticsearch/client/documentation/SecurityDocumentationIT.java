/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.ClearApiKeyCacheRequest;
import org.elasticsearch.client.security.ClearPrivilegesCacheRequest;
import org.elasticsearch.client.security.ClearPrivilegesCacheResponse;
import org.elasticsearch.client.security.ClearRealmCacheRequest;
import org.elasticsearch.client.security.ClearRealmCacheResponse;
import org.elasticsearch.client.security.ClearRolesCacheRequest;
import org.elasticsearch.client.security.ClearRolesCacheResponse;
import org.elasticsearch.client.security.ClearSecurityCacheResponse;
import org.elasticsearch.client.security.ClearServiceAccountTokenCacheRequest;
import org.elasticsearch.client.security.CreateApiKeyRequest;
import org.elasticsearch.client.security.CreateApiKeyRequestTests;
import org.elasticsearch.client.security.CreateApiKeyResponse;
import org.elasticsearch.client.security.CreateServiceAccountTokenRequest;
import org.elasticsearch.client.security.CreateServiceAccountTokenResponse;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationResponse;
import org.elasticsearch.client.security.DeletePrivilegesRequest;
import org.elasticsearch.client.security.DeletePrivilegesResponse;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleMappingResponse;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteRoleResponse;
import org.elasticsearch.client.security.DeleteServiceAccountTokenRequest;
import org.elasticsearch.client.security.DeleteServiceAccountTokenResponse;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DeleteUserResponse;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.ExpressionRoleMapping;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetApiKeyResponse;
import org.elasticsearch.client.security.GetBuiltinPrivilegesResponse;
import org.elasticsearch.client.security.GetPrivilegesRequest;
import org.elasticsearch.client.security.GetPrivilegesResponse;
import org.elasticsearch.client.security.GetRoleMappingsRequest;
import org.elasticsearch.client.security.GetRoleMappingsResponse;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetRolesResponse;
import org.elasticsearch.client.security.GetServiceAccountCredentialsRequest;
import org.elasticsearch.client.security.GetServiceAccountCredentialsResponse;
import org.elasticsearch.client.security.GetServiceAccountsRequest;
import org.elasticsearch.client.security.GetServiceAccountsResponse;
import org.elasticsearch.client.security.GetSslCertificatesResponse;
import org.elasticsearch.client.security.GetUserPrivilegesResponse;
import org.elasticsearch.client.security.GetUsersRequest;
import org.elasticsearch.client.security.GetUsersResponse;
import org.elasticsearch.client.security.GrantApiKeyRequest;
import org.elasticsearch.client.security.HasPrivilegesRequest;
import org.elasticsearch.client.security.HasPrivilegesResponse;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.InvalidateApiKeyResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.client.security.NodeEnrollmentResponse;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutPrivilegesResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleMappingResponse;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutRoleResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.TemplateRoleName;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.client.security.support.CertificateInfo;
import org.elasticsearch.client.security.support.ServiceAccountInfo;
import org.elasticsearch.client.security.support.ServiceTokenInfo;
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
import org.elasticsearch.client.security.user.privileges.UserIndicesPrivileges;
import org.elasticsearch.client.security.KibanaEnrollmentResponse;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.hamcrest.Matchers;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SecurityDocumentationIT extends ESRestHighLevelClientTestCase {

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testGetUsers() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        String[] usernames = new String[]{"user1", "user2", "user3"};
        addUser(client, usernames[0], randomAlphaOfLengthBetween(14, 18));
        addUser(client, usernames[1], randomAlphaOfLengthBetween(14, 18));
        addUser(client, usernames[2], randomAlphaOfLengthBetween(14, 18));
        {
            //tag::get-users-request
            GetUsersRequest request = new GetUsersRequest(usernames[0]);
            //end::get-users-request
            //tag::get-users-execute
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            //end::get-users-execute
            //tag::get-users-response
            List<User> users = new ArrayList<>(1);
            users.addAll(response.getUsers());
            //end::get-users-response

            assertNotNull(response);
            assertThat(users.size(), equalTo(1));
            assertThat(users.get(0).getUsername(), is(usernames[0]));
        }

        {
            //tag::get-users-list-request
            GetUsersRequest request = new GetUsersRequest(usernames);
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            //end::get-users-list-request

            List<User> users = new ArrayList<>(3);
            users.addAll(response.getUsers());
            users.sort(Comparator.comparing(User::getUsername));
            assertNotNull(response);
            assertThat(users.size(), equalTo(3));
            assertThat(users.get(0).getUsername(), equalTo(usernames[0]));
            assertThat(users.get(1).getUsername(), equalTo(usernames[1]));
            assertThat(users.get(2).getUsername(), equalTo(usernames[2]));
            assertThat(users.size(), equalTo(3));
        }

        {
            //tag::get-users-all-request
            GetUsersRequest request = new GetUsersRequest();
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            //end::get-users-all-request

            List<User> users = new ArrayList<>(3);
            users.addAll(response.getUsers());
            assertNotNull(response);
            // 10 users are expected to be returned
            // test_users (3): user1, user2, user3
            // system_users (6): elastic, beats_system, apm_system, logstash_system, kibana, kibana_system, remote_monitoring_user
            logger.info(users);
            assertThat(users.size(), equalTo(10));
        }

        {
            GetUsersRequest request = new GetUsersRequest(usernames[0]);
            ActionListener<GetUsersResponse> listener;

            //tag::get-users-execute-listener
            listener = new ActionListener<GetUsersResponse>() {
                @Override
                public void onResponse(GetUsersResponse getRolesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::get-users-execute-listener

            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<GetUsersResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::get-users-execute-async
            client.security().getUsersAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::get-users-execute-async

            final GetUsersResponse response = future.get(30, TimeUnit.SECONDS);
            List<User> users = new ArrayList<>(1);
            users.addAll(response.getUsers());
            assertNotNull(response);
            assertThat(users.size(), equalTo(1));
            assertThat(users.get(0).getUsername(), equalTo(usernames[0]));
        }
    }

    public void testPutUser() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::put-user-password-request
            char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
            User user = new User("example", Collections.singletonList("superuser"));
            PutUserRequest request = PutUserRequest.withPassword(user, password, true, RefreshPolicy.NONE);
            //end::put-user-password-request

            //tag::put-user-execute
            PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
            //end::put-user-execute

            //tag::put-user-response
            boolean isCreated = response.isCreated(); // <1>
            //end::put-user-response

            assertTrue(isCreated);
        }
        {
            byte[] salt = new byte[32];
            // no need for secure random in a test; it could block and would not be reproducible anyway
            random().nextBytes(salt);
            char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
            User user = new User("example2", Collections.singletonList("superuser"));

            //tag::put-user-hash-request
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(password, salt, 10000, 256);
            final byte[] pbkdfEncoded = secretKeyFactory.generateSecret(keySpec).getEncoded();
            char[] passwordHash = ("{PBKDF2}10000$" + Base64.getEncoder().encodeToString(salt)
                + "$" + Base64.getEncoder().encodeToString(pbkdfEncoded)).toCharArray();

            PutUserRequest request = PutUserRequest.withPasswordHash(user, passwordHash, true, RefreshPolicy.NONE);
            //end::put-user-hash-request

            try {
                client.security().putUser(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException e) {
                // This is expected to fail as the server will not be using PBKDF2, but that's easiest hasher to support
                // in a standard JVM without introducing additional libraries.
                assertThat(e.getDetailedMessage(), containsString("PBKDF2"));
            }
        }

        {
            User user = new User("example", Arrays.asList("superuser", "another-role"));
            //tag::put-user-update-request
            PutUserRequest request = PutUserRequest.updateUser(user, true, RefreshPolicy.NONE);
            //end::put-user-update-request

            // tag::put-user-execute-listener
            ActionListener<PutUserResponse> listener = new ActionListener<PutUserResponse>() {
                @Override
                public void onResponse(PutUserResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-user-execute-async
            client.security().putUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        addUser(client, "testUser", "testUserPassword");

        {
            // tag::delete-user-request
            DeleteUserRequest deleteUserRequest = new DeleteUserRequest(
                "testUser");    // <1>
            // end::delete-user-request

            // tag::delete-user-execute
            DeleteUserResponse deleteUserResponse = client.security().deleteUser(deleteUserRequest, RequestOptions.DEFAULT);
            // end::delete-user-execute

            // tag::delete-user-response
            boolean found = deleteUserResponse.isAcknowledged();    // <1>
            // end::delete-user-response
            assertTrue(found);

            // check if deleting the already deleted user again will give us a different response
            deleteUserResponse = client.security().deleteUser(deleteUserRequest, RequestOptions.DEFAULT);
            assertFalse(deleteUserResponse.isAcknowledged());
        }

        {
            DeleteUserRequest deleteUserRequest = new DeleteUserRequest("testUser", RefreshPolicy.IMMEDIATE);

            ActionListener<DeleteUserResponse> listener;
            //tag::delete-user-execute-listener
            listener = new ActionListener<DeleteUserResponse>() {
                @Override
                public void onResponse(DeleteUserResponse deleteUserResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::delete-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::delete-user-execute-async
            client.security().deleteUserAsync(deleteUserRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::delete-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private void addUser(RestHighLevelClient client, String userName, String password) throws IOException {
        User user = new User(userName, Collections.singletonList(userName));
        PutUserRequest request = PutUserRequest.withPassword(user, password.toCharArray(), true, RefreshPolicy.NONE);
        PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
        assertTrue(response.isCreated());
    }

    public void testPutRoleMapping() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            // tag::put-role-mapping-execute
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder()
                .addExpression(FieldRoleMapperExpression.ofUsername("*"))
                .addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com"))
                .build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true,
                Collections.singletonList("superuser"), Collections.emptyList(), rules, null, RefreshPolicy.NONE);
            final PutRoleMappingResponse response = client.security().putRoleMapping(request, RequestOptions.DEFAULT);
            // end::put-role-mapping-execute
            // tag::put-role-mapping-response
            boolean isCreated = response.isCreated(); // <1>
            // end::put-role-mapping-response
            assertTrue(isCreated);
        }

        {
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder()
                .addExpression(FieldRoleMapperExpression.ofUsername("*"))
                .addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com"))
                .build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.emptyList(),
                Collections.singletonList(new TemplateRoleName("{\"source\":\"{{username}}\"}", TemplateRoleName.Format.STRING)),
                rules, null, RefreshPolicy.NONE);
            // tag::put-role-mapping-execute-listener
            ActionListener<PutRoleMappingResponse> listener = new ActionListener<PutRoleMappingResponse>() {
                @Override
                public void onResponse(PutRoleMappingResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-role-mapping-execute-listener

            // avoid unused local warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<PutRoleMappingResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::put-role-mapping-execute-async
            client.security().putRoleMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-role-mapping-execute-async

            assertThat(future.get(), notNullValue());
            assertThat(future.get().isCreated(), is(false));
        }
    }

    public void testGetRoleMappings() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        final TemplateRoleName monitoring = new TemplateRoleName("{\"source\":\"monitoring\"}", TemplateRoleName.Format.STRING);
        final TemplateRoleName template = new TemplateRoleName("{\"source\":\"{{username}}\"}", TemplateRoleName.Format.STRING);

        final RoleMapperExpression rules1 = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofUsername("*"))
            .addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com")).build();
        final PutRoleMappingRequest putRoleMappingRequest1 = new PutRoleMappingRequest("mapping-example-1", true, Collections.emptyList(),
            Arrays.asList(monitoring, template), rules1, null, RefreshPolicy.NONE);
        final PutRoleMappingResponse putRoleMappingResponse1 = client.security().putRoleMapping(putRoleMappingRequest1,
            RequestOptions.DEFAULT);
        boolean isCreated1 = putRoleMappingResponse1.isCreated();
        assertTrue(isCreated1);
        final RoleMapperExpression rules2 = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofGroups(
            "cn=admins,dc=example,dc=com")).build();
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("k1", "v1");
        final PutRoleMappingRequest putRoleMappingRequest2 = new PutRoleMappingRequest("mapping-example-2", true,
            Arrays.asList("superuser"), Collections.emptyList(), rules2, metadata2, RefreshPolicy.NONE);
        final PutRoleMappingResponse putRoleMappingResponse2 = client.security().putRoleMapping(putRoleMappingRequest2,
            RequestOptions.DEFAULT);
        boolean isCreated2 = putRoleMappingResponse2.isCreated();
        assertTrue(isCreated2);

        {
            // tag::get-role-mappings-execute
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest("mapping-example-1");
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            // end::get-role-mappings-execute
            // tag::get-role-mappings-response
            List<ExpressionRoleMapping> mappings = response.getMappings();
            // end::get-role-mappings-response
            assertNotNull(mappings);
            assertThat(mappings.size(), is(1));
            assertThat(mappings.get(0).isEnabled(), is(true));
            assertThat(mappings.get(0).getName(), is("mapping-example-1"));
            assertThat(mappings.get(0).getExpression(), equalTo(rules1));
            assertThat(mappings.get(0).getMetadata(), equalTo(Collections.emptyMap()));
            assertThat(mappings.get(0).getRoles(), iterableWithSize(0));
            assertThat(mappings.get(0).getRoleTemplates(), iterableWithSize(2));
            assertThat(mappings.get(0).getRoleTemplates(), containsInAnyOrder(monitoring, template));
        }

        {
            // tag::get-role-mappings-list-execute
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest("mapping-example-1", "mapping-example-2");
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            // end::get-role-mappings-list-execute
            List<ExpressionRoleMapping> mappings = response.getMappings();
            assertNotNull(mappings);
            assertThat(mappings.size(), is(2));
            for (ExpressionRoleMapping roleMapping : mappings) {
                assertThat(roleMapping.isEnabled(), is(true));
                assertThat(roleMapping.getName(), in(new String[]{"mapping-example-1", "mapping-example-2"}));
                if (roleMapping.getName().equals("mapping-example-1")) {
                    assertThat(roleMapping.getMetadata(), equalTo(Collections.emptyMap()));
                    assertThat(roleMapping.getExpression(), equalTo(rules1));
                    assertThat(roleMapping.getRoles(), emptyIterable());
                    assertThat(roleMapping.getRoleTemplates(), contains(monitoring, template));
                } else {
                    assertThat(roleMapping.getMetadata(), equalTo(metadata2));
                    assertThat(roleMapping.getExpression(), equalTo(rules2));
                    assertThat(roleMapping.getRoles(), contains("superuser"));
                    assertThat(roleMapping.getRoleTemplates(), emptyIterable());
                }
            }
        }

        {
            // tag::get-role-mappings-all-execute
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest();
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            // end::get-role-mappings-all-execute
            List<ExpressionRoleMapping> mappings = response.getMappings();
            assertNotNull(mappings);
            assertThat(mappings.size(), is(2));
            for (ExpressionRoleMapping roleMapping : mappings) {
                assertThat(roleMapping.isEnabled(), is(true));
                assertThat(roleMapping.getName(), in(new String[]{"mapping-example-1", "mapping-example-2"}));
                if (roleMapping.getName().equals("mapping-example-1")) {
                    assertThat(roleMapping.getMetadata(), equalTo(Collections.emptyMap()));
                    assertThat(roleMapping.getExpression(), equalTo(rules1));
                    assertThat(roleMapping.getRoles(), emptyIterable());
                    assertThat(roleMapping.getRoleTemplates(), containsInAnyOrder(monitoring, template));
                } else {
                    assertThat(roleMapping.getMetadata(), equalTo(metadata2));
                    assertThat(roleMapping.getExpression(), equalTo(rules2));
                    assertThat(roleMapping.getRoles(), contains("superuser"));
                    assertThat(roleMapping.getRoleTemplates(), emptyIterable());
                }
            }
        }

        {
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest();
            // tag::get-role-mappings-execute-listener
            ActionListener<GetRoleMappingsResponse> listener = new ActionListener<GetRoleMappingsResponse>() {
                @Override
                public void onResponse(GetRoleMappingsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-role-mappings-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-role-mappings-execute-async
            client.security().getRoleMappingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-role-mappings-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testEnableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        User enable_user = new User("enable_user", Collections.singletonList("superuser"));
        PutUserRequest putUserRequest = PutUserRequest.withPassword(enable_user, password, true, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());

        {
            //tag::enable-user-execute
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            boolean response = client.security().enableUser(request, RequestOptions.DEFAULT);
            //end::enable-user-execute

            assertTrue(response);
        }

        {
            //tag::enable-user-execute-listener
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::enable-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::enable-user-execute-async
            client.security().enableUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::enable-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDisableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        User disable_user = new User("disable_user", Collections.singletonList("superuser"));
        PutUserRequest putUserRequest = PutUserRequest.withPassword(disable_user, password, true, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            //tag::disable-user-execute
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            boolean response = client.security().disableUser(request, RequestOptions.DEFAULT);
            //end::disable-user-execute

            assertTrue(response);
        }

        {
            //tag::disable-user-execute-listener
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::disable-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::disable-user-execute-async
            client.security().disableUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::disable-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetRoles() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        addRole("my_role");
        addRole("my_role2");
        addRole("my_role3");
        {
            //tag::get-roles-request
            GetRolesRequest request = new GetRolesRequest("my_role");
            //end::get-roles-request
            //tag::get-roles-execute
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            //end::get-roles-execute
            //tag::get-roles-response
            List<Role> roles = response.getRoles();
            //end::get-roles-response

            assertNotNull(response);
            assertThat(roles.size(), equalTo(1));
            assertThat(roles.get(0).getName(), equalTo("my_role"));
            assertThat(roles.get(0).getClusterPrivileges().contains("all"), equalTo(true));
        }

        {
            //tag::get-roles-list-request
            GetRolesRequest request = new GetRolesRequest("my_role", "my_role2");
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            //end::get-roles-list-request

            List<Role> roles = response.getRoles();
            assertNotNull(response);
            assertThat(roles.size(), equalTo(2));
            assertThat(roles.get(0).getClusterPrivileges().contains("all"), equalTo(true));
            assertThat(roles.get(1).getClusterPrivileges().contains("all"), equalTo(true));
        }

        {
            //tag::get-roles-all-request
            GetRolesRequest request = new GetRolesRequest();
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            //end::get-roles-all-request

            List<Role> roles = response.getRoles();
            assertNotNull(response);
            // 31 system roles plus the three we created
            assertThat(roles.size(), equalTo(31 + 3));
        }

        {
            GetRolesRequest request = new GetRolesRequest("my_role");
            ActionListener<GetRolesResponse> listener;

            //tag::get-roles-execute-listener
            listener = new ActionListener<GetRolesResponse>() {
                @Override
                public void onResponse(GetRolesResponse getRolesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::get-roles-execute-listener

            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<GetRolesResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::get-roles-execute-async
            client.security().getRolesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::get-roles-execute-async

            final GetRolesResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getRoles().size(), equalTo(1));
            assertThat(response.getRoles().get(0).getName(), equalTo("my_role"));
            assertThat(response.getRoles().get(0).getClusterPrivileges().contains("all"), equalTo(true));
        }
    }

    public void testAuthenticate() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::authenticate-execute
            AuthenticateResponse response = client.security().authenticate(RequestOptions.DEFAULT);
            //end::authenticate-execute

            //tag::authenticate-response
            User user = response.getUser(); // <1>
            boolean enabled = response.enabled(); // <2>
            final String authenticationRealmName = response.getAuthenticationRealm().getName(); // <3>
            final String authenticationRealmType = response.getAuthenticationRealm().getType(); // <4>
            final String lookupRealmName = response.getLookupRealm().getName(); // <5>
            final String lookupRealmType = response.getLookupRealm().getType(); // <6>
            final String authenticationType = response.getAuthenticationType(); // <7>
            final Map<String, Object> tokenInfo = response.getToken(); // <8>
            //end::authenticate-response

            assertThat(user.getUsername(), is("test_user"));
            assertThat(user.getRoles(), contains(new String[]{"admin"}));
            assertThat(user.getFullName(), nullValue());
            assertThat(user.getEmail(), nullValue());
            assertThat(user.getMetadata().isEmpty(), is(true));
            assertThat(enabled, is(true));
            assertThat(authenticationRealmName, is("default_file"));
            assertThat(authenticationRealmType, is("file"));
            assertThat(lookupRealmName, is("default_file"));
            assertThat(lookupRealmType, is("file"));
            assertThat(authenticationType, is("realm"));
            assertThat(tokenInfo, nullValue());
        }

        {
            // tag::authenticate-execute-listener
            ActionListener<AuthenticateResponse> listener = new ActionListener<AuthenticateResponse>() {
                @Override
                public void onResponse(AuthenticateResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::authenticate-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::authenticate-execute-async
            client.security().authenticateAsync(RequestOptions.DEFAULT, listener); // <1>
            // end::authenticate-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testHasPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::has-privileges-request
            HasPrivilegesRequest request = new HasPrivilegesRequest(
                Sets.newHashSet("monitor", "manage"),
                Sets.newHashSet(
                    IndicesPrivileges.builder().indices("logstash-2018-10-05").privileges("read", "write")
                        .allowRestrictedIndices(false).build(),
                    IndicesPrivileges.builder().indices("logstash-2018-*").privileges("read")
                        .allowRestrictedIndices(true).build()
                ),
                null
            );
            //end::has-privileges-request

            //tag::has-privileges-execute
            HasPrivilegesResponse response = client.security().hasPrivileges(request, RequestOptions.DEFAULT);
            //end::has-privileges-execute

            //tag::has-privileges-response
            boolean hasMonitor = response.hasClusterPrivilege("monitor"); // <1>
            boolean hasWrite = response.hasIndexPrivilege("logstash-2018-10-05", "write"); // <2>
            boolean hasRead = response.hasIndexPrivilege("logstash-2018-*", "read"); // <3>
            //end::has-privileges-response

            assertThat(response.getUsername(), is("test_user"));
            assertThat(response.hasAllRequested(), is(true));
            assertThat(hasMonitor, is(true));
            assertThat(hasWrite, is(true));
            assertThat(hasRead, is(true));
            assertThat(response.getApplicationPrivileges().entrySet(), emptyIterable());
        }

        {
            HasPrivilegesRequest request = new HasPrivilegesRequest(Collections.singleton("monitor"), null, null);

            // tag::has-privileges-execute-listener
            ActionListener<HasPrivilegesResponse> listener = new ActionListener<HasPrivilegesResponse>() {
                @Override
                public void onResponse(HasPrivilegesResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::has-privileges-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::has-privileges-execute-async
            client.security().hasPrivilegesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::has-privileges-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetUserPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-user-privileges-execute
            GetUserPrivilegesResponse response = client.security().getUserPrivileges(RequestOptions.DEFAULT);
            //end::get-user-privileges-execute

            assertNotNull(response);
            //tag::get-user-privileges-response
            final Set<String> cluster = response.getClusterPrivileges();
            final Set<UserIndicesPrivileges> index = response.getIndicesPrivileges();
            final Set<ApplicationResourcePrivileges> application = response.getApplicationPrivileges();
            final Set<String> runAs = response.getRunAsPrivilege();
            //end::get-user-privileges-response

            assertNotNull(cluster);
            assertThat(cluster, contains("all"));

            assertNotNull(index);
            assertThat(index.size(), is(1));
            final UserIndicesPrivileges indexPrivilege = index.iterator().next();
            assertThat(indexPrivilege.getIndices(), contains("*"));
            assertThat(indexPrivilege.getPrivileges(), contains("all"));
            assertThat(indexPrivilege.getFieldSecurity().size(), is(0));
            assertThat(indexPrivilege.getQueries().size(), is(0));

            assertNotNull(application);
            assertThat(application.size(), is(1));

            assertNotNull(runAs);
            assertThat(runAs, contains("*"));
        }

        {
            //tag::get-user-privileges-execute-listener
            ActionListener<GetUserPrivilegesResponse> listener = new ActionListener<GetUserPrivilegesResponse>() {
                @Override
                public void onResponse(GetUserPrivilegesResponse getUserPrivilegesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::get-user-privileges-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-user-privileges-execute-async
            client.security().getUserPrivilegesAsync(RequestOptions.DEFAULT, listener); // <1>
            // end::get-user-privileges-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearRealmCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::clear-realm-cache-request
            ClearRealmCacheRequest request = new ClearRealmCacheRequest(Collections.emptyList(), Collections.emptyList());
            //end::clear-realm-cache-request
            //tag::clear-realm-cache-execute
            ClearRealmCacheResponse response = client.security().clearRealmCache(request, RequestOptions.DEFAULT);
            //end::clear-realm-cache-execute

            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));

            //tag::clear-realm-cache-response
            List<ClearRealmCacheResponse.Node> nodes = response.getNodes(); // <1>
            //end::clear-realm-cache-response
        }
        {
            //tag::clear-realm-cache-execute-listener
            ClearRealmCacheRequest request = new ClearRealmCacheRequest(Collections.emptyList(), Collections.emptyList());
            ActionListener<ClearRealmCacheResponse> listener = new ActionListener<ClearRealmCacheResponse>() {
                @Override
                public void onResponse(ClearRealmCacheResponse clearRealmCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::clear-realm-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-realm-cache-execute-async
            client.security().clearRealmCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-realm-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearRolesCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::clear-roles-cache-request
            ClearRolesCacheRequest request = new ClearRolesCacheRequest("my_role");
            //end::clear-roles-cache-request
            //tag::clear-roles-cache-execute
            ClearRolesCacheResponse response = client.security().clearRolesCache(request, RequestOptions.DEFAULT);
            //end::clear-roles-cache-execute

            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));

            //tag::clear-roles-cache-response
            List<ClearRolesCacheResponse.Node> nodes = response.getNodes(); // <1>
            //end::clear-roles-cache-response
        }

        {
            //tag::clear-roles-cache-execute-listener
            ClearRolesCacheRequest request = new ClearRolesCacheRequest("my_role");
            ActionListener<ClearRolesCacheResponse> listener = new ActionListener<ClearRolesCacheResponse>() {
                @Override
                public void onResponse(ClearRolesCacheResponse clearRolesCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::clear-roles-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-roles-cache-execute-async
            client.security().clearRolesCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-roles-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearPrivilegesCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::clear-privileges-cache-request
            ClearPrivilegesCacheRequest request = new ClearPrivilegesCacheRequest("my_app"); // <1>
            //end::clear-privileges-cache-request
            //tag::clear-privileges-cache-execute
            ClearPrivilegesCacheResponse response = client.security().clearPrivilegesCache(request, RequestOptions.DEFAULT);
            //end::clear-privileges-cache-execute

            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));

            //tag::clear-privileges-cache-response
            List<ClearPrivilegesCacheResponse.Node> nodes = response.getNodes(); // <1>
            //end::clear-privileges-cache-response
        }

        {
            //tag::clear-privileges-cache-execute-listener
            ClearPrivilegesCacheRequest request = new ClearPrivilegesCacheRequest("my_app");
            ActionListener<ClearPrivilegesCacheResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ClearPrivilegesCacheResponse clearPrivilegesCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::clear-privileges-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-privileges-cache-execute-async
            client.security().clearPrivilegesCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-privileges-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearApiKeyCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::clear-api-key-cache-request
            ClearApiKeyCacheRequest request = ClearApiKeyCacheRequest.clearById(
                "yVGMr3QByxdh1MSaicYx"  // <1>
            );
            //end::clear-api-key-cache-request
            //tag::clear-api-key-cache-execute
            ClearSecurityCacheResponse response = client.security().clearApiKeyCache(request, RequestOptions.DEFAULT);
            //end::clear-api-key-cache-execute

            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));

            //tag::clear-api-key-cache-response
            List<ClearSecurityCacheResponse.Node> nodes = response.getNodes(); // <1>
            //end::clear-api-key-cache-response
        }

        {
            ClearApiKeyCacheRequest request = ClearApiKeyCacheRequest.clearById("yVGMr3QByxdh1MSaicYx");
            //tag::clear-api-key-cache-execute-listener
            ActionListener<ClearSecurityCacheResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ClearSecurityCacheResponse clearSecurityCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::clear-api-key-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-api-key-cache-execute-async
            client.security().clearApiKeyCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-api-key-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearServiceAccountTokenCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::clear-service-account-token-cache-request
            ClearServiceAccountTokenCacheRequest request = new ClearServiceAccountTokenCacheRequest(
                "elastic", // <1>
                "fleet-server", // <2>
                "token1" // <3>
            );
            //end::clear-service-account-token-cache-request
            //tag::clear-service-account-token-cache-execute
            ClearSecurityCacheResponse response = client.security().clearServiceAccountTokenCache(request, RequestOptions.DEFAULT);
            //end::clear-service-account-token-cache-execute

            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));

            //tag::clear-service-account-token-cache-response
            List<ClearSecurityCacheResponse.Node> nodes = response.getNodes(); // <1>
            //end::clear-service-account-token-cache-response
        }

        {
            ClearServiceAccountTokenCacheRequest request = new ClearServiceAccountTokenCacheRequest("elastic", "fleet-server",
                "token1", "token2");
            //tag::clear-service-account-token-cache-execute-listener
            ActionListener<ClearSecurityCacheResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ClearSecurityCacheResponse clearSecurityCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::clear-service-account-token-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-service-account-token-cache-execute-async
            client.security().clearServiceAccountTokenCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-service-account-token-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetSslCertificates() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-certificates-execute
            GetSslCertificatesResponse response = client.security().getSslCertificates(RequestOptions.DEFAULT);
            //end::get-certificates-execute

            assertNotNull(response);

            //tag::get-certificates-response
            List<CertificateInfo> certificates = response.getCertificates(); // <1>
            //end::get-certificates-response

            assertThat(certificates.size(), Matchers.equalTo(9));
            final Iterator<CertificateInfo> it = certificates.iterator();
            CertificateInfo c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("c0ea4216e8ff0fd8"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b8b96c37e332cccb"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.crt"));
            assertThat(c.getFormat(), Matchers.equalTo("PEM"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("d3850b2b1995ad5f"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b8b96c37e332cccb"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b9d497f2924bbe29"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("580db8ad52bb168a4080e1df122a3f56"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("7268203b"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("3151a81eec8d4e34c56a8466a8510bcfbe63cc31"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("223c736a"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
        }

        {
            // tag::get-certificates-execute-listener
            ActionListener<GetSslCertificatesResponse> listener = new ActionListener<GetSslCertificatesResponse>() {
                @Override
                public void onResponse(GetSslCertificatesResponse getSslCertificatesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-certificates-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-certificates-execute-async
            client.security().getSslCertificatesAsync(RequestOptions.DEFAULT, listener); // <1>
            // end::get-certificates-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testChangePassword() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        char[] newPassword =
            new char[]{'n', 'e', 'w', '-', 't', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        User user = new User("change_password_user", Collections.singletonList("superuser"), Collections.emptyMap(), null, null);
        PutUserRequest putUserRequest = PutUserRequest.withPassword(user, password, true, RefreshPolicy.NONE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            //tag::change-password-execute
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", newPassword, RefreshPolicy.NONE);
            boolean response = client.security().changePassword(request, RequestOptions.DEFAULT);
            //end::change-password-execute

            assertTrue(response);
        }
        {
            //tag::change-password-execute-listener
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", password, RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::change-password-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::change-password-execute-async
            client.security().changePasswordAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::change-password-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteRoleMapping() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            // Create role mappings
            final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("*");
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true,
                Collections.singletonList("superuser"), Collections.emptyList(), rules, null, RefreshPolicy.NONE);
            final PutRoleMappingResponse response = client.security().putRoleMapping(request, RequestOptions.DEFAULT);
            boolean isCreated = response.isCreated();
            assertTrue(isCreated);
        }

        {
            // tag::delete-role-mapping-execute
            final DeleteRoleMappingRequest request = new DeleteRoleMappingRequest("mapping-example", RefreshPolicy.NONE);
            final DeleteRoleMappingResponse response = client.security().deleteRoleMapping(request, RequestOptions.DEFAULT);
            // end::delete-role-mapping-execute
            // tag::delete-role-mapping-response
            boolean isFound = response.isFound(); // <1>
            // end::delete-role-mapping-response

            assertTrue(isFound);
        }

        {
            final DeleteRoleMappingRequest request = new DeleteRoleMappingRequest("mapping-example", RefreshPolicy.NONE);
            // tag::delete-role-mapping-execute-listener
            ActionListener<DeleteRoleMappingResponse> listener = new ActionListener<DeleteRoleMappingResponse>() {
                @Override
                public void onResponse(DeleteRoleMappingResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-role-mapping-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-role-mapping-execute-async
            client.security().deleteRoleMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-role-mapping-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteRole() throws Exception {
        RestHighLevelClient client = highLevelClient();
        addRole("testrole");

        {
            // tag::delete-role-request
            DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest(
                "testrole");    // <1>
            // end::delete-role-request

            // tag::delete-role-execute
            DeleteRoleResponse deleteRoleResponse = client.security().deleteRole(deleteRoleRequest, RequestOptions.DEFAULT);
            // end::delete-role-execute

            // tag::delete-role-response
            boolean found = deleteRoleResponse.isFound();    // <1>
            // end::delete-role-response
            assertTrue(found);

            // check if deleting the already deleted role again will give us a different response
            deleteRoleResponse = client.security().deleteRole(deleteRoleRequest, RequestOptions.DEFAULT);
            assertFalse(deleteRoleResponse.isFound());
        }

        {
            DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest("testrole");

            ActionListener<DeleteRoleResponse> listener;
            //tag::delete-role-execute-listener
            listener = new ActionListener<DeleteRoleResponse>() {
                @Override
                public void onResponse(DeleteRoleResponse deleteRoleResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::delete-role-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::delete-role-execute-async
            client.security().deleteRoleAsync(deleteRoleRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::delete-role-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutRole() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-role-request
            final Role role = Role.builder()
                .name("testPutRole")
                .clusterPrivileges(randomSubsetOf(1, Role.ClusterPrivilegeName.ALL_ARRAY))
                .build();
            final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.NONE);
            // end::put-role-request
            // tag::put-role-execute
            final PutRoleResponse response = client.security().putRole(request, RequestOptions.DEFAULT);
            // end::put-role-execute
            // tag::put-role-response
            boolean isCreated = response.isCreated(); // <1>
            // end::put-role-response
            assertTrue(isCreated);
        }

        {
            final Role role = Role.builder()
                .name("testPutRole")
                .clusterPrivileges(randomSubsetOf(1, Role.ClusterPrivilegeName.ALL_ARRAY))
                .build();
            final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.NONE);
            // tag::put-role-execute-listener
            ActionListener<PutRoleResponse> listener = new ActionListener<PutRoleResponse>() {
                @Override
                public void onResponse(PutRoleResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-role-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<PutRoleResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::put-role-execute-async
            client.security().putRoleAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-role-execute-async

            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().isCreated(), is(false)); // false because it has already been created by the sync variant
        }
    }

    private void addRole(String roleName) throws IOException {
        final Role role = Role.builder()
            .name(roleName)
            .clusterPrivileges("all")
            .build();
        final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.IMMEDIATE);
        highLevelClient().security().putRole(request, RequestOptions.DEFAULT);
    }

    public void testCreateToken() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // Setup user
            User token_user = new User("token_user", Collections.singletonList("kibana_user"));
            PutUserRequest putUserRequest = PutUserRequest.withPassword(token_user, "test-user-password".toCharArray(), true,
                RefreshPolicy.IMMEDIATE);
            PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
            assertTrue(putUserResponse.isCreated());
        }
        {
            // tag::create-token-password-request
            final char[] password = new char[]{'t', 'e', 's', 't', '-', 'u', 's', 'e', 'r', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
            CreateTokenRequest createTokenRequest = CreateTokenRequest.passwordGrant("token_user", password);
            // end::create-token-password-request

            // tag::create-token-execute
            CreateTokenResponse createTokenResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            // end::create-token-execute

            // tag::create-token-response
            String accessToken = createTokenResponse.getAccessToken();    // <1>
            String refreshToken = createTokenResponse.getRefreshToken();    // <2>
            // end::create-token-response
            assertNotNull(accessToken);
            assertNotNull(refreshToken);
            assertNotNull(createTokenResponse.getExpiresIn());

            // tag::create-token-refresh-request
            createTokenRequest = CreateTokenRequest.refreshTokenGrant(refreshToken);
            // end::create-token-refresh-request

            CreateTokenResponse refreshResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(refreshResponse.getAccessToken());
            assertNotNull(refreshResponse.getRefreshToken());
        }

        {
            // tag::create-token-client-credentials-request
            CreateTokenRequest createTokenRequest = CreateTokenRequest.clientCredentialsGrant();
            // end::create-token-client-credentials-request

            ActionListener<CreateTokenResponse> listener;
            //tag::create-token-execute-listener
            listener = new ActionListener<CreateTokenResponse>() {
                @Override
                public void onResponse(CreateTokenResponse createTokenResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::create-token-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::create-token-execute-async
            client.security().createTokenAsync(createTokenRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::create-token-execute-async

            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertNotNull(future.get().getAccessToken());
            // "client-credentials" grants aren't refreshable
            assertNull(future.get().getRefreshToken());
        }
    }

    public void testInvalidateToken() throws Exception {
        RestHighLevelClient client = highLevelClient();

        String accessToken;
        String refreshToken;
        {
            // Setup users
            final char[] password = "test-user-password".toCharArray();
            User user = new User("user", Collections.singletonList("kibana_user"));
            PutUserRequest putUserRequest = PutUserRequest.withPassword(user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
            assertTrue(putUserResponse.isCreated());

            User this_user = new User("this_user", Collections.singletonList("kibana_user"));
            PutUserRequest putThisUserRequest = PutUserRequest.withPassword(this_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putThisUserResponse = client.security().putUser(putThisUserRequest, RequestOptions.DEFAULT);
            assertTrue(putThisUserResponse.isCreated());

            User that_user = new User("that_user", Collections.singletonList("kibana_user"));
            PutUserRequest putThatUserRequest = PutUserRequest.withPassword(that_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putThatUserResponse = client.security().putUser(putThatUserRequest, RequestOptions.DEFAULT);
            assertTrue(putThatUserResponse.isCreated());

            User other_user = new User("other_user", Collections.singletonList("kibana_user"));
            PutUserRequest putOtherUserRequest = PutUserRequest.withPassword(other_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putOtherUserResponse = client.security().putUser(putOtherUserRequest, RequestOptions.DEFAULT);
            assertTrue(putOtherUserResponse.isCreated());

            User extra_user = new User("extra_user", Collections.singletonList("kibana_user"));
            PutUserRequest putExtraUserRequest = PutUserRequest.withPassword(extra_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putExtraUserResponse = client.security().putUser(putExtraUserRequest, RequestOptions.DEFAULT);
            assertTrue(putExtraUserResponse.isCreated());

            // Create tokens
            final CreateTokenRequest createTokenRequest = CreateTokenRequest.passwordGrant("user", password);
            final CreateTokenResponse tokenResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            accessToken = tokenResponse.getAccessToken();
            refreshToken = tokenResponse.getRefreshToken();
            final CreateTokenRequest createThisTokenRequest = CreateTokenRequest.passwordGrant("this_user", password);
            final CreateTokenResponse thisTokenResponse = client.security().createToken(createThisTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(thisTokenResponse);
            final CreateTokenRequest createThatTokenRequest = CreateTokenRequest.passwordGrant("that_user", password);
            final CreateTokenResponse thatTokenResponse = client.security().createToken(createThatTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(thatTokenResponse);
            final CreateTokenRequest createOtherTokenRequest = CreateTokenRequest.passwordGrant("other_user", password);
            final CreateTokenResponse otherTokenResponse = client.security().createToken(createOtherTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(otherTokenResponse);
            final CreateTokenRequest createExtraTokenRequest = CreateTokenRequest.passwordGrant("extra_user", password);
            final CreateTokenResponse extraTokenResponse = client.security().createToken(createExtraTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(extraTokenResponse);
        }

        {
            // tag::invalidate-access-token-request
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.accessToken(accessToken);
            // end::invalidate-access-token-request

            // tag::invalidate-token-execute
            InvalidateTokenResponse invalidateTokenResponse =
                client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            // end::invalidate-token-execute

            // tag::invalidate-token-response
            final List<ElasticsearchException> errors = invalidateTokenResponse.getErrors();
            final int invalidatedTokens = invalidateTokenResponse.getInvalidatedTokens();
            final int previouslyInvalidatedTokens = invalidateTokenResponse.getPreviouslyInvalidatedTokens();
            // end::invalidate-token-response
            assertTrue(errors.isEmpty());
            assertThat(invalidatedTokens, equalTo(1));
            assertThat(previouslyInvalidatedTokens, equalTo(0));
        }

        {
            // tag::invalidate-refresh-token-request
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.refreshToken(refreshToken);
            // end::invalidate-refresh-token-request
            InvalidateTokenResponse invalidateTokenResponse =
                client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(1));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }

        {
            // tag::invalidate-user-tokens-request
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.userTokens("other_user");
            // end::invalidate-user-tokens-request
            InvalidateTokenResponse invalidateTokenResponse =
                client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            // We have one refresh and one access token for that user
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(2));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }

        {
            // tag::invalidate-user-realm-tokens-request
            InvalidateTokenRequest invalidateTokenRequest = new InvalidateTokenRequest(null, null, "default_native", "extra_user");
            // end::invalidate-user-realm-tokens-request
            InvalidateTokenResponse invalidateTokenResponse =
                client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            // We have one refresh and one access token for that user in this realm
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(2));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }

        {
            // tag::invalidate-realm-tokens-request
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.realmTokens("default_native");
            // end::invalidate-realm-tokens-request

            ActionListener<InvalidateTokenResponse> listener;
            //tag::invalidate-token-execute-listener
            listener = new ActionListener<InvalidateTokenResponse>() {
                @Override
                public void onResponse(InvalidateTokenResponse invalidateTokenResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::invalidate-token-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<InvalidateTokenResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::invalidate-token-execute-async
            client.security().invalidateTokenAsync(invalidateTokenRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::invalidate-token-execute-async

            final InvalidateTokenResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertTrue(response.getErrors().isEmpty());
            //We still have 4 tokens ( 2 access_tokens and 2 refresh_tokens ) for the default_native realm
            assertThat(response.getInvalidatedTokens(), equalTo(4));
            assertThat(response.getPreviouslyInvalidatedTokens(), equalTo(0));
        }
    }

    public void testGetBuiltinPrivileges() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        {
            //tag::get-builtin-privileges-execute
            GetBuiltinPrivilegesResponse response = client.security().getBuiltinPrivileges(RequestOptions.DEFAULT);
            //end::get-builtin-privileges-execute

            assertNotNull(response);
            //tag::get-builtin-privileges-response
            final Set<String> cluster = response.getClusterPrivileges();
            final Set<String> index = response.getIndexPrivileges();
            //end::get-builtin-privileges-response

            assertThat(cluster, hasItem("all"));
            assertThat(cluster, hasItem("manage"));
            assertThat(cluster, hasItem("monitor"));
            assertThat(cluster, hasItem("manage_security"));

            assertThat(index, hasItem("all"));
            assertThat(index, hasItem("manage"));
            assertThat(index, hasItem("monitor"));
            assertThat(index, hasItem("read"));
            assertThat(index, hasItem("write"));
        }
        {
            // tag::get-builtin-privileges-execute-listener
            ActionListener<GetBuiltinPrivilegesResponse> listener = new ActionListener<GetBuiltinPrivilegesResponse>() {
                @Override
                public void onResponse(GetBuiltinPrivilegesResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-builtin-privileges-execute-listener

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<GetBuiltinPrivilegesResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::get-builtin-privileges-execute-async
            client.security().getBuiltinPrivilegesAsync(RequestOptions.DEFAULT, listener); // <1>
            // end::get-builtin-privileges-execute-async

            final GetBuiltinPrivilegesResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getClusterPrivileges(), hasItem("manage_security"));
            assertThat(response.getIndexPrivileges(), hasItem("read"));
        }
    }

    public void testGetPrivileges() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        final ApplicationPrivilege readTestappPrivilege =
            new ApplicationPrivilege("testapp", "read", Arrays.asList("action:login", "data:read/*"), null);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        final ApplicationPrivilege writeTestappPrivilege =
            new ApplicationPrivilege("testapp", "write", Arrays.asList("action:login", "data:write/*"), metadata);
        final ApplicationPrivilege allTestappPrivilege =
            new ApplicationPrivilege("testapp", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("key2", "value2");
        final ApplicationPrivilege readTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "read", Arrays.asList("action:login", "data:read/*"), metadata2);
        final ApplicationPrivilege writeTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "write", Arrays.asList("action:login", "data:write/*"), null);
        final ApplicationPrivilege allTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);

        {
            List<ApplicationPrivilege> applicationPrivileges = new ArrayList<>();
            applicationPrivileges.add(readTestappPrivilege);
            applicationPrivileges.add(writeTestappPrivilege);
            applicationPrivileges.add(allTestappPrivilege);
            applicationPrivileges.add(readTestapp2Privilege);
            applicationPrivileges.add(writeTestapp2Privilege);
            applicationPrivileges.add(allTestapp2Privilege);
            PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(applicationPrivileges, RefreshPolicy.IMMEDIATE);
            PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest, RequestOptions.DEFAULT);

            assertNotNull(putPrivilegesResponse);
            assertThat(putPrivilegesResponse.wasCreated("testapp", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "read"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "all"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "all"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "read"), is(true));
        }

        {
            //tag::get-privileges-request
            GetPrivilegesRequest request = new GetPrivilegesRequest("testapp", "write");
            //end::get-privileges-request
            //tag::get-privileges-execute
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);
            //end::get-privileges-execute
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(1));
            assertThat(response.getPrivileges().contains(writeTestappPrivilege), equalTo(true));
        }

        {
            //tag::get-all-application-privileges-request
            GetPrivilegesRequest request = GetPrivilegesRequest.getApplicationPrivileges("testapp");
            //end::get-all-application-privileges-request
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);

            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(3));
            final GetPrivilegesResponse expectedResponse =
                new GetPrivilegesResponse(Arrays.asList(readTestappPrivilege, writeTestappPrivilege, allTestappPrivilege));
            assertThat(response, equalTo(expectedResponse));
            //tag::get-privileges-response
            Set<ApplicationPrivilege> privileges = response.getPrivileges();
            //end::get-privileges-response
            for (ApplicationPrivilege privilege : privileges) {
                assertThat(privilege.getApplication(), equalTo("testapp"));
                if (privilege.getName().equals("read")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:read/*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(true));
                } else if (privilege.getName().equals("write")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:write/*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(false));
                    assertThat(privilege.getMetadata().get("key1"), equalTo("value1"));
                } else if (privilege.getName().equals("all")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:write/*", "manage:*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(true));
                }
            }
        }

        {
            //tag::get-all-privileges-request
            GetPrivilegesRequest request = GetPrivilegesRequest.getAllPrivileges();
            //end::get-all-privileges-request
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);

            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(6));
            final GetPrivilegesResponse exptectedResponse =
                new GetPrivilegesResponse(Arrays.asList(readTestappPrivilege, writeTestappPrivilege, allTestappPrivilege,
                    readTestapp2Privilege, writeTestapp2Privilege, allTestapp2Privilege));
            assertThat(response, equalTo(exptectedResponse));
        }

        {
            GetPrivilegesRequest request = new GetPrivilegesRequest("testapp", "read");
            //tag::get-privileges-execute-listener
            ActionListener<GetPrivilegesResponse> listener = new ActionListener<GetPrivilegesResponse>() {
                @Override
                public void onResponse(GetPrivilegesResponse getPrivilegesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::get-privileges-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<GetPrivilegesResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::get-privileges-execute-async
            client.security().getPrivilegesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::get-privileges-execute-async

            final GetPrivilegesResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(1));
            assertThat(response.getPrivileges().contains(readTestappPrivilege), equalTo(true));
        }
    }

    public void testPutPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-privileges-request
            final List<ApplicationPrivilege> privileges = new ArrayList<>();
            privileges.add(ApplicationPrivilege.builder()
                .application("app01")
                .privilege("all")
                .actions(List.of("action:login"))
                .metadata(Collections.singletonMap("k1", "v1"))
                .build());
            privileges.add(ApplicationPrivilege.builder()
                .application("app01")
                .privilege("write")
                .actions(List.of("action:write"))
                .build());
            final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, RefreshPolicy.IMMEDIATE);
            // end::put-privileges-request

            // tag::put-privileges-execute
            final PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest,
                RequestOptions.DEFAULT);
            // end::put-privileges-execute

            final String applicationName = "app01";
            final String privilegeName = "all";
            // tag::put-privileges-response
            final boolean status = putPrivilegesResponse.wasCreated(applicationName, privilegeName); // <1>
            // end::put-privileges-response
            assertThat(status, is(true));
        }

        {
            final List<ApplicationPrivilege> privileges = new ArrayList<>();
            privileges.add(ApplicationPrivilege.builder()
                .application("app01")
                .privilege("all")
                .actions(List.of("action:login"))
                .metadata(Collections.singletonMap("k1", "v1"))
                .build());
            final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, RefreshPolicy.IMMEDIATE);

            // tag::put-privileges-execute-listener
            ActionListener<PutPrivilegesResponse> listener = new ActionListener<PutPrivilegesResponse>() {
                @Override
                public void onResponse(PutPrivilegesResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-privileges-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<PutPrivilegesResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::put-privileges-execute-async
            client.security().putPrivilegesAsync(putPrivilegesRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::put-privileges-execute-async

            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().wasCreated("app01", "all"), is(false));
        }
    }

    public void testDeletePrivilege() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            List<ApplicationPrivilege> applicationPrivileges = new ArrayList<>();
            applicationPrivileges.add(ApplicationPrivilege.builder()
                .application("testapp")
                .privilege("read")
                .actions("action:login", "data:read/*")
                .build());
            applicationPrivileges.add(ApplicationPrivilege.builder()
                .application("testapp")
                .privilege("write")
                .actions("action:login", "data:write/*")
                .build());
            applicationPrivileges.add(ApplicationPrivilege.builder()
                .application("testapp")
                .privilege("all")
                .actions("action:login", "data:write/*")
                .build());
            PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(applicationPrivileges, RefreshPolicy.IMMEDIATE);
            PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest, RequestOptions.DEFAULT);

            assertNotNull(putPrivilegesResponse);
            assertThat(putPrivilegesResponse.wasCreated("testapp", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "read"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "all"), is(true));
        }
        {
            // tag::delete-privileges-request
            DeletePrivilegesRequest request = new DeletePrivilegesRequest(
                "testapp",          // <1>
                "read", "write"); // <2>
            // end::delete-privileges-request

            // tag::delete-privileges-execute
            DeletePrivilegesResponse response = client.security().deletePrivileges(request, RequestOptions.DEFAULT);
            // end::delete-privileges-execute

            // tag::delete-privileges-response
            String application = response.getApplication();        // <1>
            boolean found = response.isFound("read");              // <2>
            // end::delete-privileges-response
            assertThat(application, equalTo("testapp"));
            assertTrue(response.isFound("write"));
            assertTrue(found);

            // check if deleting the already deleted privileges again will give us a different response
            response = client.security().deletePrivileges(request, RequestOptions.DEFAULT);
            assertFalse(response.isFound("write"));
        }
        {
            DeletePrivilegesRequest deletePrivilegesRequest = new DeletePrivilegesRequest("testapp", "all");

            ActionListener<DeletePrivilegesResponse> listener;
            //tag::delete-privileges-execute-listener
            listener = new ActionListener<DeletePrivilegesResponse>() {
                @Override
                public void onResponse(DeletePrivilegesResponse deletePrivilegesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::delete-privileges-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::delete-privileges-execute-async
            client.security().deletePrivilegesAsync(deletePrivilegesRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::delete-privileges-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testCreateApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();

        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
            .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, Object> metadata = CreateApiKeyRequestTests.randomMetadata();
        {
            final String name = randomAlphaOfLength(5);
            // tag::create-api-key-request
            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, metadata);
            // end::create-api-key-request

            // tag::create-api-key-execute
            CreateApiKeyResponse createApiKeyResponse = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            // end::create-api-key-execute

            // tag::create-api-key-response
            SecureString apiKey = createApiKeyResponse.getKey(); // <1>
            Instant apiKeyExpiration = createApiKeyResponse.getExpiration(); // <2>
            // end::create-api-key-response
            assertThat(createApiKeyResponse.getName(), equalTo(name));
            assertNotNull(apiKey);
            assertNotNull(apiKeyExpiration);
        }

        {
            final String name = randomAlphaOfLength(5);
            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, metadata);

            ActionListener<CreateApiKeyResponse> listener;
            // tag::create-api-key-execute-listener
            listener = new ActionListener<CreateApiKeyResponse>() {
                @Override
                public void onResponse(CreateApiKeyResponse createApiKeyResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::create-api-key-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::create-api-key-execute-async
            client.security().createApiKeyAsync(createApiKeyRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::create-api-key-execute-async

            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().getName(), equalTo(name));
            assertNotNull(future.get().getKey());
            assertNotNull(future.get().getExpiration());
        }
    }

    public void testGrantApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();

        final String username = "grant_apikey_user";
        final String passwordString = randomAlphaOfLengthBetween(14, 18);
        final char[] password = passwordString.toCharArray();

        addUser(client, username, passwordString);

        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
            .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());


        final Instant start = Instant.now();
        final Map<String, Object> metadata = CreateApiKeyRequestTests.randomMetadata();
        CheckedConsumer<CreateApiKeyResponse, IOException> apiKeyVerifier = (created) -> {
            final GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(created.getId(), false);
            final GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), iterableWithSize(1));
            final ApiKey apiKeyInfo = getApiKeyResponse.getApiKeyInfos().get(0);
            assertThat(apiKeyInfo.getUsername(), equalTo(username));
            assertThat(apiKeyInfo.getId(), equalTo(created.getId()));
            assertThat(apiKeyInfo.getName(), equalTo(created.getName()));
            assertThat(apiKeyInfo.getExpiration(), equalTo(created.getExpiration()));
            assertThat(apiKeyInfo.isInvalidated(), equalTo(false));
            assertThat(apiKeyInfo.getCreation(), greaterThanOrEqualTo(start));
            assertThat(apiKeyInfo.getCreation(), lessThanOrEqualTo(Instant.now()));
            if (metadata == null) {
                assertThat(apiKeyInfo.getMetadata(), equalTo(Map.of()));
            } else {
                assertThat(apiKeyInfo.getMetadata(), equalTo(metadata));
            }
        };

        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        {
            final String name = randomAlphaOfLength(5);
            // tag::grant-api-key-request
            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, metadata);
            GrantApiKeyRequest.Grant grant = GrantApiKeyRequest.Grant.passwordGrant(username, password);
            GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest(grant, createApiKeyRequest);
            // end::grant-api-key-request

            // tag::grant-api-key-execute
            CreateApiKeyResponse apiKeyResponse = client.security().grantApiKey(grantApiKeyRequest, RequestOptions.DEFAULT);
            // end::grant-api-key-execute

            // tag::grant-api-key-response
            SecureString apiKey = apiKeyResponse.getKey(); // <1>
            Instant apiKeyExpiration = apiKeyResponse.getExpiration(); // <2>
            // end::grant-api-key-response
            assertThat(apiKeyResponse.getName(), equalTo(name));
            assertNotNull(apiKey);
            assertNotNull(apiKeyExpiration);

            apiKeyVerifier.accept(apiKeyResponse);
        }

        {
            final String name = randomAlphaOfLength(5);
            final CreateTokenRequest tokenRequest = CreateTokenRequest.passwordGrant(username, password);
            final CreateTokenResponse token = client.security().createToken(tokenRequest, RequestOptions.DEFAULT);

            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, metadata);
            GrantApiKeyRequest.Grant grant = GrantApiKeyRequest.Grant.accessTokenGrant(token.getAccessToken());
            GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest(grant, createApiKeyRequest);

            ActionListener<CreateApiKeyResponse> listener;
            // tag::grant-api-key-execute-listener
            listener = new ActionListener<CreateApiKeyResponse>() {
                @Override
                public void onResponse(CreateApiKeyResponse createApiKeyResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::grant-api-key-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::grant-api-key-execute-async
            client.security().grantApiKeyAsync(grantApiKeyRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::grant-api-key-execute-async

            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().getName(), equalTo(name));
            assertNotNull(future.get().getKey());
            assertNotNull(future.get().getExpiration());

            apiKeyVerifier.accept(future.get());
        }
    }

    public void testGetApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();

        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
            .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, Object> metadata = CreateApiKeyRequestTests.randomMetadata();
        // Create API Keys
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("k1", roles, expiration, refreshPolicy, metadata);
        CreateApiKeyResponse createApiKeyResponse1 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
        assertThat(createApiKeyResponse1.getName(), equalTo("k1"));
        assertNotNull(createApiKeyResponse1.getKey());

        final ApiKey expectedApiKeyInfo = new ApiKey(createApiKeyResponse1.getName(), createApiKeyResponse1.getId(), Instant.now(),
            Instant.now().plusMillis(expiration.getMillis()), false, "test_user", "default_file", metadata);
        {
            // tag::get-api-key-id-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId(), false);
            // end::get-api-key-id-request

            // tag::get-api-key-execute
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            // end::get-api-key-execute

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-api-key-name-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyName(createApiKeyResponse1.getName(), false);
            // end::get-api-key-name-request

            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-realm-api-keys-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingRealmName("default_file");
            // end::get-realm-api-keys-request

            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-user-api-keys-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingUserName("test_user");
            // end::get-user-api-keys-request

            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-api-keys-owned-by-authenticated-user-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.forOwnedApiKeys();
            // end::get-api-keys-owned-by-authenticated-user-request

            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-all-api-keys-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.forAllApiKeys();
            // end::get-all-api-keys-request

            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            // tag::get-user-realm-api-keys-request
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName("default_file", "test_user");
            // end::get-user-realm-api-keys-request

            // tag::get-api-key-response
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            // end::get-api-key-response

            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }

        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId(), false);

            ActionListener<GetApiKeyResponse> listener;
            // tag::get-api-key-execute-listener
            listener = new ActionListener<GetApiKeyResponse>() {
                @Override
                public void onResponse(GetApiKeyResponse getApiKeyResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-api-key-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<GetApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::get-api-key-execute-async
            client.security().getApiKeyAsync(getApiKeyRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::get-api-key-execute-async

            final GetApiKeyResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);

            assertThat(response.getApiKeyInfos(), is(notNullValue()));
            assertThat(response.getApiKeyInfos().size(), is(1));
            verifyApiKey(response.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
    }

    private void verifyApiKey(final ApiKey actual, final ApiKey expected) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getName(), is(expected.getName()));
        assertThat(actual.getUsername(), is(expected.getUsername()));
        assertThat(actual.getRealm(), is(expected.getRealm()));
        assertThat(actual.isInvalidated(), is(expected.isInvalidated()));
        assertThat(actual.getExpiration(), is(greaterThan(Instant.now())));
        if (expected.getMetadata() == null) {
            assertThat(actual.getMetadata(), equalTo(Map.of()));
        } else {
            assertThat(actual.getMetadata(), equalTo(expected.getMetadata()));
        }
    }

    public void testInvalidateApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();

        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
            .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, Object> metadata = CreateApiKeyRequestTests.randomMetadata();
        // Create API Keys
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("k1", roles, expiration, refreshPolicy, metadata);
        CreateApiKeyResponse createApiKeyResponse1 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
        assertThat(createApiKeyResponse1.getName(), equalTo("k1"));
        assertNotNull(createApiKeyResponse1.getKey());

        {
            // tag::invalidate-api-key-id-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId(), false);
            // end::invalidate-api-key-id-request

            // tag::invalidate-api-key-execute
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);
            // end::invalidate-api-key-execute

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse1.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            // tag::invalidate-api-key-ids-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyIds(
                Arrays.asList("kI3QZHYBnpSXoDRq1XzR", "ko3SZHYBnpSXoDRqk3zm"), false);
            // end::invalidate-api-key-ids-request

            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            assertThat(invalidatedApiKeyIds.size(), equalTo(0));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k2", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse2 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse2.getName(), equalTo("k2"));
            assertNotNull(createApiKeyResponse2.getKey());

            // tag::invalidate-api-key-name-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyName(createApiKeyResponse2.getName(),
                false);
            // end::invalidate-api-key-name-request

            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse2.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k3", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse3 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse3.getName(), equalTo("k3"));
            assertNotNull(createApiKeyResponse3.getKey());

            // tag::invalidate-realm-api-keys-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmName("default_file");
            // end::invalidate-realm-api-keys-request

            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse3.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k4", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse4 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse4.getName(), equalTo("k4"));
            assertNotNull(createApiKeyResponse4.getKey());

            // tag::invalidate-user-api-keys-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingUserName("test_user");
            // end::invalidate-user-api-keys-request

            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse4.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k5", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse5 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse5.getName(), equalTo("k5"));
            assertNotNull(createApiKeyResponse5.getKey());

            // tag::invalidate-user-realm-api-keys-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName("default_file", "test_user");
            // end::invalidate-user-realm-api-keys-request

            // tag::invalidate-api-key-response
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);
            // end::invalidate-api-key-response

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse5.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k6", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse6 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse6.getName(), equalTo("k6"));
            assertNotNull(createApiKeyResponse6.getKey());

            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(createApiKeyResponse6.getId(), false);

            ActionListener<InvalidateApiKeyResponse> listener;
            // tag::invalidate-api-key-execute-listener
            listener = new ActionListener<InvalidateApiKeyResponse>() {
                @Override
                public void onResponse(InvalidateApiKeyResponse invalidateApiKeyResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::invalidate-api-key-execute-listener

            // Avoid unused variable warning
            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<InvalidateApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::invalidate-api-key-execute-async
            client.security().invalidateApiKeyAsync(invalidateApiKeyRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::invalidate-api-key-execute-async

            final InvalidateApiKeyResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            final List<String> invalidatedApiKeyIds = response.getInvalidatedApiKeys();
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse6.getId());
            assertTrue(response.getErrors().isEmpty());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(response.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        }

        {
            createApiKeyRequest = new CreateApiKeyRequest("k7", roles, expiration, refreshPolicy, metadata);
            CreateApiKeyResponse createApiKeyResponse7 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse7.getName(), equalTo("k7"));
            assertNotNull(createApiKeyResponse7.getKey());

            // tag::invalidate-api-keys-owned-by-authenticated-user-request
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.forOwnedApiKeys();
            // end::invalidate-api-keys-owned-by-authenticated-user-request

            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest,
                RequestOptions.DEFAULT);

            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();

            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse7.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }

    }

    public void testGetServiceAccounts() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::get-service-accounts-request
            final GetServiceAccountsRequest getServiceAccountsRequest = new GetServiceAccountsRequest("elastic", "fleet-server");
            // end::get-service-accounts-request

            // tag::get-service-accounts-execute
            final GetServiceAccountsResponse getServiceAccountsResponse =
                client.security().getServiceAccounts(getServiceAccountsRequest, RequestOptions.DEFAULT);
            // end::get-service-accounts-execute

            // tag::get-service-accounts-response
            final ServiceAccountInfo serviceAccountInfo = getServiceAccountsResponse.getServiceAccountInfos().get(0); // <1>
            // end::get-service-accounts-response
            assertThat(serviceAccountInfo.getPrincipal(), equalTo("elastic/fleet-server"));
        }

        {
            // tag::get-service-accounts-request-namespace
            final GetServiceAccountsRequest getServiceAccountsRequest = new GetServiceAccountsRequest("elastic");
            // end::get-service-accounts-request-namespace
        }

        {
            // tag::get-service-accounts-request-all
            final GetServiceAccountsRequest getServiceAccountsRequest = new GetServiceAccountsRequest();
            // end::get-service-accounts-request-all
        }

        {
            final GetServiceAccountsRequest getServiceAccountsRequest = new GetServiceAccountsRequest("elastic", "fleet-server");

            ActionListener<GetServiceAccountsResponse> listener;
            // tag::get-service-accounts-execute-listener
            listener = new ActionListener<GetServiceAccountsResponse>() {
                @Override
                public void onResponse(GetServiceAccountsResponse getServiceAccountsResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-service-accounts-execute-listener

            final PlainActionFuture<GetServiceAccountsResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::get-service-accounts-execute-async
            client.security().getServiceAccountsAsync(getServiceAccountsRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::get-service-accounts-execute-async

            assertNotNull(future.actionGet());
            assertThat(future.actionGet().getServiceAccountInfos().size(), equalTo(1));
            assertThat(future.actionGet().getServiceAccountInfos().get(0).getPrincipal(), equalTo("elastic/fleet-server"));
        }
    }

    public void testCreateServiceAccountToken() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::create-service-account-token-request
            CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
                new CreateServiceAccountTokenRequest("elastic", "fleet-server", "my_token_1");
            // end::create-service-account-token-request

            // tag::create-service-account-token-execute
            CreateServiceAccountTokenResponse createServiceAccountTokenResponse =
                client.security().createServiceAccountToken(createServiceAccountTokenRequest, RequestOptions.DEFAULT);
            // end::create-service-account-token-execute

            // tag::create-service-account-token-response
            final String tokenName = createServiceAccountTokenResponse.getName(); // <1>
            final SecureString tokenValue = createServiceAccountTokenResponse.getValue(); // <2>
            // end::create-service-account-token-response
            assertThat(createServiceAccountTokenResponse.getName(), equalTo("my_token_1"));
            assertNotNull(tokenValue);
        }

        {
            // tag::create-service-account-token-request-auto-name
            CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
                new CreateServiceAccountTokenRequest("elastic", "fleet-server");
            // end::create-service-account-token-request-auto-name

            ActionListener<CreateServiceAccountTokenResponse> listener;
            // tag::create-service-account-token-execute-listener
            listener = new ActionListener<CreateServiceAccountTokenResponse>() {
                @Override
                public void onResponse(CreateServiceAccountTokenResponse createServiceAccountTokenResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::create-service-account-token-execute-listener

            final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::create-service-account-token-execute-async
            client.security().createServiceAccountTokenAsync(createServiceAccountTokenRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::create-service-account-token-execute-async

            assertNotNull(future.actionGet());
            assertNotNull(future.actionGet().getName());
            assertNotNull(future.actionGet().getValue());
        }
    }

    public void testDeleteServiceAccountToken() throws IOException {
        RestHighLevelClient client = highLevelClient();
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
            new CreateServiceAccountTokenRequest("elastic", "fleet-server", "test-token");
        client.security().createServiceAccountToken(createServiceAccountTokenRequest, RequestOptions.DEFAULT);
        {
            // tag::delete-service-account-token-request
            DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest =
                new DeleteServiceAccountTokenRequest("elastic", "fleet-server", "test-token");
            // end::delete-service-account-token-request

            // tag::delete-service-account-token-execute
            DeleteServiceAccountTokenResponse deleteServiceAccountTokenResponse =
                client.security().deleteServiceAccountToken(deleteServiceAccountTokenRequest, RequestOptions.DEFAULT);
            // end::delete-service-account-token-execute

            // tag::delete-service-account-token-response
            final boolean found = deleteServiceAccountTokenResponse.isAcknowledged(); // <1>
            // end::delete-service-account-token-response
            assertTrue(deleteServiceAccountTokenResponse.isAcknowledged());
        }

        client.security().createServiceAccountToken(createServiceAccountTokenRequest, RequestOptions.DEFAULT);
        {
            DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest =
                new DeleteServiceAccountTokenRequest("elastic", "fleet-server", "test-token");
            ActionListener<DeleteServiceAccountTokenResponse> listener;
            // tag::delete-service-account-token-execute-listener
            listener = new ActionListener<DeleteServiceAccountTokenResponse>() {
                @Override
                public void onResponse(DeleteServiceAccountTokenResponse deleteServiceAccountTokenResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-service-account-token-execute-listener

            final PlainActionFuture<DeleteServiceAccountTokenResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::delete-service-account-token-execute-async
            client.security().deleteServiceAccountTokenAsync(deleteServiceAccountTokenRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-service-account-token-execute-async

            assertNotNull(future.actionGet());
            assertTrue(future.actionGet().isAcknowledged());
        }
    }

    public void testGetServiceAccountCredentials() throws IOException {
        RestHighLevelClient client = highLevelClient();
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
            new CreateServiceAccountTokenRequest("elastic", "fleet-server", "token2");
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse =
            client.security().createServiceAccountToken(createServiceAccountTokenRequest, RequestOptions.DEFAULT);
        assertThat(createServiceAccountTokenResponse.getName(), equalTo("token2"));

        {
            // tag::get-service-account-credentials-request
            final GetServiceAccountCredentialsRequest getServiceAccountCredentialsRequest =
                new GetServiceAccountCredentialsRequest("elastic", "fleet-server");
            // end::get-service-account-credentials-request

            // tag::get-service-account-credentials-execute
            final GetServiceAccountCredentialsResponse getServiceAccountCredentialsResponse =
                client.security().getServiceAccountCredentials(getServiceAccountCredentialsRequest, RequestOptions.DEFAULT);
            // end::get-service-account-credentials-execute

            // tag::get-service-account-credentials-response
            final String principal = getServiceAccountCredentialsResponse.getPrincipal(); // <1>
            final List<ServiceTokenInfo> indexTokenInfos = getServiceAccountCredentialsResponse.getIndexTokenInfos(); // <2>
            final String tokenName = indexTokenInfos.get(0).getName(); // <3>
            final String tokenSource = indexTokenInfos.get(0).getSource(); // <4>
            final Collection<String> nodeNames = indexTokenInfos.get(0).getNodeNames(); // <5>
            final List<ServiceTokenInfo> fileTokenInfos
                = getServiceAccountCredentialsResponse.getNodesResponse().getFileTokenInfos(); // <6>
            final NodesResponseHeader fileTokensResponseHeader
                = getServiceAccountCredentialsResponse.getNodesResponse().getHeader(); // <7>
            final int nSuccessful = fileTokensResponseHeader.getSuccessful(); // <8>
            final int nFailed = fileTokensResponseHeader.getFailed(); // <9>
            // end::get-service-account-credentials-response
            assertThat(principal, equalTo("elastic/fleet-server"));
            // Cannot assert exactly one token because there are rare occasions where tests overlap and it will see
            // token created from other tests
            assertThat(indexTokenInfos.size(), greaterThanOrEqualTo(1));
            assertThat(indexTokenInfos.stream().map(ServiceTokenInfo::getName).collect(Collectors.toSet()), hasItem("token2"));
            assertThat(indexTokenInfos.stream().map(ServiceTokenInfo::getSource).collect(Collectors.toSet()), hasItem("index"));
        }

        {
            final GetServiceAccountCredentialsRequest getServiceAccountCredentialsRequest =
                new GetServiceAccountCredentialsRequest("elastic", "fleet-server");

            ActionListener<GetServiceAccountCredentialsResponse> listener;
            // tag::get-service-account-credentials-execute-listener
            listener = new ActionListener<GetServiceAccountCredentialsResponse>() {
                @Override
                public void onResponse(GetServiceAccountCredentialsResponse getServiceAccountCredentialsResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-service-account-credentials-execute-listener

            final PlainActionFuture<GetServiceAccountCredentialsResponse> future = new PlainActionFuture<>();
            listener = future;

            // tag::get-service-account-credentials-execute-async
            client.security().getServiceAccountCredentialsAsync(
                getServiceAccountCredentialsRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::get-service-account-credentials-execute-async

            assertNotNull(future.actionGet());
            assertThat(future.actionGet().getPrincipal(), equalTo("elastic/fleet-server"));
            assertThat(future.actionGet().getIndexTokenInfos().size(), greaterThanOrEqualTo(1));
            assertThat(future.actionGet().getIndexTokenInfos().stream().map(ServiceTokenInfo::getName).collect(Collectors.toSet()),
                hasItem("token2"));
        }
    }

    public void testDelegatePkiAuthentication() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        {
            //tag::delegate-pki-request
            DelegatePkiAuthenticationRequest request = new DelegatePkiAuthenticationRequest(
                Arrays.asList(clientCertificate, intermediateCA));
            //end::delegate-pki-request
            //tag::delegate-pki-execute
            DelegatePkiAuthenticationResponse response = client.security().delegatePkiAuthentication(request, RequestOptions.DEFAULT);
            //end::delegate-pki-execute
            //tag::delegate-pki-response
            String accessToken = response.getAccessToken(); // <1>
            //end::delegate-pki-response

            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + accessToken);
            AuthenticateResponse resp = client.security().authenticate(optionsBuilder.build());
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki1"));
            assertThat(authnRealm.getType(), is("pki"));
            assertThat(resp.getAuthenticationType(), is("token"));
        }

        {
            DelegatePkiAuthenticationRequest request = new DelegatePkiAuthenticationRequest(
                Arrays.asList(clientCertificate, intermediateCA));
            ActionListener<DelegatePkiAuthenticationResponse> listener;

            //tag::delegate-pki-execute-listener
            listener = new ActionListener<DelegatePkiAuthenticationResponse>() {
                @Override
                public void onResponse(DelegatePkiAuthenticationResponse getRolesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::delegate-pki-execute-listener

            assertNotNull(listener);

            // Replace the empty listener by a blocking listener in test
            final PlainActionFuture<DelegatePkiAuthenticationResponse> future = new PlainActionFuture<>();
            listener = future;

            //tag::delegate-pki-execute-async
            client.security().delegatePkiAuthenticationAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::delegate-pki-execute-async

            final DelegatePkiAuthenticationResponse response = future.get(30, TimeUnit.SECONDS);
            String accessToken = response.getAccessToken();
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + accessToken);
            AuthenticateResponse resp = client.security().authenticate(optionsBuilder.build());
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki1"));
            assertThat(authnRealm.getType(), is("pki"));
            assertThat(resp.getAuthenticationType(), is("token"));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/75097")
    public void testNodeEnrollment() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::node-enrollment-execute
            NodeEnrollmentResponse response = client.security().enrollNode(RequestOptions.DEFAULT);
            // end::node-enrollment-execute

            // tag::node-enrollment-response
            String httpCaKey = response.getHttpCaKey(); // <1>
            String httpCaCert = response.getHttpCaCert(); // <2>
            String transportKey = response.getTransportKey(); // <3>
            String transportCert = response.getTransportCert(); // <4>
            List<String> nodesAddresses = response.getNodesAddresses();  // <5>
            // end::node-enrollment-response
        }

        {
            // tag::node-enrollment-execute-listener
            ActionListener<NodeEnrollmentResponse> listener =
                new ActionListener<NodeEnrollmentResponse>() {
                    @Override
                    public void onResponse(NodeEnrollmentResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }};
            // end::node-enrollment-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::node-enrollment-execute-async
            client.security().enrollNodeAsync(RequestOptions.DEFAULT, listener);
            // end::node-enrollment-execute-async
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/75097")
    public void testKibanaEnrollment() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::kibana-enrollment-execute
            KibanaEnrollmentResponse response = client.security().enrollKibana(RequestOptions.DEFAULT);
            // end::kibana-enrollment-execute

            // tag::kibana-enrollment-response
            SecureString token = response.getToken(); // <1>
            String httoCa = response.getHttpCa(); // <2>
            // end::kibana-enrollment-response
            assertNotNull(token);
        }

        {
            // tag::kibana-enrollment-execute-listener
            ActionListener<KibanaEnrollmentResponse> listener =
                new ActionListener<KibanaEnrollmentResponse>() {
                    @Override
                    public void onResponse(KibanaEnrollmentResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }};
            // end::kibana-enrollment-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::kibana-enrollment-execute-async
            client.security().enrollKibanaAsync(RequestOptions.DEFAULT, listener);
            // end::kibana-enrollment-execute-async
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private X509Certificate readCertForPkiDelegation(String certificateName) throws Exception {
        Path path = getDataPath("/org/elasticsearch/client/security/delegate_pki/" + certificateName);
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
