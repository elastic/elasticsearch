/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.apikey;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class ApiKeySingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        builder.put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testQueryWithExpiredKeys() throws InterruptedException {
        final String id1 = client().execute(
            CreateApiKeyAction.INSTANCE,
            new CreateApiKeyRequest("expired-shortly", null, TimeValue.timeValueMillis(1), null)
        ).actionGet().getId();
        final String id2 = client().execute(
            CreateApiKeyAction.INSTANCE,
            new CreateApiKeyRequest("long-lived", null, TimeValue.timeValueDays(1), null)
        ).actionGet().getId();
        Thread.sleep(10); // just to be 100% sure that the 1st key is expired when we search for it

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.idsQuery().addIds(id1, id2))
                .filter(QueryBuilders.rangeQuery("expiration").from(Instant.now().toEpochMilli()))
        );
        final QueryApiKeyResponse queryApiKeyResponse = client().execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest).actionGet();
        assertThat(queryApiKeyResponse.getItems().length, equalTo(1));
        assertThat(queryApiKeyResponse.getItems()[0].getApiKey().getId(), equalTo(id2));
        assertThat(queryApiKeyResponse.getItems()[0].getApiKey().getName(), equalTo("long-lived"));
        assertThat(queryApiKeyResponse.getItems()[0].getSortValues(), emptyArray());
    }

    public void testCreatingApiKeyWithNoAccess() {
        final PutUserRequest putUserRequest = new PutUserRequest();
        final String username = randomAlphaOfLength(8);
        putUserRequest.username(username);
        final SecureString password = new SecureString("super-strong-password".toCharArray());
        putUserRequest.passwordHash(Hasher.PBKDF2.hash(password));
        putUserRequest.roles(Strings.EMPTY_ARRAY);
        client().execute(PutUserAction.INSTANCE, putUserRequest).actionGet();

        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.getGrant().setType("password");
        grantApiKeyRequest.getGrant().setUsername(username);
        grantApiKeyRequest.getGrant().setPassword(password);
        grantApiKeyRequest.getApiKeyRequest().setName(randomAlphaOfLength(8));
        grantApiKeyRequest.getApiKeyRequest()
            .setRoleDescriptors(
                List.of(
                    new RoleDescriptor(
                        "x",
                        new String[] { "all" },
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices("*")
                                .privileges("all")
                                .allowRestrictedIndices(true)
                                .build() },
                        null,
                        null,
                        null,
                        null,
                        null
                    )
                )
            );
        final CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();

        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString(
                (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8)
            );

        // No cluster access
        final ElasticsearchSecurityException e1 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(MainAction.INSTANCE, new MainRequest())
                .actionGet()
        );
        assertThat(e1.status().getStatus(), equalTo(403));
        assertThat(e1.getMessage(), containsString("is unauthorized for API key"));

        // No index access
        final ElasticsearchSecurityException e2 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(
                    CreateIndexAction.INSTANCE,
                    new CreateIndexRequest(randomFrom(randomAlphaOfLengthBetween(3, 8), SECURITY_MAIN_ALIAS))
                )
                .actionGet()
        );
        assertThat(e2.status().getStatus(), equalTo(403));
        assertThat(e2.getMessage(), containsString("is unauthorized for API key"));
    }

    public void testServiceAccountApiKey() throws IOException {
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            randomAlphaOfLength(8)
        );
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse = client().execute(
            CreateServiceAccountTokenAction.INSTANCE,
            createServiceAccountTokenRequest
        ).actionGet();

        final CreateApiKeyResponse createApiKeyResponse = client().filterWithHeader(
            Map.of("Authorization", "Bearer " + createServiceAccountTokenResponse.getValue())
        ).execute(CreateApiKeyAction.INSTANCE, new CreateApiKeyRequest(randomAlphaOfLength(8), null, null)).actionGet();

        final Map<String, Object> apiKeyDocument = getApiKeyDocument(createApiKeyResponse.getId());

        @SuppressWarnings("unchecked")
        final Map<String, Object> fleetServerRoleDescriptor = (Map<String, Object>) apiKeyDocument.get("limited_by_role_descriptors");
        assertThat(fleetServerRoleDescriptor.size(), equalTo(1));
        assertThat(fleetServerRoleDescriptor, hasKey("elastic/fleet-server"));

        @SuppressWarnings("unchecked")
        final Map<String, ?> descriptor = (Map<String, ?>) fleetServerRoleDescriptor.get("elastic/fleet-server");

        final RoleDescriptor roleDescriptor = RoleDescriptor.parse(
            "elastic/fleet-server",
            XContentTestUtils.convertToXContent(descriptor, XContentType.JSON),
            false,
            XContentType.JSON
        );
        assertThat(roleDescriptor, equalTo(ServiceAccountService.getServiceAccounts().get("elastic/fleet-server").roleDescriptor()));
    }

    public void testGetApiKeyWorksForTheApiKeyItself() {
        final String apiKeyName = randomAlphaOfLength(10);
        final CreateApiKeyResponse createApiKeyResponse = client().execute(
            CreateApiKeyAction.INSTANCE,
            new CreateApiKeyRequest(
                apiKeyName,
                List.of(new RoleDescriptor("x", new String[] { "manage_own_api_key", "manage_token" }, null, null, null, null, null, null)),
                null,
                null
            )
        ).actionGet();

        final String apiKeyId = createApiKeyResponse.getId();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));

        // Works for both the API key itself or the token created by it
        final Client clientKey1;
        if (randomBoolean()) {
            clientKey1 = client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        } else {
            final CreateTokenResponse createTokenResponse = new CreateTokenRequestBuilder(
                client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue)),
                CreateTokenAction.INSTANCE
            ).setGrantType("client_credentials").get();
            clientKey1 = client().filterWithHeader(Map.of("Authorization", "Bearer " + createTokenResponse.getTokenString()));
        }

        // Can get its own info
        final GetApiKeyResponse getApiKeyResponse = clientKey1.execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.builder().apiKeyId(apiKeyId).ownedByAuthenticatedUser(randomBoolean()).build()
        ).actionGet();
        assertThat(getApiKeyResponse.getApiKeyInfos().length, equalTo(1));
        assertThat(getApiKeyResponse.getApiKeyInfos()[0].getId(), equalTo(apiKeyId));

        // Cannot get any other keys
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> clientKey1.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.builder().build()).actionGet()
        );
        assertThat(e.getMessage(), containsString("unauthorized for API key id [" + apiKeyId + "]"));
    }

    public void testGrantApiKeyForUserWithRunAs() throws IOException {
        final TestSecurityClient securityClient = getSecurityClient();
        securityClient.putRole(new RoleDescriptor("user1_role", new String[] { "manage_token" }, null, new String[] { "user2", "user4" }));
        securityClient.putRole(new RoleDescriptor("user2_role", new String[] { "monitor", "read_pipeline" }, null, null));
        final SecureString user1Password = new SecureString("user1-strong-password".toCharArray());
        securityClient.putUser(new User("user1", "user1_role"), user1Password);
        securityClient.putUser(new User("user2", "user2_role"), new SecureString("user2-strong-password".toCharArray()));
        securityClient.putUser(new User("user3", "user3_role"), new SecureString("user3-strong-password".toCharArray()));

        // Success: user1 runas user2
        final GrantApiKeyRequest grantApiKeyRequest = buildGrantApiKeyRequest("user1", user1Password, "user2");
        final CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();
        final String apiKeyId = createApiKeyResponse.getId();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        assertThat(securityClient.getApiKey(apiKeyId).getUsername(), equalTo("user2"));
        final Client clientWithGrantedKey = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        // The API key has privileges (inherited from user2) to check cluster health
        clientWithGrantedKey.execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest()).actionGet();
        // If the API key is granted with limiting descriptors, it should not be able to read pipeline
        if (grantApiKeyRequest.getApiKeyRequest().getRoleDescriptors().isEmpty()) {
            clientWithGrantedKey.execute(GetPipelineAction.INSTANCE, new GetPipelineRequest()).actionGet();
        } else {
            assertThat(
                expectThrows(
                    ElasticsearchSecurityException.class,
                    () -> clientWithGrantedKey.execute(GetPipelineAction.INSTANCE, new GetPipelineRequest()).actionGet()
                ).getMessage(),
                containsString("unauthorized")
            );
        }
        // The API key does not have privileges to create oauth2 token (i.e. it does not inherit privileges from user1)
        assertThat(
            expectThrows(
                ElasticsearchSecurityException.class,
                () -> new CreateTokenRequestBuilder(clientWithGrantedKey, CreateTokenAction.INSTANCE).setGrantType("client_credentials")
                    .get()
            ).getMessage(),
            containsString("unauthorized")
        );

        // Failure 1: user1 run-as user3 but does not have the corresponding run-as privilege
        final GrantApiKeyRequest grantApiKeyRequest1 = buildGrantApiKeyRequest("user1", user1Password, "user3");
        final ElasticsearchSecurityException e1 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest1).actionGet()
        );
        assertThat(
            e1.getMessage(),
            containsString(
                "action [cluster:admin/xpack/security/user/authenticate] is unauthorized "
                    + "for user [user1] with effective roles [user1_role]"
                    + ", because user [user1] is unauthorized to run as [user3]"
            )
        );

        // Failure 2: user1 run-as user4 but user4 does not exist
        final GrantApiKeyRequest grantApiKeyRequest2 = buildGrantApiKeyRequest("user1", user1Password, "user4");
        final ElasticsearchSecurityException e2 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest2).actionGet()
        );
        assertThat(
            e2.getMessage(),
            containsString(
                "action [cluster:admin/xpack/security/user/authenticate] is unauthorized "
                    + "for user [user1] with effective roles [user1_role]"
                    + ", because user [user1] is unauthorized to run as [user4]"
            )
        );

        // Failure 3: user1's token run-as user2, but the token itself is a run-as
        final TestSecurityClient.OAuth2Token oAuth2Token3 = getSecurityClient(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, new SecureString(TEST_PASSWORD.toCharArray())))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "user1")
                .build()
        ).createTokenWithClientCredentialsGrant();
        final GrantApiKeyRequest grantApiKeyRequest3 = new GrantApiKeyRequest();
        grantApiKeyRequest3.getApiKeyRequest().setName("granted-api-key-must-not-have-chained-runas");
        grantApiKeyRequest3.getGrant().setType("access_token");
        grantApiKeyRequest3.getGrant().setAccessToken(new SecureString(oAuth2Token3.accessToken().toCharArray()));
        grantApiKeyRequest3.getGrant().setRunAsUsername("user2");
        final ElasticsearchStatusException e3 = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest3).actionGet()
        );
        assertThat(e3.getMessage(), containsString("the provided grant credentials do not support run-as"));

        // Failure 4: user1 run-as user4 and creates a token. The token is used for GrantApiKey. But the token loses the run-as
        // privileges when it is used.
        securityClient.putRole(new RoleDescriptor("user4_role", new String[] { "manage_token" }, null, null));
        securityClient.putUser(new User("user4", "user4_role"), new SecureString("user4-strong-password".toCharArray()));
        final TestSecurityClient.OAuth2Token oAuth2Token4 = getSecurityClient(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("user1", user1Password.clone()))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "user4")
                .build()
        ).createTokenWithClientCredentialsGrant();
        // drop user1's run-as privilege for user4
        securityClient.putRole(new RoleDescriptor("user1_role", new String[] { "manage_token" }, null, new String[] { "user2" }));
        final GrantApiKeyRequest grantApiKeyRequest4 = new GrantApiKeyRequest();
        grantApiKeyRequest4.getApiKeyRequest().setName("granted-api-key-will-check-token-run-as-privilege");
        grantApiKeyRequest4.getGrant().setType("access_token");
        grantApiKeyRequest4.getGrant().setAccessToken(new SecureString(oAuth2Token4.accessToken().toCharArray()));
        final ElasticsearchStatusException e4 = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest4).actionGet()
        );
        assertThat(
            e4.getMessage(),
            containsString(
                "action [cluster:admin/xpack/security/user/authenticate] is unauthorized "
                    + "for user [user1] with effective roles [user1_role]"
                    + ", because user [user1] is unauthorized to run as [user4]"
            )
        );
    }

    private GrantApiKeyRequest buildGrantApiKeyRequest(String username, SecureString password, String runAsUsername) throws IOException {
        final SecureString clonedPassword = password.clone();
        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        // randomly use either password or access token grant
        grantApiKeyRequest.getApiKeyRequest().setName("granted-api-key-for-" + username + "-runas-" + runAsUsername);
        if (randomBoolean()) {
            grantApiKeyRequest.getApiKeyRequest()
                .setRoleDescriptors(List.of(new RoleDescriptor(randomAlphaOfLengthBetween(3, 8), new String[] { "monitor" }, null, null)));
        }
        final Grant grant = grantApiKeyRequest.getGrant();
        grant.setRunAsUsername(runAsUsername);
        if (randomBoolean()) {
            grant.setType("password");
            grant.setUsername(username);
            grant.setPassword(clonedPassword);
        } else {
            final TestSecurityClient.OAuth2Token oAuth2Token;
            if (randomBoolean()) {
                oAuth2Token = getSecurityClient().createToken(new UsernamePasswordToken(username, clonedPassword));
            } else {
                oAuth2Token = getSecurityClient(
                    RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(username, clonedPassword)).build()
                ).createTokenWithClientCredentialsGrant();
            }
            grant.setType("access_token");
            grant.setAccessToken(new SecureString(oAuth2Token.accessToken().toCharArray()));
        }
        return grantApiKeyRequest;
    }

    private Map<String, Object> getApiKeyDocument(String apiKeyId) {
        final GetResponse getResponse = client().execute(GetAction.INSTANCE, new GetRequest(".security-7", apiKeyId)).actionGet();
        return getResponse.getSource();
    }
}
