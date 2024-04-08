/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.apikey;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
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

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeySingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        builder.put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true);
        builder.put("xpack.security.authc.realms.jwt.jwt1.order", 2)
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", "https://audience.example.com/")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", "https://issuer.example.com/")
            .put(
                "xpack.security.authc.realms.jwt.jwt1.pkc_jwkset_path",
                getDataPath("/org/elasticsearch/xpack/security/authc/apikey/rsa-public-jwkset.json")
            )
            .put("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "NONE")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.name", "name")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.dn", "dn")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.groups", "roles")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.mail", "mail");
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testQueryWithExpiredKeys() throws InterruptedException {
        CreateApiKeyRequest request1 = new CreateApiKeyRequest("expired-shortly", null, TimeValue.timeValueMillis(1), null);
        request1.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        final String id1 = client().execute(CreateApiKeyAction.INSTANCE, request1).actionGet().getId();
        CreateApiKeyRequest request2 = new CreateApiKeyRequest("long-lived", null, TimeValue.timeValueDays(1), null);
        request2.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        final String id2 = client().execute(CreateApiKeyAction.INSTANCE, request2).actionGet().getId();
        Thread.sleep(10); // just to be 100% sure that the 1st key is expired when we search for it

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.idsQuery().addIds(id1, id2))
                .filter(QueryBuilders.rangeQuery("expiration").from(Instant.now().toEpochMilli()))
        );
        final QueryApiKeyResponse queryApiKeyResponse = client().execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest).actionGet();
        assertThat(queryApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        assertThat(queryApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo().getId(), equalTo(id2));
        assertThat(queryApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo().getName(), equalTo("long-lived"));
        assertThat(queryApiKeyResponse.getApiKeyInfoList().get(0).sortValues(), emptyArray());
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
        grantApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));
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
                .execute(TransportNodesInfoAction.TYPE, new NodesInfoRequest())
                .actionGet()
        );
        assertThat(e1.status().getStatus(), equalTo(403));
        assertThat(e1.getMessage(), containsString("is unauthorized for API key"));

        // No index access
        final ElasticsearchSecurityException e2 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(
                    TransportCreateIndexAction.TYPE,
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

        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLength(8), null, null);
        createApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));
        final CreateApiKeyResponse createApiKeyResponse = client().filterWithHeader(
            Map.of("Authorization", "Bearer " + createServiceAccountTokenResponse.getValue())
        ).execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).actionGet();

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
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(
            randomAlphaOfLength(10),
            List.of(new RoleDescriptor("x", new String[] { "manage_own_api_key", "manage_token" }, null, null, null, null, null, null)),
            null,
            null
        );
        createApiKeyRequest.setRefreshPolicy(randomFrom(WAIT_UNTIL, IMMEDIATE));
        final CreateApiKeyResponse createApiKeyResponse = client().execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).actionGet();

        final String apiKeyId = createApiKeyResponse.getId();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));

        // Works for both the API key itself or the token created by it
        final Client clientKey1;
        if (randomBoolean()) {
            clientKey1 = client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        } else {
            final CreateTokenResponse createTokenResponse = new CreateTokenRequestBuilder(
                client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue))
            ).setGrantType("client_credentials").get();
            clientKey1 = client().filterWithHeader(Map.of("Authorization", "Bearer " + createTokenResponse.getTokenString()));
        }

        // Can get its own info
        final GetApiKeyResponse getApiKeyResponse = clientKey1.execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.builder().apiKeyId(apiKeyId).ownedByAuthenticatedUser(randomBoolean()).build()
        ).actionGet();
        assertThat(getApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        assertThat(getApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo().getId(), is(apiKeyId));

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
        ApiKey apiKey = securityClient.getApiKey(apiKeyId);
        assertThat(apiKey.getUsername(), equalTo("user2"));
        assertThat(apiKey.getRealm(), equalTo("index"));
        assertThat(apiKey.getRealmType(), equalTo("native"));
        final Client clientWithGrantedKey = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        // The API key has privileges (inherited from user2) to check cluster health
        clientWithGrantedKey.execute(TransportClusterHealthAction.TYPE, new ClusterHealthRequest()).actionGet();
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
                () -> new CreateTokenRequestBuilder(clientWithGrantedKey).setGrantType("client_credentials").get()
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
        grantApiKeyRequest3.setRefreshPolicy(randomFrom(NONE, WAIT_UNTIL, IMMEDIATE));
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
        grantApiKeyRequest4.setRefreshPolicy(randomFrom(NONE, WAIT_UNTIL, IMMEDIATE));
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

    public void testGrantAPIKeyFromTokens() throws IOException {
        final String jwtToken;
        try (var in = getDataInputStream("/org/elasticsearch/xpack/security/authc/apikey/serialized-signed-RS256-jwt.txt")) {
            jwtToken = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        getSecurityClient().putRole(new RoleDescriptor("user1_role", new String[] { "manage_token" }, null, null));
        String role_mapping_rules = """
            {
              "enabled": true,
              "roles": "user1_role",
              "rules": {
                "all": [
                  {
                    "field": {
                      "realm.name": "jwt1"
                    }
                  },
                  {
                    "field": {
                      "username": "user1"
                    }
                  }
                ]
              }
            }
            """;
        getSecurityClient().putRoleMapping(
            "user1_role_mapping",
            XContentHelper.convertToMap(XContentType.JSON.xContent(), role_mapping_rules, true)
        );
        // grant API Key for regular ES access tokens (itself created from JWT credentials)
        {
            // get ES access token from JWT
            final TestSecurityClient.OAuth2Token oAuth2Token = getSecurityClient(
                RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + jwtToken).build()
            ).createTokenWithClientCredentialsGrant();
            String apiKeyName = randomAlphaOfLength(8);
            GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
            grantApiKeyRequest.getGrant().setType("access_token");
            grantApiKeyRequest.getGrant().setAccessToken(new SecureString(oAuth2Token.accessToken().toCharArray()));
            grantApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
            grantApiKeyRequest.getApiKeyRequest().setName(apiKeyName);
            CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();
            // use the API Key to check it's legit
            assertThat(createApiKeyResponse.getName(), is(apiKeyName));
            assertThat(createApiKeyResponse.getId(), notNullValue());
            assertThat(createApiKeyResponse.getKey(), notNullValue());
            final String apiKeyId = createApiKeyResponse.getId();
            final String base64ApiKeyKeyValue = Base64.getEncoder()
                .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8));
            AuthenticateResponse authenticateResponse = client().filterWithHeader(
                Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue)
            ).execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE).actionGet();
            assertThat(authenticateResponse.authentication().getEffectiveSubject().getUser().principal(), is("user1"));
            assertThat(authenticateResponse.authentication().getAuthenticationType(), is(Authentication.AuthenticationType.API_KEY));
            // BUT client_authentication is not supported with the ES access token
            {
                GrantApiKeyRequest wrongGrantApiKeyRequest = new GrantApiKeyRequest();
                wrongGrantApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));
                wrongGrantApiKeyRequest.getApiKeyRequest().setName(randomAlphaOfLength(8));
                wrongGrantApiKeyRequest.getGrant().setType("access_token");
                wrongGrantApiKeyRequest.getGrant().setAccessToken(new SecureString(oAuth2Token.accessToken().toCharArray()));
                wrongGrantApiKeyRequest.getGrant()
                    .setClientAuthentication(new Grant.ClientAuthentication(new SecureString("whatever".toCharArray())));
                ElasticsearchSecurityException e = expectThrows(
                    ElasticsearchSecurityException.class,
                    () -> client().execute(GrantApiKeyAction.INSTANCE, wrongGrantApiKeyRequest).actionGet()
                );
                assertThat(e.getMessage(), containsString("[client_authentication] not supported with the supplied access_token type"));
            }
        }
        // grant API Key for JWT token
        {
            String apiKeyName = randomAlphaOfLength(8);
            GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
            grantApiKeyRequest.getGrant().setType("access_token");
            grantApiKeyRequest.getGrant().setAccessToken(new SecureString(jwtToken.toCharArray()));
            grantApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
            grantApiKeyRequest.getApiKeyRequest().setName(apiKeyName);
            // client authentication is ignored for JWTs that don't require it
            if (randomBoolean()) {
                grantApiKeyRequest.getGrant()
                    .setClientAuthentication(new Grant.ClientAuthentication(new SecureString("whatever".toCharArray())));
            }
            CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();
            // use the API Key to check it's legit
            assertThat(createApiKeyResponse.getName(), is(apiKeyName));
            assertThat(createApiKeyResponse.getId(), notNullValue());
            assertThat(createApiKeyResponse.getKey(), notNullValue());
            final String apiKeyId = createApiKeyResponse.getId();
            final String base64ApiKeyKeyValue = Base64.getEncoder()
                .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8));
            AuthenticateResponse authenticateResponse = client().filterWithHeader(
                Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue)
            ).execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE).actionGet();
            assertThat(authenticateResponse.authentication().getEffectiveSubject().getUser().principal(), is("user1"));
            assertThat(authenticateResponse.authentication().getAuthenticationType(), is(Authentication.AuthenticationType.API_KEY));
        }
    }

    public void testInvalidateApiKeyWillRecordTimestamp() {
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(
            randomAlphaOfLengthBetween(3, 8),
            null,
            TimeValue.timeValueMillis(randomLongBetween(1, 1000)),
            null
        );
        createApiKeyRequest.setRefreshPolicy(randomFrom(WAIT_UNTIL, IMMEDIATE));
        final String apiKeyId = client().execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).actionGet().getId();
        assertThat(getApiKeyDocument(apiKeyId).get("invalidation_time"), nullValue());

        final long start = Instant.now().toEpochMilli();
        final List<String> invalidatedApiKeys = client().execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, true)
        ).actionGet().getInvalidatedApiKeys();
        final long finish = Instant.now().toEpochMilli();

        assertThat(invalidatedApiKeys, equalTo(List.of(apiKeyId)));
        final Map<String, Object> apiKeyDocument = getApiKeyDocument(apiKeyId);
        assertThat(apiKeyDocument.get("api_key_invalidated"), is(true));
        assertThat(apiKeyDocument.get("invalidation_time"), instanceOf(Long.class));
        final long invalidationTime = (long) apiKeyDocument.get("invalidation_time");
        assertThat(invalidationTime, greaterThanOrEqualTo(start));
        assertThat(invalidationTime, lessThanOrEqualTo(finish));

        // Invalidate it again won't change the timestamp
        client().execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, true)).actionGet();
        assertThat((long) getApiKeyDocument(apiKeyId).get("invalidation_time"), equalTo(invalidationTime));
    }

    public void testCreateCrossClusterApiKey() throws IOException {
        final var request = CreateCrossClusterApiKeyRequest.withNameAndAccess(randomAlphaOfLengthBetween(3, 8), """
            {
              "search": [ {"names": ["logs"]} ]
            }""");
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        client().execute(CreateCrossClusterApiKeyAction.INSTANCE, request, future);
        final CreateApiKeyResponse createApiKeyResponse = future.actionGet();

        final String apiKeyId = createApiKeyResponse.getId();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));

        // cross cluster API key cannot be used for regular actions
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE)
                .actionGet()
        );
        assertThat(
            e.getMessage(),
            containsString("authentication expected API key type of [rest], but API key [" + apiKeyId + "] has type [cross_cluster]")
        );

        // Check the API key attributes with raw document
        final Map<String, Object> document = client().execute(TransportGetAction.TYPE, new GetRequest(SECURITY_MAIN_ALIAS, apiKeyId))
            .actionGet()
            .getSource();
        assertThat(document.get("type"), equalTo("cross_cluster"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> roleDescriptors = (Map<String, Object>) document.get("role_descriptors");
        assertThat(roleDescriptors.keySet(), contains("cross_cluster"));
        @SuppressWarnings("unchecked")
        final RoleDescriptor actualRoleDescriptor = RoleDescriptor.parse(
            "cross_cluster",
            XContentTestUtils.convertToXContent((Map<String, Object>) roleDescriptors.get("cross_cluster"), XContentType.JSON),
            false,
            XContentType.JSON
        );

        final RoleDescriptor expectedRoleDescriptor = new RoleDescriptor(
            "cross_cluster",
            new String[] { "cross_cluster_search" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build() },
            null
        );
        assertThat(actualRoleDescriptor, equalTo(expectedRoleDescriptor));
        assertThat((Map<?, ?>) document.get("limited_by_role_descriptors"), anEmptyMap());

        // Check the API key attributes with Get API
        final GetApiKeyResponse getApiKeyResponse = client().execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.builder().apiKeyId(apiKeyId).withLimitedBy(randomBoolean()).build()
        ).actionGet();
        assertThat(getApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        ApiKey getApiKeyInfo = getApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo();
        assertThat(getApiKeyInfo.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(getApiKeyInfo.getRoleDescriptors(), contains(expectedRoleDescriptor));
        assertThat(getApiKeyInfo.getLimitedBy(), nullValue());
        assertThat(getApiKeyInfo.getMetadata(), anEmptyMap());
        assertThat(getApiKeyInfo.getUsername(), equalTo("test_user"));
        assertThat(getApiKeyInfo.getRealm(), equalTo("file"));
        assertThat(getApiKeyInfo.getRealmType(), equalTo("file"));

        // Check the API key attributes with Query API
        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery().addIds(apiKeyId)),
            null,
            null,
            null,
            null,
            null,
            randomBoolean(),
            randomBoolean()
        );
        final QueryApiKeyResponse queryApiKeyResponse = client().execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest).actionGet();
        assertThat(queryApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        ApiKey queryApiKeyInfo = queryApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo();
        assertThat(queryApiKeyInfo.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(queryApiKeyInfo.getRoleDescriptors(), contains(expectedRoleDescriptor));
        assertThat(queryApiKeyInfo.getLimitedBy(), nullValue());
        assertThat(queryApiKeyInfo.getMetadata(), anEmptyMap());
        assertThat(queryApiKeyInfo.getUsername(), equalTo("test_user"));
        assertThat(queryApiKeyInfo.getRealm(), equalTo("file"));
        assertThat(queryApiKeyInfo.getRealmType(), equalTo("file"));
    }

    public void testUpdateCrossClusterApiKey() throws IOException {
        final RoleDescriptor originalRoleDescriptor = new RoleDescriptor(
            "cross_cluster",
            new String[] { "cross_cluster_search" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build() },
            null
        );
        final var createApiKeyRequest = CreateCrossClusterApiKeyRequest.withNameAndAccess(randomAlphaOfLengthBetween(3, 8), """
            {
              "search": [ {"names": ["logs"]} ]
            }""");
        createApiKeyRequest.setRefreshPolicy(randomFrom(WAIT_UNTIL, IMMEDIATE));
        final CreateApiKeyResponse createApiKeyResponse = client().execute(CreateCrossClusterApiKeyAction.INSTANCE, createApiKeyRequest)
            .actionGet();
        final String apiKeyId = createApiKeyResponse.getId();

        final GetApiKeyResponse getApiKeyResponse = client().execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.builder().apiKeyId(apiKeyId).withLimitedBy(randomBoolean()).build()
        ).actionGet();
        assertThat(getApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        ApiKey getApiKeyInfo = getApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo();
        assertThat(getApiKeyInfo.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(getApiKeyInfo.getRoleDescriptors(), contains(originalRoleDescriptor));
        assertThat(getApiKeyInfo.getLimitedBy(), nullValue());
        assertThat(getApiKeyInfo.getMetadata(), anEmptyMap());
        assertThat(getApiKeyInfo.getUsername(), equalTo("test_user"));
        assertThat(getApiKeyInfo.getRealm(), equalTo("file"));
        assertThat(getApiKeyInfo.getRealmType(), equalTo("file"));

        final CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder;
        final boolean shouldUpdateAccess = randomBoolean();
        if (shouldUpdateAccess) {
            roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(randomFrom("""
                {
                  "search": [ {"names": ["logs"]} ]
                }""", """
                {
                  "search": [ {"names": ["metrics"]} ]
                }""", """
                {
                  "replication": [ {"names": ["archive"]} ]
                }""", """
                {
                  "search": [ {"names": ["logs"]} ],
                  "replication": [ {"names": ["archive"]} ]
                }"""));
        } else {
            roleDescriptorBuilder = null;
        }

        final Map<String, Object> updateMetadata;
        final boolean shouldUpdateMetadata = shouldUpdateAccess == false || randomBoolean();
        if (shouldUpdateMetadata) {
            updateMetadata = randomFrom(
                randomMap(
                    0,
                    5,
                    () -> new Tuple<>(randomAlphaOfLengthBetween(8, 12), randomFrom(randomAlphaOfLengthBetween(3, 8), 42, randomBoolean()))
                )
            );
        } else {
            updateMetadata = null;
        }

        final boolean shouldUpdateExpiration = randomBoolean();
        TimeValue expiration = null;
        if (shouldUpdateExpiration) {
            ApiKeyTests.randomFutureExpirationTime();
        }

        final var updateApiKeyRequest = new UpdateCrossClusterApiKeyRequest(apiKeyId, roleDescriptorBuilder, updateMetadata, expiration);
        final UpdateApiKeyResponse updateApiKeyResponse = client().execute(UpdateCrossClusterApiKeyAction.INSTANCE, updateApiKeyRequest)
            .actionGet();

        if ((roleDescriptorBuilder == null || roleDescriptorBuilder.build().equals(originalRoleDescriptor)) && (updateMetadata == null)) {
            assertThat(updateApiKeyResponse.isUpdated(), is(false));
        } else {
            assertThat(updateApiKeyResponse.isUpdated(), is(true));
        }

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery().addIds(apiKeyId)),
            null,
            null,
            null,
            null,
            null,
            randomBoolean(),
            randomBoolean()
        );
        final QueryApiKeyResponse queryApiKeyResponse = client().execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest).actionGet();
        assertThat(queryApiKeyResponse.getApiKeyInfoList(), iterableWithSize(1));
        final ApiKey queryApiKeyInfo = queryApiKeyResponse.getApiKeyInfoList().get(0).apiKeyInfo();
        assertThat(queryApiKeyInfo.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(
            queryApiKeyInfo.getRoleDescriptors(),
            contains(roleDescriptorBuilder == null ? originalRoleDescriptor : roleDescriptorBuilder.build())
        );
        assertThat(queryApiKeyInfo.getLimitedBy(), nullValue());
        assertThat(queryApiKeyInfo.getMetadata(), equalTo(updateMetadata == null ? Map.of() : updateMetadata));
        assertThat(queryApiKeyInfo.getUsername(), equalTo("test_user"));
        assertThat(queryApiKeyInfo.getRealm(), equalTo("file"));
        assertThat(queryApiKeyInfo.getRealmType(), equalTo("file"));
    }

    // Cross-cluster API keys cannot be created by an API key even if it has manage_security privilege
    // This is intentional until we solve the issue of derived API key ownership
    public void testCannotCreateDerivedCrossClusterApiKey() throws IOException {
        final CreateApiKeyResponse createAdminKeyResponse = new CreateApiKeyRequestBuilder(client()).setName("admin-key")
            .setRoleDescriptors(
                randomFrom(
                    List.of(new RoleDescriptor(randomAlphaOfLengthBetween(3, 8), new String[] { "manage_security" }, null, null)),
                    null
                )
            )
            .setRefreshPolicy(randomFrom(NONE, WAIT_UNTIL, IMMEDIATE))
            .get();
        final String encoded = Base64.getEncoder()
            .encodeToString(
                (createAdminKeyResponse.getId() + ":" + createAdminKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8)
            );

        final var request = CreateCrossClusterApiKeyRequest.withNameAndAccess(randomAlphaOfLengthBetween(3, 8), """
            {
              "search": [ {"names": ["logs"]} ]
            }""");
        request.setRefreshPolicy(randomFrom(NONE, WAIT_UNTIL, IMMEDIATE));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        client().filterWithHeader(Map.of("Authorization", "ApiKey " + encoded))
            .execute(CreateCrossClusterApiKeyAction.INSTANCE, request, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString("authentication via API key not supported: An API key cannot be used to create a cross-cluster API key")
        );
    }

    private GrantApiKeyRequest buildGrantApiKeyRequest(String username, SecureString password, String runAsUsername) throws IOException {
        final SecureString clonedPassword = password.clone();
        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        // randomly use either password or access token grant
        grantApiKeyRequest.getApiKeyRequest().setName("granted-api-key-for-" + username + "-runas-" + runAsUsername);
        grantApiKeyRequest.setRefreshPolicy(randomFrom(WAIT_UNTIL, IMMEDIATE));
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
        final GetResponse getResponse = client().execute(TransportGetAction.TYPE, new GetRequest(".security-7", apiKeyId)).actionGet();
        return getResponse.getSource();
    }
}
