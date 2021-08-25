/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.apikey;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class ApiKeySingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        return builder.build();
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
        grantApiKeyRequest.getApiKeyRequest().setRoleDescriptors(org.elasticsearch.core.List.of(
            new RoleDescriptor("x", new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[]{
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(true).build()
                },
                null, null, null, null, null)));
        final CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();

        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));

        // No cluster access
        final ElasticsearchSecurityException e1 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(org.elasticsearch.core.Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(MainAction.INSTANCE, new MainRequest())
                .actionGet());
        assertThat(e1.status().getStatus(), equalTo(403));
        assertThat(e1.getMessage(), containsString("is unauthorized for API key"));

        // No index access
        final ElasticsearchSecurityException e2 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(org.elasticsearch.core.Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(CreateIndexAction.INSTANCE, new CreateIndexRequest(
                    randomFrom(randomAlphaOfLengthBetween(3, 8), SECURITY_MAIN_ALIAS)))
                .actionGet());
        assertThat(e2.status().getStatus(), equalTo(403));
        assertThat(e2.getMessage(), containsString("is unauthorized for API key"));
    }

    public void testServiceAccountApiKey() throws IOException {
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
            new CreateServiceAccountTokenRequest("elastic", "fleet-server", randomAlphaOfLength(8));
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse =
            client().execute(CreateServiceAccountTokenAction.INSTANCE, createServiceAccountTokenRequest).actionGet();

        final CreateApiKeyResponse createApiKeyResponse =
            client()
                .filterWithHeader(org.elasticsearch.core.Map.of("Authorization", "Bearer " + createServiceAccountTokenResponse.getValue()))
                .execute(CreateApiKeyAction.INSTANCE, new CreateApiKeyRequest(randomAlphaOfLength(8), null, null))
                .actionGet();

        final Map<String, Object> apiKeyDocument = getApiKeyDocument(createApiKeyResponse.getId());

        @SuppressWarnings("unchecked")
        final Map<String, Object> fleetServerRoleDescriptor =
            (Map<String, Object>) apiKeyDocument.get("limited_by_role_descriptors");
        assertThat(fleetServerRoleDescriptor.size(), equalTo(1));
        assertThat(fleetServerRoleDescriptor, hasKey("elastic/fleet-server"));

        @SuppressWarnings("unchecked")
        final Map<String, ?> descriptor = (Map<String, ?>) fleetServerRoleDescriptor.get("elastic/fleet-server");

        final RoleDescriptor roleDescriptor = RoleDescriptor.parse("elastic/fleet-server",
            XContentTestUtils.convertToXContent(descriptor, XContentType.JSON),
            false,
            XContentType.JSON);
        assertThat(roleDescriptor, equalTo(ServiceAccountService.getServiceAccounts().get("elastic/fleet-server").roleDescriptor()));
    }

    private Map<String, Object> getApiKeyDocument(String apiKeyId) {
        final GetResponse getResponse =
            client().execute(GetAction.INSTANCE, new GetRequest(".security-7", apiKeyId)).actionGet();
        return getResponse.getSource();
    }
}
