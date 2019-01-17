/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ApiKeyIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() {
        deleteSecurityIndex();
    }

    public void testCreateApiKey() {
        final Instant start = Instant.now();
        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        final CreateApiKeyResponse response = securityClient.prepareCreateApiKey()
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .get();

        assertEquals("test key", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());
        Instant expiration = response.getExpiration();
        final long daysBetween = ChronoUnit.DAYS.between(start, expiration);
        assertThat(daysBetween, is(7L));

        // create simple api key
        final CreateApiKeyResponse simple = securityClient.prepareCreateApiKey().setName("simple").get();
        assertEquals("simple", simple.getName());
        assertNotNull(simple.getId());
        assertNotNull(simple.getKey());
        assertThat(simple.getId(), not(containsString(new String(simple.getKey().getChars()))));
        assertNull(simple.getExpiration());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        ClusterHealthResponse healthResponse = client()
            .filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue))
            .admin()
            .cluster()
            .prepareHealth()
            .get();
        assertFalse(healthResponse.isTimedOut());

        // use the first ApiKey for an unauthorized action
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
            client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .admin()
                .cluster()
                .prepareUpdateSettings().setTransientSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), true))
                .get());
        assertThat(e.getMessage(), containsString("unauthorized"));
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
    }

    public void testCreateApiKeyFailsWhenApiKeyWithSameNameAlreadyExists() {
        String keyName = randomAlphaOfLength(5);
        {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            SecurityClient securityClient = new SecurityClient(client);
            final CreateApiKeyResponse response = securityClient.prepareCreateApiKey().setName(keyName).setExpiration(null)
                    .setRoleDescriptors(Collections.singletonList(descriptor)).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
        }

        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> securityClient.prepareCreateApiKey()
            .setName(keyName)
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .get());
        assertThat(e.getMessage(), equalTo("Error creating api key as api key with name ["+keyName+"] already exists"));
    }

    public void testInvalidateApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmName("file"), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(noOfApiKeys));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                equalTo(responses.stream().map(r -> r.getId()).collect(Collectors.toList())));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidateApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingUserName(SecuritySettingsSource.TEST_SUPERUSER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(noOfApiKeys));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                equalTo(responses.stream().map(r -> r.getId()).collect(Collectors.toList())));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidateApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmAndUserName("file", SecuritySettingsSource.TEST_SUPERUSER),
                listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                equalTo(responses.stream().map(r -> r.getId()).collect(Collectors.toList())));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidateApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                equalTo(responses.stream().map(r -> r.getId()).collect(Collectors.toList())));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidateApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(responses.get(0).getName()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                equalTo(responses.stream().map(r -> r.getId()).collect(Collectors.toList())));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    private List<CreateApiKeyResponse> createApiKeys(int noOfApiKeys, TimeValue expiration) {
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        for (int i = 0; i < noOfApiKeys; i++) {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            SecurityClient securityClient = new SecurityClient(client);
            final CreateApiKeyResponse response = securityClient.prepareCreateApiKey()
                    .setName("test-key-" + randomAlphaOfLengthBetween(5, 9) + i).setExpiration(expiration)
                    .setRoleDescriptors(Collections.singletonList(descriptor)).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
        }
        return responses;
    }
}
