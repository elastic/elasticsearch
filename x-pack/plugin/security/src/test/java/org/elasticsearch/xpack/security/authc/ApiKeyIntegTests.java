/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import com.google.common.collect.Sets;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeyIntegTests extends SecurityIntegTestCase {
    private static final long DELETE_INTERVAL_MILLIS = 100L;

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(ApiKeyService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(DELETE_INTERVAL_MILLIS))
            .put(ApiKeyService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5L))
            .build();
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() throws InterruptedException {
        // get the api key service and wait until api key expiration is not in progress!
        awaitApiKeysRemoverCompletion();
        deleteSecurityIndex();
    }

    private void awaitApiKeysRemoverCompletion() throws InterruptedException {
        for (ApiKeyService apiKeyService : internalCluster().getInstances(ApiKeyService.class)) {
            final boolean done = awaitBusy(() -> apiKeyService.isExpirationInProgress() == false);
            assertTrue(done);
        }
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

    public void testCreateApiKeyFailsWhenApiKeyWithSameNameAlreadyExists() throws InterruptedException, ExecutionException {
        String keyName = randomAlphaOfLength(5);
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            SecurityClient securityClient = new SecurityClient(client);
            final CreateApiKeyResponse response = securityClient.prepareCreateApiKey().setName(keyName).setExpiration(null)
                    .setRoleDescriptors(Collections.singletonList(descriptor)).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
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

        // Now invalidate the API key
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(keyName), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);

        // try to create API key with same name, should succeed now
        CreateApiKeyResponse createResponse = securityClient.prepareCreateApiKey().setName(keyName)
                .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
                .setRoleDescriptors(Collections.singletonList(descriptor)).get();
        assertNotNull(createResponse.getId());
        assertNotNull(createResponse.getKey());
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
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
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
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
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
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(responses.get(0).getName()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    private void verifyInvalidateResponse(int noOfApiKeys, List<CreateApiKeyResponse> responses,
                                          InvalidateApiKeyResponse invalidateResponse) {
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(noOfApiKeys));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                containsInAnyOrder(responses.stream().map(r -> r.getId()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY)));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidatedApiKeysDeletedByRemover() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));

        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(2, null);

        SecurityClient securityClient = new SecurityClient(client);

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(2));

        client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        securityClient = new SecurityClient(client);

        // invalidate API key to trigger remover
        listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(1).getId()), listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify that 1st invalidated API key is deleted whereas the next one is not
        getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(1));
        ApiKey apiKey = getApiKeyResponseListener.get().getApiKeyInfos()[0];
        assertThat(apiKey.getId(), is(createdApiKeys.get(1).getId()));
        assertThat(apiKey.isInvalidated(), is(true));
    }

    private Client waitForExpiredApiKeysRemoverTriggerReadyAndGetClient() throws Exception {
        String nodeWithMostRecentRun = null;
        long apiKeyLastTrigger = -1L;
        for (String nodeName : internalCluster().getNodeNames()) {
            ApiKeyService apiKeyService = internalCluster().getInstance(ApiKeyService.class, nodeName);
            if (apiKeyService != null) {
                if (apiKeyService.lastTimeWhenApiKeysRemoverWasTriggered() > apiKeyLastTrigger) {
                    nodeWithMostRecentRun = nodeName;
                    apiKeyLastTrigger = apiKeyService.lastTimeWhenApiKeysRemoverWasTriggered();
               }
            }
        }
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeWithMostRecentRun);
        final long lastRunTime = apiKeyLastTrigger;
        assertBusy(() -> {
            assertThat(threadPool.relativeTimeInMillis() - lastRunTime, greaterThan(DELETE_INTERVAL_MILLIS));
        });
        return internalCluster().client(nodeWithMostRecentRun);
    }

    public void testExpiredApiKeysBehaviorWhenKeysExpired1WeekBeforeAnd1DayBefore() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));

        int noOfKeys = 4;
        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(noOfKeys, null);
        Instant created = Instant.now();

        SecurityClient securityClient = new SecurityClient(client);

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(noOfKeys));

        // Expire the 1st key such that it cannot be deleted by the remover
        // hack doc to modify the expiration time to a day before
        Instant dayBefore = created.minus(1L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(dayBefore));
        UpdateResponse expirationDateUpdatedResponse = client
                .prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, "doc", createdApiKeys.get(0).getId())
                .setDoc("expiration_time", dayBefore.toEpochMilli()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Expire the 2nd key such that it cannot be deleted by the remover
        // hack doc to modify the expiration time to the week before
        Instant weekBefore = created.minus(8L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(weekBefore));
        expirationDateUpdatedResponse = client
                .prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, "doc", createdApiKeys.get(1).getId())
                .setDoc("expiration_time", weekBefore.toEpochMilli()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Invalidate to trigger the remover
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(2).getId()), listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify get API keys does not return expired and deleted key
        getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(3));

        Set<String> expectedKeyIds = Sets.newHashSet(createdApiKeys.get(0).getId(), createdApiKeys.get(2).getId(),
                createdApiKeys.get(3).getId());
        for (ApiKey apiKey : getApiKeyResponseListener.get().getApiKeyInfos()) {
            assertThat(apiKey.getId(), isIn(expectedKeyIds));
            if (apiKey.getId().equals(createdApiKeys.get(0).getId())) {
                // has been expired, not invalidated
                assertTrue(apiKey.getExpiration().isBefore(Instant.now()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else if (apiKey.getId().equals(createdApiKeys.get(2).getId())) {
                // has not been expired as no expiration, but invalidated
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(true));
            } else if (apiKey.getId().equals(createdApiKeys.get(3).getId())) {
                // has not been expired as no expiration, not invalidated
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else {
                fail("unexpected API key " + apiKey);
            }
        }
    }

    private void refreshSecurityIndex() throws Exception {
        assertBusy(() -> {
            final RefreshResponse refreshResponse = client()
                    .filterWithHeader(Collections.singletonMap("Authorization",
                            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)))
                    .admin().indices().prepareRefresh(SecurityIndexManager.SECURITY_INDEX_NAME).get();
            assertThat(refreshResponse.getFailedShards(), is(0));
        });
    }

    public void testActiveApiKeysWithNoExpirationNeverGetDeletedByRemover() throws Exception {
        List<CreateApiKeyResponse> responses = createApiKeys(2, null);

        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        // trigger expired keys remover
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        GetApiKeyResponse response = getApiKeyResponseListener.get();
        verifyGetResponse(2, responses, response, Collections.singleton(responses.get(0).getId()),
                Collections.singletonList(responses.get(1).getId()));
    }

    public void testGetApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        boolean invalidate= randomBoolean();
        List<String> invalidatedApiKeyIds = null;
        Set<String> expectedValidKeyIds = null;
        if (invalidate) {
            PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
            InvalidateApiKeyResponse invalidateResponse = listener.get();
            invalidatedApiKeyIds = invalidateResponse.getInvalidatedApiKeys();
            expectedValidKeyIds = responses.stream().filter(o -> !o.getId().equals(responses.get(0).getId())).map(o -> o.getId())
                    .collect(Collectors.toSet());
        } else {
            invalidatedApiKeyIds = Collections.emptyList();
            expectedValidKeyIds = responses.stream().map(o -> o.getId()).collect(Collectors.toSet());
        }

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, response,
                expectedValidKeyIds,
                invalidatedApiKeyIds);
    }

    public void testGetApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingUserName(SecuritySettingsSource.TEST_SUPERUSER), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, response, responses.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmAndUserName("file", SecuritySettingsSource.TEST_SUPERUSER),
                listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingApiKeyName(responses.get(0).getName()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    private void verifyGetResponse(int noOfApiKeys, List<CreateApiKeyResponse> responses, GetApiKeyResponse response,
                                   Set<String> validApiKeyIds,
                                   List<String> invalidatedApiKeyIds) {
        assertThat(response.getApiKeyInfos().length, equalTo(noOfApiKeys));
        List<String> expectedIds = responses.stream().filter(o -> validApiKeyIds.contains(o.getId())).map(o -> o.getId())
                .collect(Collectors.toList());
        List<String> actualIds = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false).map(o -> o.getId())
                .collect(Collectors.toList());
        assertThat(actualIds, containsInAnyOrder(expectedIds.toArray(Strings.EMPTY_ARRAY)));
        List<String> expectedNames = responses.stream().filter(o -> validApiKeyIds.contains(o.getId())).map(o -> o.getName())
                .collect(Collectors.toList());
        List<String> actualNames = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false).map(o -> o.getName())
                .collect(Collectors.toList());
        assertThat(actualNames, containsInAnyOrder(expectedNames.toArray(Strings.EMPTY_ARRAY)));
        Set<String> expectedUsernames = (validApiKeyIds.isEmpty()) ? Collections.emptySet()
                : Collections.singleton(SecuritySettingsSource.TEST_SUPERUSER);
        Set<String> actualUsernames = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false)
                .map(o -> o.getUsername()).collect(Collectors.toSet());
        assertThat(actualUsernames, containsInAnyOrder(expectedUsernames.toArray(Strings.EMPTY_ARRAY)));
        if (invalidatedApiKeyIds != null) {
            List<String> actualInvalidatedApiKeyIds = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated())
                    .map(o -> o.getId()).collect(Collectors.toList());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(actualInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
        }

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
