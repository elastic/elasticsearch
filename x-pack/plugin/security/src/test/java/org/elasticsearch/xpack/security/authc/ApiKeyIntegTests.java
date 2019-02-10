/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.XPackSettings;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@TestLogging("org.elasticsearch.xpack.security.authc.ApiKeyService:TRACE")
public class ApiKeyIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(ApiKeyService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(200L))
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
        for (ApiKeyService apiKeyService : internalCluster().getInstances(ApiKeyService.class)) {
            final boolean done = awaitBusy(() -> apiKeyService.isExpirationInProgress() == false);
            assertTrue(done);
        }
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

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/38408")
    public void testGetAndInvalidateApiKeysWithExpiredAndInvalidatedApiKey() throws Exception {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Instant created = Instant.now();

        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);

        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", "api_key"))).setSize(10)
                    .setTerminateAfter(10).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });
        logger.info("searched and found API key with doc id = " + docId.get());
        assertThat(docId.get(), is(notNullValue()));
        assertThat(docId.get(), is(responses.get(0).getId()));

        // hack doc to modify the expiration time to the week before
        Instant weekBefore = created.minus(8L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(weekBefore));
        client.prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, "doc", docId.get())
                .setDoc("expiration_time", weekBefore.toEpochMilli()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        if (invalidateResponse.getErrors().isEmpty() == false) {
            logger.error("error occurred while invalidating API key by id : " + invalidateResponse.getErrors().stream()
                    .map(ElasticsearchException::getMessage)
                    .collect(Collectors.joining(", ")));
        }
        verifyInvalidateResponse(1, responses, invalidateResponse);

        // try again
        listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        invalidateResponse = listener.get();
        assertTrue(invalidateResponse.getInvalidatedApiKeys().isEmpty());

        // Get API key though returns the API key information
        PlainActionFuture<GetApiKeyResponse> listener1 = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener1);
        GetApiKeyResponse response = listener1.get();
        verifyGetResponse(1, responses, response, Collections.emptySet(), Collections.singletonList(responses.get(0).getId()));
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
        List<CreateApiKeyResponse> responses = createApiKeys(2, null);

        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", "api_key"))).setSize(10)
                    .setTerminateAfter(10).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });
        logger.info("searched and found API key with doc id = " + docId.get());
        assertThat(docId.get(), is(notNullValue()));
        assertThat(docId.get(), isIn(responses.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toList())));

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId()), new PlainActionFuture<>());
            }
            client.admin().indices().prepareRefresh(SecurityIndexManager.SECURITY_INDEX_NAME).get();
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", "api_key")))
                    .setTerminateAfter(10).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        }, 30, TimeUnit.SECONDS);
    }

    public void testExpiredApiKeysDeletedAfter1Week() throws Exception {
        List<CreateApiKeyResponse> responses = createApiKeys(2, null);
        Instant created = Instant.now();

        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);

        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", "api_key"))).setSize(10)
                    .setTerminateAfter(10).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });
        logger.info("searched and found API key with doc id = " + docId.get());
        assertThat(docId.get(), is(notNullValue()));
        assertThat(docId.get(), isIn(responses.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toList())));

        // hack doc to modify the expiration time to the week before
        Instant weekBefore = created.minus(8L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(weekBefore));
        client.prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, "doc", docId.get())
                .setDoc("expiration_time", weekBefore.toEpochMilli()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId()), new PlainActionFuture<>());
            }
            client.admin().indices().prepareRefresh(SecurityIndexManager.SECURITY_INDEX_NAME).get();
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", "api_key")))
                    .setTerminateAfter(10).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        }, 30, TimeUnit.SECONDS);
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
        assertThat(responses.size(), is(noOfApiKeys));
        return responses;
    }
}
