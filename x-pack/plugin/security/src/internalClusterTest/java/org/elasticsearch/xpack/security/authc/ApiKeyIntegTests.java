/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.elasticsearch.xpack.core.security.action.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_SUPERUSER;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeyIntegTests extends SecurityIntegTestCase {
    private static final long DELETE_INTERVAL_MILLIS = 100L;
    private static final int CRYPTO_THREAD_POOL_QUEUE_SIZE = 10;

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(ApiKeyService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(DELETE_INTERVAL_MILLIS))
            .put(ApiKeyService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5L))
            .put("xpack.security.crypto.thread_pool.queue_size", CRYPTO_THREAD_POOL_QUEUE_SIZE)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() throws Exception {
        // get the api key service and wait until api key expiration is not in progress!
        awaitApiKeysRemoverCompletion();
        deleteSecurityIndex();
    }

    @Override
    public String configRoles() {
        return super.configRoles() + "\n" +
            "no_api_key_role:\n" +
            "  cluster: [\"manage_token\"]\n" +
            "manage_api_key_role:\n" +
            "  cluster: [\"manage_api_key\"]\n" +
            "manage_own_api_key_role:\n" +
            "  cluster: [\"manage_own_api_key\"]\n" +
            "run_as_role:\n" +
            "  run_as: [\"user_with_manage_own_api_key_role\"]\n";
    }

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(
            getFastStoredHashAlgoForTests().hash(TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() +
            "user_with_no_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_manage_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_manage_own_api_key_role:" + usersPasswdHashed + "\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() +
            "no_api_key_role:user_with_no_api_key_role\n" +
            "manage_api_key_role:user_with_manage_api_key_role\n" +
            "manage_own_api_key_role:user_with_manage_own_api_key_role\n";
    }

    private void awaitApiKeysRemoverCompletion() throws Exception {
        for (ApiKeyService apiKeyService : internalCluster().getInstances(ApiKeyService.class)) {
            assertBusy(() -> assertFalse(apiKeyService.isExpirationInProgress()));
        }
    }

    public void testCreateApiKey() throws Exception {
        // Get an instant without nanoseconds as the expiration has millisecond precision
        final Instant start = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client)
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        assertEquals("test key", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());
        Instant expiration = response.getExpiration();
        // Expiration has millisecond precision
        final long daysBetween = ChronoUnit.DAYS.between(start, expiration);
        assertThat(daysBetween, is(7L));

        // create simple api key
        final CreateApiKeyResponse simple = new CreateApiKeyRequestBuilder(client).setName("simple").get();
        assertEquals("simple", simple.getName());
        assertNotNull(simple.getId());
        assertNotNull(simple.getKey());
        assertThat(simple.getId(), not(containsString(new String(simple.getKey().getChars()))));
        assertNull(simple.getExpiration());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        // Assert that we can authenticate with the API KEY
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        AuthenticateResponse authResponse = restClient.security().authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            "ApiKey " + base64ApiKeyKeyValue).build());
        assertThat(authResponse.getUser().getUsername(), equalTo(TEST_SUPERUSER));
        assertThat(authResponse.getAuthenticationType(), equalTo("api_key"));

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

    public void testMultipleApiKeysCanHaveSameName() {
        String keyName = randomAlphaOfLength(5);
        int noOfApiKeys = randomIntBetween(2, 5);
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        for (int i = 0; i < noOfApiKeys; i++) {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(
                Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
            final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName(keyName).setExpiration(null)
                .setRoleDescriptors(Collections.singletonList(descriptor))
                .setMetadata(ApiKeyTests.randomMetadata()).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
        }
        assertThat(responses.size(), is(noOfApiKeys));
        for (int i = 0; i < noOfApiKeys; i++) {
            assertThat(responses.get(i).getName(), is(keyName));
        }
    }

    public void testCreateApiKeyWithoutNameWillFail() {
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        final ActionRequestValidationException e =
            expectThrows(ActionRequestValidationException.class, () -> new CreateApiKeyRequestBuilder(client).get());
        assertThat(e.getMessage(), containsString("api key name is required"));
    }

    public void testInvalidateApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingRealmName("file"), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingUserName(TEST_SUPERUSER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingRealmAndUserName("file", TEST_SUPERUSER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyName(responses.get(0).getName(), false),
            listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeyWillClearApiKeyCache() throws IOException, ExecutionException, InterruptedException {
        final List<ApiKeyService> services = Arrays.stream(internalCluster().getNodeNames())
            .map(n -> internalCluster().getInstance(ApiKeyService.class, n))
            .collect(Collectors.toList());

        // Create two API keys and authenticate with them
        Tuple<String, String> apiKey1 = createApiKeyAndAuthenticateWithIt();
        Tuple<String, String> apiKey2 = createApiKeyAndAuthenticateWithIt();

        // Find out which nodes handled the above authentication requests
        final ApiKeyService serviceForDoc1 =
            services.stream().filter(s -> s.getDocCache().get(apiKey1.v1()) != null).findFirst().orElseThrow();
        final ApiKeyService serviceForDoc2 =
            services.stream().filter(s -> s.getDocCache().get(apiKey2.v1()) != null).findFirst().orElseThrow();
        assertNotNull(serviceForDoc1.getFromCache(apiKey1.v1()));
        assertNotNull(serviceForDoc2.getFromCache(apiKey2.v1()));
        final boolean sameServiceNode = serviceForDoc1 == serviceForDoc2;
        if (sameServiceNode) {
            assertEquals(2, serviceForDoc1.getDocCache().count());
        } else {
            assertEquals(1, serviceForDoc1.getDocCache().count());
            assertEquals(1, serviceForDoc2.getDocCache().count());
        }

        // Invalidate the first key
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(apiKey1.v1(), false), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        // The cache entry should be gone for the first key
        if (sameServiceNode) {
            assertEquals(1, serviceForDoc1.getDocCache().count());
            assertNull(serviceForDoc1.getDocCache().get(apiKey1.v1()));
            assertNotNull(serviceForDoc1.getDocCache().get(apiKey2.v1()));
        } else {
            assertEquals(0, serviceForDoc1.getDocCache().count());
            assertEquals(1, serviceForDoc2.getDocCache().count());
        }

        // Authentication with the first key should fail
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (apiKey1.v1() + ":" + apiKey1.v2()).getBytes(StandardCharsets.UTF_8));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new TestRestHighLevelClient().security()
                .authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                    "ApiKey " + base64ApiKeyKeyValue).build()));
        assertThat(e.getMessage(), containsString("security_exception"));
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));

        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(2, null).v1();

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(0).getId(), false),
            listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        awaitApiKeysRemoverCompletion();
        refreshSecurityIndex();

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        Set<String> expectedKeyIds = Sets.newHashSet(createdApiKeys.get(0).getId(), createdApiKeys.get(1).getId());
        boolean apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = false;
        for (ApiKey apiKey : getApiKeyResponseListener.get().getApiKeyInfos()) {
            assertThat(apiKey.getId(), is(in(expectedKeyIds)));
            if (apiKey.getId().equals(createdApiKeys.get(0).getId())) {
                // has been invalidated but not yet deleted by ExpiredApiKeysRemover
                assertThat(apiKey.isInvalidated(), is(true));
                apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = true;
            } else if (apiKey.getId().equals(createdApiKeys.get(1).getId())) {
                // active api key
                assertThat(apiKey.isInvalidated(), is(false));
            }
        }
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 2 : 1));

        client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));

        // invalidate API key to trigger remover
        listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(1).getId(), false),
            listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();
        refreshSecurityIndex();

        // Verify that 1st invalidated API key is deleted whereas the next one may be or may not be as it depends on whether update was
        // indexed before ExpiredApiKeysRemover ran
        getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        expectedKeyIds = Sets.newHashSet(createdApiKeys.get(1).getId());
        apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = false;
        for (ApiKey apiKey : getApiKeyResponseListener.get().getApiKeyInfos()) {
            assertThat(apiKey.getId(), is(in(expectedKeyIds)));
            if (apiKey.getId().equals(createdApiKeys.get(1).getId())) {
                // has been invalidated but not yet deleted by ExpiredApiKeysRemover
                assertThat(apiKey.isInvalidated(), is(true));
                apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = true;
            }
        }
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 1 : 0));
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));

        int noOfKeys = 4;
        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(noOfKeys, null).v1();
        Instant created = Instant.now();

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(noOfKeys));

        // Expire the 1st key such that it cannot be deleted by the remover
        // hack doc to modify the expiration time to a day before
        Instant dayBefore = created.minus(1L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(dayBefore));
        UpdateResponse expirationDateUpdatedResponse = client
            .prepareUpdate(SECURITY_MAIN_ALIAS, createdApiKeys.get(0).getId())
            .setDoc("expiration_time", dayBefore.toEpochMilli())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Expire the 2nd key such that it can be deleted by the remover
        // hack doc to modify the expiration time to the week before
        Instant weekBefore = created.minus(8L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(weekBefore));
        expirationDateUpdatedResponse = client.prepareUpdate(SECURITY_MAIN_ALIAS, createdApiKeys.get(1).getId())
            .setDoc("expiration_time", weekBefore.toEpochMilli())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Invalidate to trigger the remover
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(2).getId(), false),
            listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify get API keys does not return api keys deleted by ExpiredApiKeysRemover
        getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);

        Set<String> expectedKeyIds = Sets.newHashSet(createdApiKeys.get(0).getId(), createdApiKeys.get(2).getId(),
            createdApiKeys.get(3).getId());
        boolean apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = false;
        for (ApiKey apiKey : getApiKeyResponseListener.get().getApiKeyInfos()) {
            assertThat(apiKey.getId(), is(in(expectedKeyIds)));
            if (apiKey.getId().equals(createdApiKeys.get(0).getId())) {
                // has been expired, not invalidated
                assertTrue(apiKey.getExpiration().isBefore(Instant.now()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else if (apiKey.getId().equals(createdApiKeys.get(2).getId())) {
                // has not been expired as no expiration, is invalidated but not yet deleted by ExpiredApiKeysRemover
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(true));
                apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover = true;
            } else if (apiKey.getId().equals(createdApiKeys.get(3).getId())) {
                // has not been expired as no expiration, not invalidated
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else {
                fail("unexpected API key " + apiKey);
            }
        }
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 3 : 2));
    }

    private void refreshSecurityIndex() throws Exception {
        assertBusy(() -> {
            final RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(SECURITY_MAIN_ALIAS).get();
            assertThat(refreshResponse.getFailedShards(), is(0));
        });
    }

    public void testActiveApiKeysWithNoExpirationNeverGetDeletedByRemover() throws Exception {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(2, null);
        List<CreateApiKeyResponse> responses = tuple.v1();

        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        // trigger expired keys remover
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId(), false), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        GetApiKeyResponse response = getApiKeyResponseListener.get();
        verifyGetResponse(2, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()),
            Collections.singletonList(responses.get(1).getId()));
    }

    public void testGetApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(noOfApiKeys, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        boolean invalidate = randomBoolean();
        List<String> invalidatedApiKeyIds = null;
        Set<String> expectedValidKeyIds = null;
        if (invalidate) {
            PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false),
                listener);
            InvalidateApiKeyResponse invalidateResponse = listener.get();
            invalidatedApiKeyIds = invalidateResponse.getInvalidatedApiKeys();
            expectedValidKeyIds = responses.stream().filter(o -> o.getId().equals(responses.get(0).getId()) == false).map(o -> o.getId())
                .collect(Collectors.toSet());
        } else {
            invalidatedApiKeyIds = Collections.emptyList();
            expectedValidKeyIds = responses.stream().map(o -> o.getId()).collect(Collectors.toSet());
        }

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, tuple.v2(), response,
            expectedValidKeyIds,
            invalidatedApiKeyIds);
    }

    public void testGetApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(noOfApiKeys, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingUserName(TEST_SUPERUSER), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, tuple.v2(),
            response, responses.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(1, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmAndUserName("file", TEST_SUPERUSER),
            listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(1, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        final Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING));

        final int noOfApiKeys = randomIntBetween(1, 3);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple1 = createApiKeys(noOfApiKeys, null);
        final List<CreateApiKeyResponse> createApiKeyResponses1 = tuple1.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple2 =
            createApiKeys(headers, noOfApiKeys, "another-test-key-", null, "monitor");
        final List<CreateApiKeyResponse> createApiKeyResponses2 = tuple2.v1();

        Client client = client().filterWithHeader(headers);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        @SuppressWarnings("unchecked")
        List<CreateApiKeyResponse> responses = randomFrom(createApiKeyResponses1, createApiKeyResponses2);
        List<Map<String, Object>> metadatas = responses == createApiKeyResponses1 ? tuple1.v2() : tuple2.v2();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName(responses.get(0).getName(), false), listener);
        verifyGetResponse(1, responses, metadatas, listener.get(), Collections.singleton(responses.get(0).getId()), null);

        PlainActionFuture<GetApiKeyResponse> listener2 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("test-key*", false), listener2);
        verifyGetResponse(noOfApiKeys, createApiKeyResponses1, tuple1.v2(), listener2.get(),
            createApiKeyResponses1.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()), null);

        PlainActionFuture<GetApiKeyResponse> listener3 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("*", false), listener3);
        responses = Stream.concat(createApiKeyResponses1.stream(), createApiKeyResponses2.stream()).collect(Collectors.toList());
        metadatas = Stream.concat(tuple1.v2().stream(), tuple2.v2().stream()).collect(Collectors.toList());
        verifyGetResponse(2 * noOfApiKeys, responses, metadatas, listener3.get(),
            responses.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()), null);

        PlainActionFuture<GetApiKeyResponse> listener4 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("does-not-exist*", false), listener4);
        verifyGetResponse(0, Collections.emptyList(), null, listener4.get(), Collections.emptySet(), null);

        PlainActionFuture<GetApiKeyResponse> listener5 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("another-test-key*", false), listener5);
        verifyGetResponse(noOfApiKeys, createApiKeyResponses2, tuple2.v2(), listener5.get(),
            createApiKeyResponses2.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysOwnedByCurrentAuthenticatedUser() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = createApiKeys(noOfSuperuserApiKeys, null).v1();
        String userWithManageApiKeyRole = randomFrom("user_with_manage_api_key_role", "user_with_manage_own_api_key_role");
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple =
            createApiKeys(userWithManageApiKeyRole, noOfApiKeysForUserWithManageApiKeyRole, null, "monitor");
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = tuple.v1();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(userWithManageApiKeyRole, TEST_PASSWORD_SECURE_STRING)));

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(userWithManageApiKeyRole, noOfApiKeysForUserWithManageApiKeyRole, userWithManageApiKeyRoleApiKeys, tuple.v2(),
            response, userWithManageApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysOwnedByRunAsUserWhenOwnerIsTrue() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor");
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = tuple.v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse("user_with_manage_own_api_key_role", noOfApiKeysForUserWithManageApiKeyRole,
            userWithManageOwnApiKeyRoleApiKeys, tuple.v2(),
            response, userWithManageOwnApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysOwnedByRunAsUserWhenRunAsUserInfoIsGiven() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor");
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = tuple.v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingRealmAndUserName("file", "user_with_manage_own_api_key_role"), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse("user_with_manage_own_api_key_role", noOfApiKeysForUserWithManageApiKeyRole,
            userWithManageOwnApiKeyRoleApiKeys, tuple.v2(),
            response, userWithManageOwnApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysOwnedByRunAsUserWillNotWorkWhenAuthUserInfoIsGiven() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role", noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        @SuppressWarnings("unchecked")
        final Tuple<String, String> invalidRealmAndUserPair = randomFrom(
            new Tuple<>("file", "user_with_run_as_role"),
            new Tuple<>("index", "user_with_manage_own_api_key_role"),
            new Tuple<>("index", "user_with_run_as_role"));
        getClientForRunAsUser().execute(GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingRealmAndUserName(invalidRealmAndUserPair.v1(), invalidRealmAndUserPair.v2()), listener);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, listener::actionGet);
        assertThat(e.getMessage(), containsString(
            "unauthorized for user [user_with_run_as_role] run as [user_with_manage_own_api_key_role]"));
    }

    public void testGetAllApiKeys() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageOwnApiKeyRole = randomIntBetween(3, 7);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> defaultUserTuple = createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = defaultUserTuple.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> userWithManageTuple =
            createApiKeys("user_with_manage_api_key_role", noOfApiKeysForUserWithManageApiKeyRole, null, "monitor");
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = userWithManageTuple.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> userWithManageOwnTuple =
            createApiKeys("user_with_manage_own_api_key_role", noOfApiKeysForUserWithManageOwnApiKeyRole, null, "monitor");
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = userWithManageOwnTuple.v1();

        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue("user_with_manage_api_key_role", TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, new GetApiKeyRequest(), listener);
        GetApiKeyResponse response = listener.get();
        int totalApiKeys = noOfSuperuserApiKeys + noOfApiKeysForUserWithManageApiKeyRole + noOfApiKeysForUserWithManageOwnApiKeyRole;
        List<CreateApiKeyResponse> allApiKeys = new ArrayList<>();
        Stream.of(defaultUserCreatedKeys, userWithManageApiKeyRoleApiKeys, userWithManageOwnApiKeyRoleApiKeys).forEach(
            allApiKeys::addAll);
        final List<Map<String, Object>> metadatas = Stream.of(defaultUserTuple.v2(), userWithManageTuple.v2(), userWithManageOwnTuple.v2())
            .flatMap(List::stream).collect(Collectors.toList());
        verifyGetResponse(new String[] {TEST_SUPERUSER, "user_with_manage_api_key_role",
                "user_with_manage_own_api_key_role" }, totalApiKeys, allApiKeys, metadatas, response,
            allApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetAllApiKeysFailsForUserWithNoRoleOrRetrieveOwnApiKeyRole() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageOwnApiKeyRole = randomIntBetween(3, 7);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = createApiKeys(noOfSuperuserApiKeys, null).v1();
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_api_key_role",
            noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = createApiKeys("user_with_manage_own_api_key_role",
            noOfApiKeysForUserWithManageOwnApiKeyRole, null, "monitor").v1();

        final String withUser = randomFrom("user_with_manage_own_api_key_role", "user_with_no_api_key_role");
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(withUser, TEST_PASSWORD_SECURE_STRING)));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, new GetApiKeyRequest(), listener);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", withUser);
    }

    public void testInvalidateApiKeysOwnedByCurrentAuthenticatedUser() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = createApiKeys(noOfSuperuserApiKeys, null).v1();
        String userWithManageApiKeyRole = randomFrom("user_with_manage_api_key_role", "user_with_manage_own_api_key_role");
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(userWithManageApiKeyRole,
            noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(userWithManageApiKeyRole, TEST_PASSWORD_SECURE_STRING)));

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.forOwnedApiKeys(), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();

        verifyInvalidateResponse(noOfApiKeysForUserWithManageApiKeyRole, userWithManageApiKeyRoleApiKeys, invalidateResponse);
    }

    public void testInvalidateApiKeysOwnedByRunAsUserWhenOwnerIsTrue() throws InterruptedException, ExecutionException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role", noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.forOwnedApiKeys(), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeysForUserWithManageApiKeyRole, userWithManageApiKeyRoleApiKeys, invalidateResponse);
    }

    public void testInvalidateApiKeysOwnedByRunAsUserWhenRunAsUserInfoIsGiven() throws InterruptedException, ExecutionException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role", noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingRealmAndUserName("file", "user_with_manage_own_api_key_role"), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeysForUserWithManageApiKeyRole, userWithManageApiKeyRoleApiKeys, invalidateResponse);
    }

    public void testInvalidateApiKeysOwnedByRunAsUserWillNotWorkWhenAuthUserInfoIsGiven() throws InterruptedException, ExecutionException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_own_api_key_role",
            "user_with_run_as_role", noOfApiKeysForUserWithManageApiKeyRole, null, "monitor").v1();
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        @SuppressWarnings("unchecked")
        final Tuple<String, String> invalidRealmAndUserPair = randomFrom(
            new Tuple<>("file", "user_with_run_as_role"),
            new Tuple<>("index", "user_with_manage_own_api_key_role"),
            new Tuple<>("index", "user_with_run_as_role"));
        getClientForRunAsUser().execute(InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingRealmAndUserName(invalidRealmAndUserPair.v1(), invalidRealmAndUserPair.v2()), listener);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, listener::actionGet);
        assertThat(e.getMessage(), containsString(
            "unauthorized for user [user_with_run_as_role] run as [user_with_manage_own_api_key_role]"));
    }

    public void testApiKeyAuthorizationApiKeyMustBeAbleToRetrieveItsOwnInformationButNotAnyOtherKeysCreatedBySameOwner()
        throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple =
            createApiKeys(TEST_SUPERUSER, 2, null, (String[]) null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (responses.get(0).getId() + ":" + responses.get(0).getKey().toString()).getBytes(StandardCharsets.UTF_8));
        Client client = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(responses.get(0).getId(), randomBoolean()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);

        final PlainActionFuture<GetApiKeyResponse> failureListener = new PlainActionFuture<>();
        // for any other API key id, it must deny access
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(responses.get(1).getId(), randomBoolean()),
            failureListener);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", TEST_SUPERUSER,
            responses.get(0).getId());

        final PlainActionFuture<GetApiKeyResponse> failureListener1 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), failureListener1);
        ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener1.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", TEST_SUPERUSER, responses.get(0).getId());
    }

    public void testApiKeyWithManageOwnPrivilegeIsAbleToInvalidateItselfButNotAnyOtherKeysCreatedBySameOwner()
        throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(TEST_SUPERUSER, 2, null, "manage_own_api_key").v1();
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (responses.get(0).getId() + ":" + responses.get(0).getKey().toString()).getBytes(StandardCharsets.UTF_8));
        Client client = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));

        final PlainActionFuture<InvalidateApiKeyResponse> failureListener = new PlainActionFuture<>();
        // for any other API key id, it must deny access
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId(), randomBoolean()),
            failureListener);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", TEST_SUPERUSER,
            responses.get(0).getId());

        final PlainActionFuture<InvalidateApiKeyResponse> failureListener1 = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.forOwnedApiKeys(), failureListener1);
        ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener1.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", TEST_SUPERUSER, responses.get(0).getId());

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), randomBoolean()),
            listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();

        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getInvalidatedApiKeys(), containsInAnyOrder(responses.get(0).getId()));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testDerivedKeys() throws ExecutionException, InterruptedException {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            basicAuthHeaderValue(TEST_SUPERUSER,
                TEST_PASSWORD_SECURE_STRING)));
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client)
            .setName("key-1")
            .setRoleDescriptors(Collections.singletonList(
                new RoleDescriptor("role", new String[] { "manage_api_key" }, null, null)))
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        assertEquals("key-1", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        final Client clientKey1 = client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue));

        final String expectedMessage = "creating derived api keys requires an explicit role descriptor that is empty";

        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-2").setMetadata(ApiKeyTests.randomMetadata()).get());
        assertThat(e1.getMessage(), containsString(expectedMessage));

        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-3")
                .setRoleDescriptors(Collections.emptyList()).get());
        assertThat(e2.getMessage(), containsString(expectedMessage));

        final IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-4")
                .setMetadata(ApiKeyTests.randomMetadata())
                .setRoleDescriptors(Collections.singletonList(
                    new RoleDescriptor("role", new String[] { "manage_own_api_key" }, null, null)
                )).get());
        assertThat(e3.getMessage(), containsString(expectedMessage));

        final List<RoleDescriptor> roleDescriptors = randomList(2, 10,
            () -> new RoleDescriptor("role", null, null, null));
        roleDescriptors.set(randomInt(roleDescriptors.size() - 1),
            new RoleDescriptor("role", new String[] { "manage_own_api_key" }, null, null));

        final IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-5")
                .setMetadata(ApiKeyTests.randomMetadata())
                .setRoleDescriptors(roleDescriptors).get());
        assertThat(e4.getMessage(), containsString(expectedMessage));

        final CreateApiKeyResponse key100Response = new CreateApiKeyRequestBuilder(clientKey1).setName("key-100")
            .setMetadata(ApiKeyTests.randomMetadata())
            .setRoleDescriptors(Collections.singletonList(
                new RoleDescriptor("role", null, null, null)
            )).get();
        assertEquals("key-100", key100Response.getName());
        assertNotNull(key100Response.getId());
        assertNotNull(key100Response.getKey());

        // Check at the end to allow sometime for the operation to happen. Since an erroneous creation is
        // asynchronous so that the document is not available immediately.
        assertApiKeyNotCreated(client, "key-2");
        assertApiKeyNotCreated(client, "key-3");
        assertApiKeyNotCreated(client, "key-4");
        assertApiKeyNotCreated(client, "key-5");
    }

    public void testCreationAndAuthenticationReturns429WhenThreadPoolIsSaturated() throws Exception {
        final String nodeName = randomFrom(internalCluster().getNodeNames());
        final Settings settings = internalCluster().getInstance(Settings.class, nodeName);
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        final ApiKeyService apiKeyService = internalCluster().getInstance(ApiKeyService.class, nodeName);

        final RoleDescriptor descriptor = new RoleDescriptor("auth_only", new String[] { }, null, null);
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        final CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyRequestBuilder(client)
            .setName("auth only key")
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        assertNotNull(createApiKeyResponse.getId());
        assertNotNull(createApiKeyResponse.getKey());
        // Clear the auth cache to force recompute the expensive hash which requires the crypto thread pool
        apiKeyService.getApiKeyAuthCache().invalidateAll();

        final List<NodeInfo> nodeInfos = client().admin().cluster().prepareNodesInfo().get().getNodes().stream()
            .filter(nodeInfo -> nodeInfo.getNode().getName().equals(nodeName))
            .collect(Collectors.toList());
        assertEquals(1, nodeInfos.size());

        final ExecutorService executorService = threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME);
        final int numberOfThreads = (allocatedProcessors + 1) / 2;
        final CountDownLatch blockingLatch = new CountDownLatch(1);
        final CountDownLatch readyLatch = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(() -> {
                readyLatch.countDown();
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        // Make sure above tasks are running
        readyLatch.await();
        // Then fill the whole queue for the crypto thread pool
        Future<?> lastTaskFuture = null;
        int i = 0;
        try {
            for (i = 0; i < CRYPTO_THREAD_POOL_QUEUE_SIZE; i++) {
                lastTaskFuture = executorService.submit(() -> { });
            }
        } catch (EsRejectedExecutionException e) {
            logger.info("Attempted to push {} tasks but only pushed {}", CRYPTO_THREAD_POOL_QUEUE_SIZE, i + 1);
        }

        try (RestClient restClient = createRestClient(nodeInfos, null, "http")) {
            final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
                (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));

            final Request authRequest = new Request("GET", "_security/_authenticate");
            authRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(
                "Authorization", "ApiKey " + base64ApiKeyKeyValue).build());
            final ResponseException e1 = expectThrows(ResponseException.class, () -> restClient.performRequest(authRequest));
            assertThat(e1.getMessage(), containsString("429 Too Many Requests"));
            assertThat(e1.getResponse().getStatusLine().getStatusCode(), is(429));

            final Request createApiKeyRequest = new Request("POST", "_security/api_key");
            createApiKeyRequest.setJsonEntity("{\"name\":\"key\"}");
            createApiKeyRequest.setOptions(createApiKeyRequest.getOptions().toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
            final ResponseException e2 = expectThrows(ResponseException.class, () -> restClient.performRequest(createApiKeyRequest));
            assertThat(e2.getMessage(), containsString("429 Too Many Requests"));
            assertThat(e2.getResponse().getStatusLine().getStatusCode(), is(429));
        } finally {
            blockingLatch.countDown();
            if (lastTaskFuture != null) {
                lastTaskFuture.get();
            }
        }
    }

    public void testCacheInvalidationViaApiCalls() throws Exception {
        final List<ApiKeyService> services = Arrays.stream(internalCluster().getNodeNames())
            .map(n -> internalCluster().getInstance(ApiKeyService.class, n))
            .collect(Collectors.toList());

        // Create two API keys and authenticate with them
        String docId1 = createApiKeyAndAuthenticateWithIt().v1();
        String docId2 = createApiKeyAndAuthenticateWithIt().v1();

        // Find out which nodes handled the above authentication requests
        final ApiKeyService serviceForDoc1 =
            services.stream().filter(s -> s.getDocCache().get(docId1) != null).findFirst().orElseThrow();
        final ApiKeyService serviceForDoc2 =
            services.stream().filter(s -> s.getDocCache().get(docId2) != null).findFirst().orElseThrow();
        assertNotNull(serviceForDoc1.getFromCache(docId1));
        assertNotNull(serviceForDoc2.getFromCache(docId2));
        final boolean sameServiceNode = serviceForDoc1 == serviceForDoc2;
        if (sameServiceNode) {
            assertEquals(2, serviceForDoc1.getDocCache().count());
            assertEquals(2, serviceForDoc1.getRoleDescriptorsBytesCache().count());
        } else {
            assertEquals(1, serviceForDoc1.getDocCache().count());
            assertEquals(2, serviceForDoc1.getRoleDescriptorsBytesCache().count());
            assertEquals(1, serviceForDoc2.getDocCache().count());
            assertEquals(2, serviceForDoc2.getRoleDescriptorsBytesCache().count());
        }

        // Invalidate cache for only the first key
        ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest();
        clearSecurityCacheRequest.cacheName("api_key");
        clearSecurityCacheRequest.keys(docId1);
        ClearSecurityCacheResponse clearSecurityCacheResponse =
            client().execute(ClearSecurityCacheAction.INSTANCE, clearSecurityCacheRequest).get();
        assertFalse(clearSecurityCacheResponse.hasFailures());

        assertBusy(() -> {
            expectThrows(NullPointerException.class, () -> serviceForDoc1.getFromCache(docId1));
            if (sameServiceNode) {
                assertEquals(1, serviceForDoc1.getDocCache().count());
                assertNotNull(serviceForDoc1.getFromCache(docId2));
            } else {
                assertEquals(0, serviceForDoc1.getDocCache().count());
                assertEquals(1, serviceForDoc2.getDocCache().count());
                assertNotNull(serviceForDoc2.getFromCache(docId2));
            }
            // Role descriptors are not invalidated when invalidation is for specific API keys
            assertEquals(2, serviceForDoc1.getRoleDescriptorsBytesCache().count());
            assertEquals(2, serviceForDoc2.getRoleDescriptorsBytesCache().count());
        });

        // Invalidate all cache entries by setting keys to an empty array
        clearSecurityCacheRequest.keys(new String[0]);
        clearSecurityCacheResponse =
            client().execute(ClearSecurityCacheAction.INSTANCE, clearSecurityCacheRequest).get();
        assertFalse(clearSecurityCacheResponse.hasFailures());
        assertBusy(() -> {
            assertEquals(0, serviceForDoc1.getDocCache().count());
            assertEquals(0, serviceForDoc1.getRoleDescriptorsBytesCache().count());
            if (sameServiceNode) {
                expectThrows(NullPointerException.class, () -> serviceForDoc1.getFromCache(docId2));
            } else {
                expectThrows(NullPointerException.class, () -> serviceForDoc2.getFromCache(docId2));
                assertEquals(0, serviceForDoc2.getDocCache().count());
                assertEquals(0, serviceForDoc2.getRoleDescriptorsBytesCache().count());
            }
        });
    }

    public void testSecurityIndexStateChangeWillInvalidateApiKeyCaches() throws Exception {
        final List<ApiKeyService> services = Arrays.stream(internalCluster().getNodeNames())
            .map(n -> internalCluster().getInstance(ApiKeyService.class, n))
            .collect(Collectors.toList());

        String docId = createApiKeyAndAuthenticateWithIt().v1();

        // The API key is cached by one of the node that the above request hits, find out which one
        final ApiKeyService apiKeyService =
            services.stream().filter(s -> s.getDocCache().count() > 0).findFirst().orElseThrow();
        assertNotNull(apiKeyService.getFromCache(docId));
        assertEquals(1, apiKeyService.getDocCache().count());
        assertEquals(2, apiKeyService.getRoleDescriptorsBytesCache().count());

        // Close security index to trigger invalidation
        final CloseIndexResponse closeIndexResponse = client().admin().indices().close(
            new CloseIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7)).get();
        assertTrue(closeIndexResponse.isAcknowledged());
        assertBusy(() -> {
            expectThrows(NullPointerException.class, () -> apiKeyService.getFromCache(docId));
            assertEquals(0, apiKeyService.getDocCache().count());
            assertEquals(0, apiKeyService.getRoleDescriptorsBytesCache().count());
        });
    }

    private Tuple<String, String> createApiKeyAndAuthenticateWithIt() throws IOException {
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));

        final CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyRequestBuilder(client)
            .setName("test key")
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();
        final String docId = createApiKeyResponse.getId();
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (docId + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        AuthenticateResponse authResponse = new TestRestHighLevelClient().security()
            .authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                "ApiKey " + base64ApiKeyKeyValue).build());
        assertEquals("api_key", authResponse.getAuthenticationType());
        return Tuple.tuple(docId, createApiKeyResponse.getKey().toString());
    }

    private void assertApiKeyNotCreated(Client client, String keyName) throws ExecutionException, InterruptedException {
        new RefreshRequestBuilder(client, RefreshAction.INSTANCE).setIndices(SECURITY_MAIN_ALIAS).execute().get();
        assertEquals(0, client.execute(GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingApiKeyName(keyName, false)).get().getApiKeyInfos().length);
    }

    private void verifyGetResponse(int expectedNumberOfApiKeys, List<CreateApiKeyResponse> responses,
                                   List<Map<String, Object>> metadatas,
                                   GetApiKeyResponse response, Set<String> validApiKeyIds, List<String> invalidatedApiKeyIds) {
        verifyGetResponse(TEST_SUPERUSER, expectedNumberOfApiKeys, responses, metadatas, response, validApiKeyIds,
            invalidatedApiKeyIds);
    }

    private void verifyGetResponse(String user, int expectedNumberOfApiKeys, List<CreateApiKeyResponse> responses,
                                   List<Map<String, Object>> metadatas,
                                   GetApiKeyResponse response, Set<String> validApiKeyIds, List<String> invalidatedApiKeyIds) {
        verifyGetResponse(
            new String[]{user}, expectedNumberOfApiKeys, responses, metadatas, response, validApiKeyIds, invalidatedApiKeyIds);
    }

    private void verifyGetResponse(String[] user, int expectedNumberOfApiKeys, List<CreateApiKeyResponse> responses,
                                   List<Map<String, Object>> metadatas,
                                   GetApiKeyResponse response, Set<String> validApiKeyIds, List<String> invalidatedApiKeyIds) {
        assertThat(response.getApiKeyInfos().length, equalTo(expectedNumberOfApiKeys));
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
            : Set.of(user);
        Set<String> actualUsernames = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false)
            .map(o -> o.getUsername()).collect(Collectors.toSet());
        assertThat(actualUsernames, containsInAnyOrder(expectedUsernames.toArray(Strings.EMPTY_ARRAY)));
        if (invalidatedApiKeyIds != null) {
            List<String> actualInvalidatedApiKeyIds = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated())
                .map(o -> o.getId()).collect(Collectors.toList());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(actualInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
        }
        if (metadatas != null) {
            final HashMap<String, Map<String, Object>> idToMetadata = IntStream.range(0, responses.size()).collect(
                (Supplier<HashMap<String, Map<String, Object>>>) HashMap::new,
                (m, i) -> m.put(responses.get(i).getId(), metadatas.get(i)),
                HashMap::putAll);
            for (ApiKey apiKey : response.getApiKeyInfos()) {
                final Map<String, Object> metadata = idToMetadata.get(apiKey.getId());
                assertThat(apiKey.getMetadata(), equalTo(metadata == null ? Map.of() : metadata));
            }
        }
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(int noOfApiKeys, TimeValue expiration) {
        return createApiKeys(TEST_SUPERUSER, noOfApiKeys, expiration, "monitor");
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        String user, int noOfApiKeys, TimeValue expiration, String... clusterPrivileges) {
        final Map<String, String> headers = Collections.singletonMap("Authorization",
            basicAuthHeaderValue(user, TEST_PASSWORD_SECURE_STRING));
        return createApiKeys(headers, noOfApiKeys, expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        String owningUser, String authenticatingUser, int noOfApiKeys, TimeValue expiration, String... clusterPrivileges) {
        final Map<String, String> headers = Map.of(
            "Authorization", basicAuthHeaderValue(authenticatingUser, TEST_PASSWORD_SECURE_STRING),
            "es-security-runas-user", owningUser);
        return createApiKeys(headers, noOfApiKeys, expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        Map<String, String> headers, int noOfApiKeys, TimeValue expiration, String... clusterPrivileges) {
        return createApiKeys(headers, noOfApiKeys, "test-key-", expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        Map<String, String> headers, int noOfApiKeys, String namePrefix, TimeValue expiration, String... clusterPrivileges) {
        List<Map<String, Object>> metadatas = new ArrayList<>(noOfApiKeys);
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        for (int i = 0; i < noOfApiKeys; i++) {
            final RoleDescriptor descriptor = new RoleDescriptor("role", clusterPrivileges, null, null);
            Client client = client().filterWithHeader(headers);
            final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
            metadatas.add(metadata);
            final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client)
                .setName(namePrefix + randomAlphaOfLengthBetween(5, 9) + i).setExpiration(expiration)
                .setRoleDescriptors(Collections.singletonList(descriptor))
                .setMetadata(metadata).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
        }
        assertThat(responses.size(), is(noOfApiKeys));
        return new Tuple<>(responses, metadatas);
    }

    /**
     * In order to have negative tests for realm name mismatch, user_with_run_as_role
     * needs to be created in a different realm other than file (which is handled by configureUsers()).
     * This new helper method creates the user in the native realm.
     */
    private void createUserWithRunAsRole() throws ExecutionException, InterruptedException {
        final PutUserRequest putUserRequest = new PutUserRequest();
        putUserRequest.username("user_with_run_as_role");
        putUserRequest.roles("run_as_role");
        putUserRequest.passwordHash(SecuritySettingsSource.TEST_PASSWORD_HASHED.toCharArray());
        PlainActionFuture<PutUserResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(TEST_SUPERUSER, TEST_PASSWORD_SECURE_STRING)));
        client.execute(PutUserAction.INSTANCE, putUserRequest, listener);
        final PutUserResponse putUserResponse = listener.get();
        assertTrue(putUserResponse.created());
    }

    private Client getClientForRunAsUser() {
        return client().filterWithHeader(Map.of(
            "Authorization", basicAuthHeaderValue("user_with_run_as_role", TEST_PASSWORD_SECURE_STRING),
            "es-security-runas-user", "user_with_manage_own_api_key_role"));
    }

    private void assertErrorMessage(final ElasticsearchSecurityException ese, String action, String userName, String apiKeyId) {
        assertThat(ese, throwableWithMessage(
            containsString("action [" + action + "] is unauthorized for API key id [" + apiKeyId + "] of user [" + userName + "]")));
        assertThat(ese, throwableWithMessage(containsString(", this action is granted by the cluster privileges [")));
        assertThat(ese, throwableWithMessage(containsString("manage_api_key,manage_security,all]")));
    }

    private void assertErrorMessage(final ElasticsearchSecurityException ese, String action, String userName) {
        assertThat(ese, throwableWithMessage(
            containsString("action [" + action + "] is unauthorized for user [" + userName + "]")));
        assertThat(ese, throwableWithMessage(containsString(", this action is granted by the cluster privileges [")));
        assertThat(ese, throwableWithMessage(containsString("manage_api_key,manage_security,all]")));
    }
}
