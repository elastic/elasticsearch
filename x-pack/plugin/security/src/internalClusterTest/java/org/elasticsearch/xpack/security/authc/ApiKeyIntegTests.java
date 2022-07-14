/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.RoleDescriptorTests;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSource.HASHER;
import static org.elasticsearch.test.SecuritySettingsSource.TEST_ROLE;
import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@SuppressWarnings("removal")
public class ApiKeyIntegTests extends SecurityIntegTestCase {
    private static final long DELETE_INTERVAL_MILLIS = 100L;
    private static final int CRYPTO_THREAD_POOL_QUEUE_SIZE = 10;

    private static final RoleDescriptor DEFAULT_API_KEY_ROLE_DESCRIPTOR = new RoleDescriptor(
        "role",
        new String[] { "monitor" },
        null,
        null
    );

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
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
        return super.configRoles() + """

            no_api_key_role:
              cluster: ["manage_token"]
            manage_api_key_role:
              cluster: ["manage_api_key"]
            manage_own_api_key_role:
              cluster: ["manage_own_api_key"]
            run_as_role:
              run_as: ["user_with_manage_own_api_key_role"]
            """;
    }

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(TEST_PASSWORD_SECURE_STRING));
        return super.configUsers()
            + "user_with_no_api_key_role:"
            + usersPasswdHashed
            + "\n"
            + "user_with_manage_api_key_role:"
            + usersPasswdHashed
            + "\n"
            + "user_with_manage_own_api_key_role:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() + """
            no_api_key_role:user_with_no_api_key_role
            manage_api_key_role:user_with_manage_api_key_role
            manage_own_api_key_role:user_with_manage_own_api_key_role
            """;
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName("test key")
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

        // Assert that we can authenticate with the API KEY
        final Map<String, Object> authResponse = authenticateWithApiKey(response.getId(), response.getKey());
        assertThat(authResponse.get(User.Fields.USERNAME.getPreferredName()), equalTo(ES_TEST_ROOT_USER));

        // use the first ApiKey for an unauthorized action
        final Map<String, String> authorizationHeaders = Collections.singletonMap(
            "Authorization",
            "ApiKey " + getBase64EncodedApiKeyValue(response.getId(), response.getKey())
        );
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(authorizationHeaders)
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), true))
                .get()
        );
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
                Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
            );
            final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName(keyName)
                .setExpiration(null)
                .setRoleDescriptors(Collections.singletonList(descriptor))
                .setMetadata(ApiKeyTests.randomMetadata())
                .get();
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> new CreateApiKeyRequestBuilder(client).get()
        );
        assertThat(e.getMessage(), containsString("api key name is required"));
    }

    public void testInvalidateApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingRealmName("file"), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingUserName(ES_TEST_ROOT_USER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingRealmAndUserName("file", ES_TEST_ROOT_USER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null).v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyName(responses.get(0).getName(), false),
            listener
        );
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
        final ApiKeyService serviceForDoc1 = services.stream()
            .filter(s -> s.getDocCache().get(apiKey1.v1()) != null)
            .findFirst()
            .orElseThrow();
        final ApiKeyService serviceForDoc2 = services.stream()
            .filter(s -> s.getDocCache().get(apiKey2.v1()) != null)
            .findFirst()
            .orElseThrow();
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
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
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> authenticateWithApiKey(apiKey1.v1(), new SecureString(apiKey1.v2().toCharArray()))
        );
        assertThat(e.getMessage(), containsString("security_exception"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private void verifyInvalidateResponse(
        int noOfApiKeys,
        List<CreateApiKeyResponse> responses,
        InvalidateApiKeyResponse invalidateResponse
    ) {
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(noOfApiKeys));
        assertThat(
            invalidateResponse.getInvalidatedApiKeys(),
            containsInAnyOrder(responses.stream().map(r -> r.getId()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY))
        );
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidatedApiKeysDeletedByRemover() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );

        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(2, null).v1();

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(0).getId(), false),
            listener
        );
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
        assertThat(
            getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 2 : 1)
        );

        client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );

        // invalidate API key to trigger remover
        listener = new PlainActionFuture<>();
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(1).getId(), false),
            listener
        );
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
        assertThat(
            getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 1 : 0)
        );
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
        assertBusy(() -> { assertThat(threadPool.relativeTimeInMillis() - lastRunTime, greaterThan(DELETE_INTERVAL_MILLIS)); });
        return internalCluster().client(nodeWithMostRecentRun);
    }

    public void testExpiredApiKeysBehaviorWhenKeysExpired1WeekBeforeAnd1DayBefore() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );

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
        UpdateResponse expirationDateUpdatedResponse = client.prepareUpdate(SECURITY_MAIN_ALIAS, createdApiKeys.get(0).getId())
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
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(2).getId(), false),
            listener
        );
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify get API keys does not return api keys deleted by ExpiredApiKeysRemover
        getApiKeyResponseListener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);

        Set<String> expectedKeyIds = Sets.newHashSet(
            createdApiKeys.get(0).getId(),
            createdApiKeys.get(2).getId(),
            createdApiKeys.get(3).getId()
        );
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
        assertThat(
            getApiKeyResponseListener.get().getApiKeyInfos().length,
            is((apiKeyInvalidatedButNotYetDeletedByExpiredApiKeysRemover) ? 3 : 2)
        );
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
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
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
        verifyGetResponse(
            2,
            responses,
            tuple.v2(),
            response,
            Collections.singleton(responses.get(0).getId()),
            Collections.singletonList(responses.get(1).getId())
        );
    }

    public void testGetApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(noOfApiKeys, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        boolean invalidate = randomBoolean();
        List<String> invalidatedApiKeyIds = null;
        Set<String> expectedValidKeyIds = null;
        if (invalidate) {
            PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            client.execute(
                InvalidateApiKeyAction.INSTANCE,
                InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false),
                listener
            );
            InvalidateApiKeyResponse invalidateResponse = listener.get();
            invalidatedApiKeyIds = invalidateResponse.getInvalidatedApiKeys();
            expectedValidKeyIds = responses.stream()
                .filter(o -> o.getId().equals(responses.get(0).getId()) == false)
                .map(o -> o.getId())
                .collect(Collectors.toSet());
        } else {
            invalidatedApiKeyIds = Collections.emptyList();
            expectedValidKeyIds = responses.stream().map(o -> o.getId()).collect(Collectors.toSet());
        }

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmName("file"), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, tuple.v2(), response, expectedValidKeyIds, invalidatedApiKeyIds);
    }

    public void testGetApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(noOfApiKeys, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingUserName(ES_TEST_ROOT_USER), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(
            noOfApiKeys,
            responses,
            tuple.v2(),
            response,
            responses.stream().map(o -> o.getId()).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(1, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingRealmAndUserName("file", ES_TEST_ROOT_USER), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(1, null);
        List<CreateApiKeyResponse> responses = tuple.v1();
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(responses.get(0).getId(), false), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        final Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING)
        );

        final int noOfApiKeys = randomIntBetween(1, 3);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple1 = createApiKeys(noOfApiKeys, null);
        final List<CreateApiKeyResponse> createApiKeyResponses1 = tuple1.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple2 = createApiKeys(
            headers,
            noOfApiKeys,
            "another-test-key-",
            null,
            "monitor"
        );
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
        verifyGetResponse(
            noOfApiKeys,
            createApiKeyResponses1,
            tuple1.v2(),
            listener2.get(),
            createApiKeyResponses1.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()),
            null
        );

        PlainActionFuture<GetApiKeyResponse> listener3 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("*", false), listener3);
        responses = Stream.concat(createApiKeyResponses1.stream(), createApiKeyResponses2.stream()).collect(Collectors.toList());
        metadatas = Stream.concat(tuple1.v2().stream(), tuple2.v2().stream()).collect(Collectors.toList());
        verifyGetResponse(
            2 * noOfApiKeys,
            responses,
            metadatas,
            listener3.get(),
            responses.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()),
            null
        );

        PlainActionFuture<GetApiKeyResponse> listener4 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("does-not-exist*", false), listener4);
        verifyGetResponse(0, Collections.emptyList(), null, listener4.get(), Collections.emptySet(), null);

        PlainActionFuture<GetApiKeyResponse> listener5 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName("another-test-key*", false), listener5);
        verifyGetResponse(
            noOfApiKeys,
            createApiKeyResponses2,
            tuple2.v2(),
            listener5.get(),
            createApiKeyResponses2.stream().map(CreateApiKeyResponse::getId).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetApiKeysOwnedByCurrentAuthenticatedUser() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = createApiKeys(noOfSuperuserApiKeys, null).v1();
        String userWithManageApiKeyRole = randomFrom("user_with_manage_api_key_role", "user_with_manage_own_api_key_role");
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(
            userWithManageApiKeyRole,
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        );
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = tuple.v1();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(userWithManageApiKeyRole, TEST_PASSWORD_SECURE_STRING))
        );

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(
            userWithManageApiKeyRole,
            noOfApiKeysForUserWithManageApiKeyRole,
            userWithManageApiKeyRoleApiKeys,
            tuple.v2(),
            response,
            userWithManageApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetApiKeysOwnedByRunAsUserWhenOwnerIsTrue() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        );
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = tuple.v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(
            "user_with_manage_own_api_key_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            userWithManageOwnApiKeyRoleApiKeys,
            tuple.v2(),
            response,
            userWithManageOwnApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetApiKeysOwnedByRunAsUserWhenRunAsUserInfoIsGiven() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        );
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = tuple.v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingRealmAndUserName("file", "user_with_manage_own_api_key_role"),
            listener
        );
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(
            "user_with_manage_own_api_key_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            userWithManageOwnApiKeyRoleApiKeys,
            tuple.v2(),
            response,
            userWithManageOwnApiKeyRoleApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetApiKeysOwnedByRunAsUserWillNotWorkWhenAuthUserInfoIsGiven() throws ExecutionException, InterruptedException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        final List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        @SuppressWarnings("unchecked")
        final Tuple<String, String> invalidRealmAndUserPair = randomFrom(
            new Tuple<>("file", "user_with_run_as_role"),
            new Tuple<>("index", "user_with_manage_own_api_key_role"),
            new Tuple<>("index", "user_with_run_as_role")
        );
        getClientForRunAsUser().execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingRealmAndUserName(invalidRealmAndUserPair.v1(), invalidRealmAndUserPair.v2()),
            listener
        );
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, listener::actionGet);
        assertThat(
            e.getMessage(),
            containsString("unauthorized for user [user_with_run_as_role] run as [user_with_manage_own_api_key_role]")
        );
    }

    public void testGetAllApiKeys() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageOwnApiKeyRole = randomIntBetween(3, 7);
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> defaultUserTuple = createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = defaultUserTuple.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> userWithManageTuple = createApiKeys(
            "user_with_manage_api_key_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        );
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = userWithManageTuple.v1();
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> userWithManageOwnTuple = createApiKeys(
            "user_with_manage_own_api_key_role",
            noOfApiKeysForUserWithManageOwnApiKeyRole,
            null,
            "monitor"
        );
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = userWithManageOwnTuple.v1();

        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue("user_with_manage_api_key_role", TEST_PASSWORD_SECURE_STRING))
        );
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, new GetApiKeyRequest(), listener);
        GetApiKeyResponse response = listener.get();
        int totalApiKeys = noOfSuperuserApiKeys + noOfApiKeysForUserWithManageApiKeyRole + noOfApiKeysForUserWithManageOwnApiKeyRole;
        List<CreateApiKeyResponse> allApiKeys = new ArrayList<>();
        Stream.of(defaultUserCreatedKeys, userWithManageApiKeyRoleApiKeys, userWithManageOwnApiKeyRoleApiKeys).forEach(allApiKeys::addAll);
        final List<Map<String, Object>> metadatas = Stream.of(defaultUserTuple.v2(), userWithManageTuple.v2(), userWithManageOwnTuple.v2())
            .flatMap(List::stream)
            .collect(Collectors.toList());
        verifyGetResponse(
            new String[] { ES_TEST_ROOT_USER, "user_with_manage_api_key_role", "user_with_manage_own_api_key_role" },
            totalApiKeys,
            allApiKeys,
            metadatas,
            response,
            allApiKeys.stream().map(o -> o.getId()).collect(Collectors.toSet()),
            null
        );
    }

    public void testGetAllApiKeysFailsForUserWithNoRoleOrRetrieveOwnApiKeyRole() throws InterruptedException, ExecutionException {
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageOwnApiKeyRole = randomIntBetween(3, 7);
        List<CreateApiKeyResponse> defaultUserCreatedKeys = createApiKeys(noOfSuperuserApiKeys, null).v1();
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_api_key_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
        List<CreateApiKeyResponse> userWithManageOwnApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_own_api_key_role",
            noOfApiKeysForUserWithManageOwnApiKeyRole,
            null,
            "monitor"
        ).v1();

        final String withUser = randomFrom("user_with_manage_own_api_key_role", "user_with_no_api_key_role");
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(withUser, TEST_PASSWORD_SECURE_STRING))
        );
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
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(
            userWithManageApiKeyRole,
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(userWithManageApiKeyRole, TEST_PASSWORD_SECURE_STRING))
        );

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
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
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
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        getClientForRunAsUser().execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingRealmAndUserName("file", "user_with_manage_own_api_key_role"),
            listener
        );
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeysForUserWithManageApiKeyRole, userWithManageApiKeyRoleApiKeys, invalidateResponse);
    }

    public void testInvalidateApiKeysOwnedByRunAsUserWillNotWorkWhenAuthUserInfoIsGiven() throws InterruptedException, ExecutionException {
        createUserWithRunAsRole();
        int noOfSuperuserApiKeys = randomIntBetween(3, 5);
        int noOfApiKeysForUserWithManageApiKeyRole = randomIntBetween(3, 5);
        createApiKeys(noOfSuperuserApiKeys, null);
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys(
            "user_with_manage_own_api_key_role",
            "user_with_run_as_role",
            noOfApiKeysForUserWithManageApiKeyRole,
            null,
            "monitor"
        ).v1();
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        @SuppressWarnings("unchecked")
        final Tuple<String, String> invalidRealmAndUserPair = randomFrom(
            new Tuple<>("file", "user_with_run_as_role"),
            new Tuple<>("index", "user_with_manage_own_api_key_role"),
            new Tuple<>("index", "user_with_run_as_role")
        );
        getClientForRunAsUser().execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingRealmAndUserName(invalidRealmAndUserPair.v1(), invalidRealmAndUserPair.v2()),
            listener
        );
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, listener::actionGet);
        assertThat(
            e.getMessage(),
            containsString("unauthorized for user [user_with_run_as_role] run as [user_with_manage_own_api_key_role]")
        );
    }

    public void testApiKeyAuthorizationApiKeyMustBeAbleToRetrieveItsOwnInformationButNotAnyOtherKeysCreatedBySameOwner()
        throws InterruptedException, ExecutionException {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> tuple = createApiKeys(
            ES_TEST_ROOT_USER,
            2,
            null,
            (String[]) null
        );
        List<CreateApiKeyResponse> responses = tuple.v1();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((responses.get(0).getId() + ":" + responses.get(0).getKey().toString()).getBytes(StandardCharsets.UTF_8));
        Client client = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(responses.get(0).getId(), randomBoolean()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, tuple.v2(), response, Collections.singleton(responses.get(0).getId()), null);

        final PlainActionFuture<GetApiKeyResponse> failureListener = new PlainActionFuture<>();
        // for any other API key id, it must deny access
        client.execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingApiKeyId(responses.get(1).getId(), randomBoolean()),
            failureListener
        );
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", ES_TEST_ROOT_USER, responses.get(0).getId());

        final PlainActionFuture<GetApiKeyResponse> failureListener1 = new PlainActionFuture<>();
        client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.forOwnedApiKeys(), failureListener1);
        ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener1.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", ES_TEST_ROOT_USER, responses.get(0).getId());
    }

    public void testApiKeyWithManageOwnPrivilegeIsAbleToInvalidateItselfButNotAnyOtherKeysCreatedBySameOwner() throws InterruptedException,
        ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(ES_TEST_ROOT_USER, 2, null, "manage_own_api_key").v1();
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((responses.get(0).getId() + ":" + responses.get(0).getKey().toString()).getBytes(StandardCharsets.UTF_8));
        Client client = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue));

        final PlainActionFuture<InvalidateApiKeyResponse> failureListener = new PlainActionFuture<>();
        // for any other API key id, it must deny access
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId(), randomBoolean()),
            failureListener
        );
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", ES_TEST_ROOT_USER, responses.get(0).getId());

        final PlainActionFuture<InvalidateApiKeyResponse> failureListener1 = new PlainActionFuture<>();
        client.execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.forOwnedApiKeys(), failureListener1);
        ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener1.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", ES_TEST_ROOT_USER, responses.get(0).getId());

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        client.execute(
            InvalidateApiKeyAction.INSTANCE,
            InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId(), randomBoolean()),
            listener
        );
        InvalidateApiKeyResponse invalidateResponse = listener.get();

        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getInvalidatedApiKeys(), containsInAnyOrder(responses.get(0).getId()));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testDerivedKeys() throws ExecutionException, InterruptedException {
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName("key-1")
            .setRoleDescriptors(
                Collections.singletonList(new RoleDescriptor("role", new String[] { "manage_api_key", "manage_token" }, null, null))
            )
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        assertEquals("key-1", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));

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

        final String expectedMessage = "creating derived api keys requires an explicit role descriptor that is empty";

        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-2").setMetadata(ApiKeyTests.randomMetadata()).get()
        );
        assertThat(e1.getMessage(), containsString(expectedMessage));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-3").setRoleDescriptors(Collections.emptyList()).get()
        );
        assertThat(e2.getMessage(), containsString(expectedMessage));

        final IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-4")
                .setMetadata(ApiKeyTests.randomMetadata())
                .setRoleDescriptors(
                    Collections.singletonList(new RoleDescriptor("role", new String[] { "manage_own_api_key" }, null, null))
                )
                .get()
        );
        assertThat(e3.getMessage(), containsString(expectedMessage));

        final List<RoleDescriptor> roleDescriptors = randomList(2, 10, () -> new RoleDescriptor("role", null, null, null));
        roleDescriptors.set(
            randomInt(roleDescriptors.size() - 1),
            new RoleDescriptor("role", new String[] { "manage_own_api_key" }, null, null)
        );

        final IllegalArgumentException e4 = expectThrows(
            IllegalArgumentException.class,
            () -> new CreateApiKeyRequestBuilder(clientKey1).setName("key-5")
                .setMetadata(ApiKeyTests.randomMetadata())
                .setRoleDescriptors(roleDescriptors)
                .get()
        );
        assertThat(e4.getMessage(), containsString(expectedMessage));

        final CreateApiKeyResponse key100Response = new CreateApiKeyRequestBuilder(clientKey1).setName("key-100")
            .setMetadata(ApiKeyTests.randomMetadata())
            .setRoleDescriptors(Collections.singletonList(new RoleDescriptor("role", null, null, null)))
            .get();
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

    public void testApiKeyRunAsAnotherUserCanCreateApiKey() {
        final RoleDescriptor descriptor = new RoleDescriptor("role", Strings.EMPTY_ARRAY, null, new String[] { ES_TEST_ROOT_USER });
        Client client = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final CreateApiKeyResponse response1 = new CreateApiKeyRequestBuilder(client).setName("run-as-key")
            .setRoleDescriptors(List.of(descriptor))
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        final String base64ApiKeyKeyValue = Base64.getEncoder()
            .encodeToString((response1.getId() + ":" + response1.getKey()).getBytes(StandardCharsets.UTF_8));

        final CreateApiKeyResponse response2 = new CreateApiKeyRequestBuilder(
            client().filterWithHeader(
                Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue, "es-security-runas-user", ES_TEST_ROOT_USER)
            )
        ).setName("create-by run-as user").setRoleDescriptors(List.of(new RoleDescriptor("a", new String[] { "all" }, null, null))).get();

        final GetApiKeyResponse getApiKeyResponse = client.execute(
            GetApiKeyAction.INSTANCE,
            GetApiKeyRequest.usingApiKeyId(response2.getId(), true)
        ).actionGet();
        assertThat(getApiKeyResponse.getApiKeyInfos(), arrayWithSize(1));
        final ApiKey apiKeyInfo = getApiKeyResponse.getApiKeyInfos()[0];
        assertThat(apiKeyInfo.getId(), equalTo(response2.getId()));
        assertThat(apiKeyInfo.getUsername(), equalTo(ES_TEST_ROOT_USER));
        assertThat(apiKeyInfo.getRealm(), equalTo("file"));
    }

    public void testCreationAndAuthenticationReturns429WhenThreadPoolIsSaturated() throws Exception {
        final String nodeName = randomFrom(internalCluster().getNodeNames());
        final Settings settings = internalCluster().getInstance(Settings.class, nodeName);
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        final ApiKeyService apiKeyService = internalCluster().getInstance(ApiKeyService.class, nodeName);

        final RoleDescriptor descriptor = new RoleDescriptor("auth_only", new String[] {}, null, null);
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyRequestBuilder(client).setName("auth only key")
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();

        assertNotNull(createApiKeyResponse.getId());
        assertNotNull(createApiKeyResponse.getKey());
        // Clear the auth cache to force recompute the expensive hash which requires the crypto thread pool
        apiKeyService.getApiKeyAuthCache().invalidateAll();

        final List<NodeInfo> nodeInfos = client().admin()
            .cluster()
            .prepareNodesInfo()
            .get()
            .getNodes()
            .stream()
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
                lastTaskFuture = executorService.submit(() -> {});
            }
        } catch (EsRejectedExecutionException e) {
            logger.info("Attempted to push {} tasks but only pushed {}", CRYPTO_THREAD_POOL_QUEUE_SIZE, i + 1);
        }

        try (RestClient restClient = createRestClient(nodeInfos, null, "http")) {
            final String base64ApiKeyKeyValue = Base64.getEncoder()
                .encodeToString(
                    (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString()).getBytes(StandardCharsets.UTF_8)
                );

            final Request authRequest = new Request("GET", "_security/_authenticate");
            authRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + base64ApiKeyKeyValue).build());
            final ResponseException e1 = expectThrows(ResponseException.class, () -> restClient.performRequest(authRequest));
            assertThat(e1.getMessage(), containsString("429 Too Many Requests"));
            assertThat(e1.getResponse().getStatusLine().getStatusCode(), is(429));

            final Request createApiKeyRequest = new Request("POST", "_security/api_key");
            createApiKeyRequest.setJsonEntity("{\"name\":\"key\"}");
            createApiKeyRequest.setOptions(
                createApiKeyRequest.getOptions()
                    .toBuilder()
                    .addHeader("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
            );
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
        final ApiKeyService serviceForDoc1 = services.stream().filter(s -> s.getDocCache().get(docId1) != null).findFirst().orElseThrow();
        final ApiKeyService serviceForDoc2 = services.stream().filter(s -> s.getDocCache().get(docId2) != null).findFirst().orElseThrow();
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
        ClearSecurityCacheResponse clearSecurityCacheResponse = client().execute(
            ClearSecurityCacheAction.INSTANCE,
            clearSecurityCacheRequest
        ).get();
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
        clearSecurityCacheResponse = client().execute(ClearSecurityCacheAction.INSTANCE, clearSecurityCacheRequest).get();
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
        final ApiKeyService apiKeyService = services.stream().filter(s -> s.getDocCache().count() > 0).findFirst().orElseThrow();
        assertNotNull(apiKeyService.getFromCache(docId));
        assertEquals(1, apiKeyService.getDocCache().count());
        assertEquals(2, apiKeyService.getRoleDescriptorsBytesCache().count());

        // Close security index to trigger invalidation
        final CloseIndexResponse closeIndexResponse = client().admin()
            .indices()
            .close(new CloseIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7))
            .get();
        assertTrue(closeIndexResponse.isAcknowledged());
        assertBusy(() -> {
            expectThrows(NullPointerException.class, () -> apiKeyService.getFromCache(docId));
            assertEquals(0, apiKeyService.getDocCache().count());
            assertEquals(0, apiKeyService.getRoleDescriptorsBytesCache().count());
        });
    }

    public void testUpdateApiKey() throws ExecutionException, InterruptedException, IOException {
        final Tuple<CreateApiKeyResponse, Map<String, Object>> createdApiKey = createApiKey(TEST_USER_NAME, null);
        final var apiKeyId = createdApiKey.v1().getId();
        final Map<String, Object> oldMetadata = createdApiKey.v2();
        final var newRoleDescriptors = randomRoleDescriptors();
        final boolean nullRoleDescriptors = newRoleDescriptors == null;
        // Role descriptor corresponding to SecuritySettingsSource.TEST_ROLE_YML
        final var expectedLimitedByRoleDescriptors = Set.of(
            new RoleDescriptor(
                TEST_ROLE,
                new String[] { "ALL" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").allowRestrictedIndices(true).privileges("ALL").build() },
                null
            )
        );
        final var request = new UpdateApiKeyRequest(apiKeyId, newRoleDescriptors, ApiKeyTests.randomMetadata());

        final UpdateApiKeyResponse response = executeUpdateApiKey(TEST_USER_NAME, request);

        assertNotNull(response);
        // In this test, non-null roleDescriptors always result in an update since they either update the role name, or associated
        // privileges. As such null descriptors (plus matching or null metadata) is the only way we can get a noop here
        final boolean metadataChanged = request.getMetadata() != null && false == request.getMetadata().equals(oldMetadata);
        final boolean isUpdated = nullRoleDescriptors == false || metadataChanged;
        assertEquals(isUpdated, response.isUpdated());

        final PlainActionFuture<GetApiKeyResponse> getListener = new PlainActionFuture<>();
        client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        ).execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyId(apiKeyId, false), getListener);
        final GetApiKeyResponse getResponse = getListener.get();
        assertEquals(1, getResponse.getApiKeyInfos().length);
        // When metadata for the update request is null (i.e., absent), we don't overwrite old metadata with it
        final var expectedMetadata = request.getMetadata() != null ? request.getMetadata() : createdApiKey.v2();
        assertEquals(expectedMetadata == null ? Map.of() : expectedMetadata, getResponse.getApiKeyInfos()[0].getMetadata());
        assertEquals(TEST_USER_NAME, getResponse.getApiKeyInfos()[0].getUsername());
        assertEquals("file", getResponse.getApiKeyInfos()[0].getRealm());

        // Test authenticate works with updated API key
        final var authResponse = authenticateWithApiKey(apiKeyId, createdApiKey.v1().getKey());
        assertThat(authResponse.get(User.Fields.USERNAME.getPreferredName()), equalTo(TEST_USER_NAME));

        // Document updated as expected
        final var updatedApiKeyDoc = getApiKeyDocument(apiKeyId);
        expectMetadataForApiKey(expectedMetadata, updatedApiKeyDoc);
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", expectedLimitedByRoleDescriptors, updatedApiKeyDoc);
        final var expectedRoleDescriptors = nullRoleDescriptors ? List.of(DEFAULT_API_KEY_ROLE_DESCRIPTOR) : newRoleDescriptors;
        expectRoleDescriptorsForApiKey("role_descriptors", expectedRoleDescriptors, updatedApiKeyDoc);
        final Map<String, Object> expectedCreator = new HashMap<>();
        expectedCreator.put("principal", TEST_USER_NAME);
        expectedCreator.put("full_name", null);
        expectedCreator.put("email", null);
        expectedCreator.put("metadata", Map.of());
        expectedCreator.put("realm_type", "file");
        expectedCreator.put("realm", "file");
        expectCreatorForApiKey(expectedCreator, updatedApiKeyDoc);

        // Check if update resulted in API key role going from `monitor` to `all` cluster privilege and assert that action that requires
        // `all` is authorized or denied accordingly
        final boolean hasAllClusterPrivilege = expectedRoleDescriptors.stream()
            .filter(rd -> Arrays.asList(rd.getClusterPrivileges()).contains("all"))
            .toList()
            .isEmpty() == false;
        final var authorizationHeaders = Collections.singletonMap(
            "Authorization",
            "ApiKey " + getBase64EncodedApiKeyValue(createdApiKey.v1().getId(), createdApiKey.v1().getKey())
        );
        if (hasAllClusterPrivilege) {
            createUserWithRunAsRole(authorizationHeaders);
        } else {
            ExecutionException e = expectThrows(ExecutionException.class, () -> createUserWithRunAsRole(authorizationHeaders));
            assertThat(e.getMessage(), containsString("unauthorized"));
            assertThat(e.getCause(), instanceOf(ElasticsearchSecurityException.class));
        }
    }

    public void testUpdateApiKeyAutoUpdatesUserFields() throws IOException, ExecutionException, InterruptedException {
        // Create separate native realm user and role for user role change test
        final var nativeRealmUser = randomAlphaOfLengthBetween(5, 10);
        final var nativeRealmRole = randomAlphaOfLengthBetween(5, 10);
        createNativeRealmUser(
            nativeRealmUser,
            nativeRealmRole,
            new String(HASHER.hash(TEST_PASSWORD_SECURE_STRING)),
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        );
        final List<String> clusterPrivileges = new ArrayList<>(randomSubsetOf(ClusterPrivilegeResolver.names()));
        // At a minimum include privilege to manage own API key to ensure no 403
        clusterPrivileges.add(randomFrom("manage_api_key", "manage_own_api_key"));
        final RoleDescriptor roleDescriptorBeforeUpdate = putRoleWithClusterPrivileges(
            nativeRealmRole,
            clusterPrivileges.toArray(new String[0])
        );

        // Create api key
        final CreateApiKeyResponse createdApiKey = createApiKeys(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(nativeRealmUser, TEST_PASSWORD_SECURE_STRING)),
            1,
            null,
            "all"
        ).v1().get(0);
        final String apiKeyId = createdApiKey.getId();
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", Set.of(roleDescriptorBeforeUpdate), getApiKeyDocument(apiKeyId));

        final List<String> newClusterPrivileges = new ArrayList<>(randomSubsetOf(ClusterPrivilegeResolver.names()));
        // At a minimum include privilege to manage own API key to ensure no 403
        newClusterPrivileges.add(randomFrom("manage_api_key", "manage_own_api_key"));
        // Update user role
        final RoleDescriptor roleDescriptorAfterUpdate = putRoleWithClusterPrivileges(
            nativeRealmRole,
            newClusterPrivileges.toArray(new String[0])
        );

        UpdateApiKeyResponse response = executeUpdateApiKey(nativeRealmUser, UpdateApiKeyRequest.usingApiKeyId(apiKeyId));

        assertNotNull(response);
        assertTrue(response.isUpdated());
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", Set.of(roleDescriptorAfterUpdate), getApiKeyDocument(apiKeyId));

        // Update user role name only
        final RoleDescriptor roleDescriptorWithNewName = putRoleWithClusterPrivileges(
            randomValueOtherThan(nativeRealmRole, () -> randomAlphaOfLength(10)),
            // Keep old privileges
            newClusterPrivileges.toArray(new String[0])
        );
        final User updatedUser = AuthenticationTestHelper.userWithRandomMetadataAndDetails(
            nativeRealmUser,
            roleDescriptorWithNewName.getName()
        );
        updateUser(updatedUser);

        // Update API key
        response = executeUpdateApiKey(nativeRealmUser, UpdateApiKeyRequest.usingApiKeyId(apiKeyId));

        assertNotNull(response);
        assertTrue(response.isUpdated());
        final Map<String, Object> updatedApiKeyDoc = getApiKeyDocument(apiKeyId);
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", Set.of(roleDescriptorWithNewName), updatedApiKeyDoc);
        final Map<String, Object> expectedCreator = new HashMap<>();
        expectedCreator.put("principal", updatedUser.principal());
        expectedCreator.put("full_name", updatedUser.fullName());
        expectedCreator.put("email", updatedUser.email());
        expectedCreator.put("metadata", updatedUser.metadata());
        expectedCreator.put("realm_type", "native");
        expectedCreator.put("realm", "index");
        expectCreatorForApiKey(expectedCreator, updatedApiKeyDoc);
    }

    public void testUpdateApiKeyNotFoundScenarios() throws ExecutionException, InterruptedException {
        final Tuple<CreateApiKeyResponse, Map<String, Object>> createdApiKey = createApiKey(TEST_USER_NAME, null);
        final var apiKeyId = createdApiKey.v1().getId();
        final var expectedRoleDescriptor = new RoleDescriptor(randomAlphaOfLength(10), new String[] { "all" }, null, null);
        final var request = new UpdateApiKeyRequest(apiKeyId, List.of(expectedRoleDescriptor), ApiKeyTests.randomMetadata());

        // Validate can update own API key
        final UpdateApiKeyResponse response = executeUpdateApiKey(TEST_USER_NAME, request);
        assertNotNull(response);
        assertTrue(response.isUpdated());

        // Test not found exception on non-existent API key
        final var otherApiKeyId = randomValueOtherThan(apiKeyId, () -> randomAlphaOfLength(20));
        doTestUpdateApiKeyNotFound(new UpdateApiKeyRequest(otherApiKeyId, request.getRoleDescriptors(), request.getMetadata()));

        // Test not found exception on other user's API key
        final Tuple<CreateApiKeyResponse, Map<String, Object>> otherUsersApiKey = createApiKey("user_with_manage_api_key_role", null);
        doTestUpdateApiKeyNotFound(
            new UpdateApiKeyRequest(otherUsersApiKey.v1().getId(), request.getRoleDescriptors(), request.getMetadata())
        );

        // Test not found exception on API key of user with the same username but from a different realm
        // Create native realm user with same username but different password to allow us to create an API key for _that_ user
        // instead of file realm one
        final var passwordSecureString = new SecureString("x-pack-test-other-password".toCharArray());
        createNativeRealmUser(
            TEST_USER_NAME,
            TEST_ROLE,
            new String(HASHER.hash(passwordSecureString)),
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        );
        final CreateApiKeyResponse apiKeyForNativeRealmUser = createApiKeys(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, passwordSecureString)),
            1,
            null,
            "all"
        ).v1().get(0);
        doTestUpdateApiKeyNotFound(
            new UpdateApiKeyRequest(apiKeyForNativeRealmUser.getId(), request.getRoleDescriptors(), request.getMetadata())
        );
    }

    public void testInvalidUpdateApiKeyScenarios() throws ExecutionException, InterruptedException {
        final List<String> apiKeyPrivileges = new ArrayList<>(randomSubsetOf(ClusterPrivilegeResolver.names()));
        // At a minimum include privilege to manage own API key to ensure no 403
        apiKeyPrivileges.add(randomFrom("manage_api_key", "manage_own_api_key"));
        final CreateApiKeyResponse createdApiKey = createApiKeys(TEST_USER_NAME, 1, null, apiKeyPrivileges.toArray(new String[0])).v1()
            .get(0);
        final var apiKeyId = createdApiKey.getId();

        final var roleDescriptor = new RoleDescriptor(randomAlphaOfLength(10), new String[] { "manage_own_api_key" }, null, null);
        final var request = new UpdateApiKeyRequest(apiKeyId, List.of(roleDescriptor), ApiKeyTests.randomMetadata());
        PlainActionFuture<UpdateApiKeyResponse> updateListener = new PlainActionFuture<>();
        client().filterWithHeader(
            Collections.singletonMap(
                "Authorization",
                "ApiKey " + getBase64EncodedApiKeyValue(createdApiKey.getId(), createdApiKey.getKey())
            )
        ).execute(UpdateApiKeyAction.INSTANCE, request, updateListener);

        final var apiKeysNotAllowedEx = expectThrows(ExecutionException.class, updateListener::get);
        assertThat(apiKeysNotAllowedEx.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            apiKeysNotAllowedEx.getMessage(),
            containsString("authentication via API key not supported: only the owner user can update an API key")
        );

        final boolean invalidated = randomBoolean();
        if (invalidated) {
            final PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            client().execute(InvalidateApiKeyAction.INSTANCE, InvalidateApiKeyRequest.usingRealmName("file"), listener);
            final var invalidateResponse = listener.get();
            assertThat(invalidateResponse.getErrors(), empty());
            assertThat(invalidateResponse.getInvalidatedApiKeys(), contains(apiKeyId));
        }
        if (invalidated == false || randomBoolean()) {
            final var dayBefore = Instant.now().minus(1L, ChronoUnit.DAYS);
            assertTrue(Instant.now().isAfter(dayBefore));
            final var expirationDateUpdatedResponse = client().prepareUpdate(SECURITY_MAIN_ALIAS, apiKeyId)
                .setDoc("expiration_time", dayBefore.toEpochMilli())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));
        }

        updateListener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        );
        client.execute(UpdateApiKeyAction.INSTANCE, request, updateListener);
        final var ex = expectThrows(ExecutionException.class, updateListener::get);

        assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
        if (invalidated) {
            assertThat(ex.getMessage(), containsString("cannot update invalidated API key [" + apiKeyId + "]"));
        } else {
            assertThat(ex.getMessage(), containsString("cannot update expired API key [" + apiKeyId + "]"));
        }
    }

    public void testUpdateApiKeyAccountsForSecurityDomains() throws ExecutionException, InterruptedException, IOException {
        final Tuple<CreateApiKeyResponse, Map<String, Object>> createdApiKey = createApiKey(TEST_USER_NAME, null);
        final var apiKeyId = createdApiKey.v1().getId();

        final ServiceWithNodeName serviceWithNodeName = getServiceWithNodeName();
        final PlainActionFuture<UpdateApiKeyResponse> listener = new PlainActionFuture<>();
        final RealmConfig.RealmIdentifier creatorRealmOnCreatedApiKey = new RealmConfig.RealmIdentifier(FileRealmSettings.TYPE, "file");
        final RealmConfig.RealmIdentifier otherRealmInDomain = AuthenticationTestHelper.randomRealmIdentifier(true);
        final var realmDomain = new RealmDomain(
            ESTestCase.randomAlphaOfLengthBetween(3, 8),
            Set.of(creatorRealmOnCreatedApiKey, otherRealmInDomain)
        );
        // Update should work for any of the realms within the domain
        final var authenticatingRealm = randomFrom(creatorRealmOnCreatedApiKey, otherRealmInDomain);
        final var authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder()
                .user(new User(TEST_USER_NAME, TEST_ROLE))
                .realmRef(
                    new Authentication.RealmRef(
                        authenticatingRealm.getName(),
                        authenticatingRealm.getType(),
                        serviceWithNodeName.nodeName(),
                        realmDomain
                    )
                )
                .build()
        );
        serviceWithNodeName.service().updateApiKey(authentication, UpdateApiKeyRequest.usingApiKeyId(apiKeyId), Set.of(), listener);
        final UpdateApiKeyResponse response = listener.get();

        assertNotNull(response);
        assertTrue(response.isUpdated());
        final Map<String, Object> expectedCreator = new HashMap<>();
        expectedCreator.put("principal", TEST_USER_NAME);
        expectedCreator.put("full_name", null);
        expectedCreator.put("email", null);
        expectedCreator.put("metadata", Map.of());
        expectedCreator.put("realm_type", authenticatingRealm.getType());
        expectedCreator.put("realm", authenticatingRealm.getName());
        final XContentBuilder builder = realmDomain.toXContent(XContentFactory.jsonBuilder(), null);
        expectedCreator.put("realm_domain", XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2());
        expectCreatorForApiKey(expectedCreator, getApiKeyDocument(apiKeyId));
    }

    public void testNoopUpdateApiKey() throws ExecutionException, InterruptedException, IOException {
        final Tuple<CreateApiKeyResponse, Map<String, Object>> createdApiKey = createApiKey(TEST_USER_NAME, null);
        final var apiKeyId = createdApiKey.v1().getId();

        final var initialRequest = new UpdateApiKeyRequest(
            apiKeyId,
            List.of(new RoleDescriptor(randomAlphaOfLength(10), new String[] { "all" }, null, null)),
            // Ensure not `null` to set metadata since we use the initialRequest further down in the test to ensure that
            // metadata updates are non-noops
            randomValueOtherThanMany(Objects::isNull, ApiKeyTests::randomMetadata)
        );
        UpdateApiKeyResponse response = executeUpdateApiKey(TEST_USER_NAME, initialRequest);
        assertNotNull(response);
        // First update is not noop, because role descriptors changed and possibly metadata
        assertTrue(response.isUpdated());

        // Update with same request is a noop and does not clear cache
        authenticateWithApiKey(apiKeyId, createdApiKey.v1().getKey());
        final var serviceWithNameForDoc1 = Arrays.stream(internalCluster().getNodeNames())
            .map(n -> internalCluster().getInstance(ApiKeyService.class, n))
            .filter(s -> s.getDocCache().get(apiKeyId) != null)
            .findFirst()
            .orElseThrow();
        final int count = serviceWithNameForDoc1.getDocCache().count();
        response = executeUpdateApiKey(TEST_USER_NAME, initialRequest);
        assertNotNull(response);
        assertFalse(response.isUpdated());
        assertEquals(count, serviceWithNameForDoc1.getDocCache().count());

        // Update with empty request is a noop
        response = executeUpdateApiKey(TEST_USER_NAME, UpdateApiKeyRequest.usingApiKeyId(apiKeyId));
        assertNotNull(response);
        assertFalse(response.isUpdated());

        // Update with different role descriptors is not a noop
        final List<RoleDescriptor> newRoleDescriptors = List.of(
            randomValueOtherThanMany(
                rd -> RoleDescriptorRequestValidator.validate(rd) != null || initialRequest.getRoleDescriptors().contains(rd),
                () -> RoleDescriptorTests.randomRoleDescriptor(false)
            ),
            randomValueOtherThanMany(
                rd -> RoleDescriptorRequestValidator.validate(rd) != null || initialRequest.getRoleDescriptors().contains(rd),
                () -> RoleDescriptorTests.randomRoleDescriptor(false)
            )
        );
        response = executeUpdateApiKey(TEST_USER_NAME, new UpdateApiKeyRequest(apiKeyId, newRoleDescriptors, null));
        assertNotNull(response);
        assertTrue(response.isUpdated());

        // Update with re-ordered role descriptors is a noop
        response = executeUpdateApiKey(
            TEST_USER_NAME,
            new UpdateApiKeyRequest(apiKeyId, List.of(newRoleDescriptors.get(1), newRoleDescriptors.get(0)), null)
        );
        assertNotNull(response);
        assertFalse(response.isUpdated());

        // Update with different metadata is not a noop
        response = executeUpdateApiKey(
            TEST_USER_NAME,
            new UpdateApiKeyRequest(
                apiKeyId,
                null,
                randomValueOtherThanMany(md -> md == null || md.equals(initialRequest.getMetadata()), ApiKeyTests::randomMetadata)
            )
        );
        assertNotNull(response);
        assertTrue(response.isUpdated());

        // Update with different creator info is not a noop
        // First, ensure that the user role descriptors alone do *not* cause an update, so we can test that we correctly perform the noop
        // check when we update creator info
        final ServiceWithNodeName serviceWithNodeName = getServiceWithNodeName();
        PlainActionFuture<UpdateApiKeyResponse> listener = new PlainActionFuture<>();
        // Role descriptor corresponding to SecuritySettingsSource.TEST_ROLE_YML, i.e., should not result in update
        final Set<RoleDescriptor> oldUserRoleDescriptors = Set.of(
            new RoleDescriptor(
                TEST_ROLE,
                new String[] { "ALL" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").allowRestrictedIndices(true).privileges("ALL").build() },
                null
            )
        );
        serviceWithNodeName.service()
            .updateApiKey(
                Authentication.newRealmAuthentication(
                    new User(TEST_USER_NAME, TEST_ROLE),
                    new Authentication.RealmRef("file", "file", serviceWithNodeName.nodeName())
                ),
                UpdateApiKeyRequest.usingApiKeyId(apiKeyId),
                oldUserRoleDescriptors,
                listener
            );
        response = listener.get();
        assertNotNull(response);
        assertFalse(response.isUpdated());
        final User updatedUser = AuthenticationTestHelper.userWithRandomMetadataAndDetails(TEST_USER_NAME, TEST_ROLE);
        final RealmConfig.RealmIdentifier creatorRealmOnCreatedApiKey = new RealmConfig.RealmIdentifier(FileRealmSettings.TYPE, "file");
        final boolean noUserChanges = updatedUser.equals(new User(TEST_USER_NAME, TEST_ROLE));
        final Authentication.RealmRef realmRef;
        if (randomBoolean() || noUserChanges) {
            final RealmConfig.RealmIdentifier otherRealmInDomain = AuthenticationTestHelper.randomRealmIdentifier(true);
            final var realmDomain = new RealmDomain(
                ESTestCase.randomAlphaOfLengthBetween(3, 8),
                Set.of(creatorRealmOnCreatedApiKey, otherRealmInDomain)
            );
            // Using other realm from domain should result in update
            realmRef = new Authentication.RealmRef(
                otherRealmInDomain.getName(),
                otherRealmInDomain.getType(),
                serviceWithNodeName.nodeName(),
                realmDomain
            );
        } else {
            realmRef = new Authentication.RealmRef(
                creatorRealmOnCreatedApiKey.getName(),
                creatorRealmOnCreatedApiKey.getType(),
                serviceWithNodeName.nodeName()
            );
        }
        final var authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder().user(updatedUser).realmRef(realmRef).build()
        );
        listener = new PlainActionFuture<>();
        serviceWithNodeName.service()
            .updateApiKey(authentication, UpdateApiKeyRequest.usingApiKeyId(apiKeyId), oldUserRoleDescriptors, listener);
        response = listener.get();
        assertNotNull(response);
        assertTrue(response.isUpdated());
    }

    public void testUpdateApiKeyAutoUpdatesLegacySuperuserRoleDescriptor() throws ExecutionException, InterruptedException, IOException {
        final Tuple<CreateApiKeyResponse, Map<String, Object>> createdApiKey = createApiKey(TEST_USER_NAME, null);
        final var apiKeyId = createdApiKey.v1().getId();
        final ServiceWithNodeName serviceWithNodeName = getServiceWithNodeName();
        final Authentication authentication = Authentication.newRealmAuthentication(
            new User(TEST_USER_NAME, TEST_ROLE),
            new Authentication.RealmRef("file", "file", serviceWithNodeName.nodeName())
        );
        final Set<RoleDescriptor> legacySuperuserRoleDescriptor = Set.of(ApiKeyService.LEGACY_SUPERUSER_ROLE_DESCRIPTOR);
        PlainActionFuture<UpdateApiKeyResponse> listener = new PlainActionFuture<>();
        // Force set user role descriptors to 7.x legacy superuser role descriptors
        serviceWithNodeName.service()
            .updateApiKey(authentication, UpdateApiKeyRequest.usingApiKeyId(apiKeyId), legacySuperuserRoleDescriptor, listener);
        UpdateApiKeyResponse response = listener.get();
        assertNotNull(response);
        assertTrue(response.isUpdated());
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", legacySuperuserRoleDescriptor, getApiKeyDocument(apiKeyId));

        final Set<RoleDescriptor> currentSuperuserRoleDescriptors = Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        PlainActionFuture<UpdateApiKeyResponse> listener2 = new PlainActionFuture<>();
        serviceWithNodeName.service()
            .updateApiKey(authentication, UpdateApiKeyRequest.usingApiKeyId(apiKeyId), currentSuperuserRoleDescriptors, listener2);
        response = listener2.get();
        assertNotNull(response);
        // The first request is not a noop because we are auto-updating the legacy role descriptors to 8.x role descriptors
        assertTrue(response.isUpdated());
        expectRoleDescriptorsForApiKey("limited_by_role_descriptors", currentSuperuserRoleDescriptors, getApiKeyDocument(apiKeyId));

        PlainActionFuture<UpdateApiKeyResponse> listener3 = new PlainActionFuture<>();
        serviceWithNodeName.service()
            .updateApiKey(authentication, UpdateApiKeyRequest.usingApiKeyId(apiKeyId), currentSuperuserRoleDescriptors, listener3);
        response = listener3.get();
        assertNotNull(response);
        // Second update is noop because role descriptors were auto-updated by the previous request
        assertFalse(response.isUpdated());
    }

    public void testUpdateApiKeyClearsApiKeyDocCache() throws IOException, ExecutionException, InterruptedException {
        final List<ServiceWithNodeName> services = Arrays.stream(internalCluster().getNodeNames())
            .map(n -> new ServiceWithNodeName(internalCluster().getInstance(ApiKeyService.class, n), n))
            .toList();

        // Create two API keys and authenticate with them
        final var apiKey1 = createApiKeyAndAuthenticateWithIt();
        final var apiKey2 = createApiKeyAndAuthenticateWithIt();

        // Find out which nodes handled the above authentication requests
        final var serviceWithNameForDoc1 = services.stream()
            .filter(s -> s.service().getDocCache().get(apiKey1.v1()) != null)
            .findFirst()
            .orElseThrow();
        final var serviceWithNameForDoc2 = services.stream()
            .filter(s -> s.service().getDocCache().get(apiKey2.v1()) != null)
            .findFirst()
            .orElseThrow();
        final var serviceForDoc1 = serviceWithNameForDoc1.service();
        final var serviceForDoc2 = serviceWithNameForDoc2.service();
        assertNotNull(serviceForDoc1.getFromCache(apiKey1.v1()));
        assertNotNull(serviceForDoc2.getFromCache(apiKey2.v1()));

        final boolean sameServiceNode = serviceWithNameForDoc1 == serviceWithNameForDoc2;
        if (sameServiceNode) {
            assertEquals(2, serviceForDoc1.getDocCache().count());
        } else {
            assertEquals(1, serviceForDoc1.getDocCache().count());
            assertEquals(1, serviceForDoc2.getDocCache().count());
        }

        final int serviceForDoc1AuthCacheCount = serviceForDoc1.getApiKeyAuthCache().count();
        final int serviceForDoc2AuthCacheCount = serviceForDoc2.getApiKeyAuthCache().count();

        // Update the first key
        final PlainActionFuture<UpdateApiKeyResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        client.execute(
            UpdateApiKeyAction.INSTANCE,
            // Set metadata to ensure update
            new UpdateApiKeyRequest(apiKey1.v1(), List.of(), Map.of(randomAlphaOfLength(5), randomAlphaOfLength(10))),
            listener
        );
        final var response = listener.get();
        assertNotNull(response);
        assertTrue(response.isUpdated());

        // The doc cache entry should be gone for the first key
        if (sameServiceNode) {
            assertEquals(1, serviceForDoc1.getDocCache().count());
            assertNull(serviceForDoc1.getDocCache().get(apiKey1.v1()));
            assertNotNull(serviceForDoc1.getDocCache().get(apiKey2.v1()));
        } else {
            assertEquals(0, serviceForDoc1.getDocCache().count());
            assertEquals(1, serviceForDoc2.getDocCache().count());
        }

        // Auth cache has not been affected
        assertEquals(serviceForDoc1AuthCacheCount, serviceForDoc1.getApiKeyAuthCache().count());
        assertEquals(serviceForDoc2AuthCacheCount, serviceForDoc2.getApiKeyAuthCache().count());
    }

    private List<RoleDescriptor> randomRoleDescriptors() {
        int caseNo = randomIntBetween(0, 3);
        return switch (caseNo) {
            case 0 -> List.of(new RoleDescriptor(randomAlphaOfLength(10), new String[] { "all" }, null, null));
            case 1 -> List.of(
                new RoleDescriptor(randomAlphaOfLength(10), new String[] { "all" }, null, null),
                randomValueOtherThanMany(
                    rd -> RoleDescriptorRequestValidator.validate(rd) != null,
                    () -> RoleDescriptorTests.randomRoleDescriptor(false)
                )
            );
            case 2 -> null;
            // vary default role descriptor assigned to created API keys by name only
            case 3 -> List.of(
                new RoleDescriptor(
                    randomValueOtherThan(DEFAULT_API_KEY_ROLE_DESCRIPTOR.getName(), () -> randomAlphaOfLength(10)),
                    DEFAULT_API_KEY_ROLE_DESCRIPTOR.getClusterPrivileges(),
                    DEFAULT_API_KEY_ROLE_DESCRIPTOR.getIndicesPrivileges(),
                    DEFAULT_API_KEY_ROLE_DESCRIPTOR.getRunAs()
                )
            );
            default -> throw new IllegalStateException("unexpected case no");
        };
    }

    private void doTestUpdateApiKeyNotFound(UpdateApiKeyRequest request) {
        final PlainActionFuture<UpdateApiKeyResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        );
        client.execute(UpdateApiKeyAction.INSTANCE, request, listener);
        final var ex = expectThrows(ExecutionException.class, listener::get);
        assertThat(ex.getCause(), instanceOf(ResourceNotFoundException.class));
        assertThat(ex.getMessage(), containsString("no API key owned by requesting user found for ID [" + request.getId() + "]"));
    }

    private void expectMetadataForApiKey(final Map<String, Object> expectedMetadata, final Map<String, Object> actualRawApiKeyDoc) {
        assertNotNull(actualRawApiKeyDoc);
        @SuppressWarnings("unchecked")
        final var actualMetadata = (Map<String, Object>) actualRawApiKeyDoc.get("metadata_flattened");
        assertThat("for api key doc " + actualRawApiKeyDoc, actualMetadata, equalTo(expectedMetadata));
    }

    private void expectCreatorForApiKey(final Map<String, Object> expectedCreator, final Map<String, Object> actualRawApiKeyDoc) {
        assertNotNull(actualRawApiKeyDoc);
        @SuppressWarnings("unchecked")
        final var actualCreator = (Map<String, Object>) actualRawApiKeyDoc.get("creator");
        assertThat("for api key doc " + actualRawApiKeyDoc, actualCreator, equalTo(expectedCreator));
    }

    @SuppressWarnings("unchecked")
    private void expectRoleDescriptorsForApiKey(
        final String roleDescriptorType,
        final Collection<RoleDescriptor> expectedRoleDescriptors,
        final Map<String, Object> actualRawApiKeyDoc
    ) throws IOException {
        assertNotNull(actualRawApiKeyDoc);
        assertThat(roleDescriptorType, in(new String[] { "role_descriptors", "limited_by_role_descriptors" }));
        final var rawRoleDescriptor = (Map<String, Object>) actualRawApiKeyDoc.get(roleDescriptorType);
        assertEquals(expectedRoleDescriptors.size(), rawRoleDescriptor.size());
        for (RoleDescriptor expectedRoleDescriptor : expectedRoleDescriptors) {
            assertThat(rawRoleDescriptor, hasKey(expectedRoleDescriptor.getName()));
            final var descriptor = (Map<String, ?>) rawRoleDescriptor.get(expectedRoleDescriptor.getName());
            final var roleDescriptor = RoleDescriptor.parse(
                expectedRoleDescriptor.getName(),
                XContentTestUtils.convertToXContent(descriptor, XContentType.JSON),
                false,
                XContentType.JSON
            );
            assertEquals(expectedRoleDescriptor, roleDescriptor);
        }
    }

    private Map<String, Object> getApiKeyDocument(String apiKeyId) {
        return client().execute(GetAction.INSTANCE, new GetRequest(SECURITY_MAIN_ALIAS, apiKeyId)).actionGet().getSource();
    }

    private ServiceWithNodeName getServiceWithNodeName() {
        final var nodeName = randomFrom(internalCluster().getNodeNames());
        final var service = internalCluster().getInstance(ApiKeyService.class, nodeName);
        return new ServiceWithNodeName(service, nodeName);
    }

    private record ServiceWithNodeName(ApiKeyService service, String nodeName) {}

    private Tuple<String, String> createApiKeyAndAuthenticateWithIt() throws IOException {
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );

        final CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyRequestBuilder(client).setName("test key")
            .setMetadata(ApiKeyTests.randomMetadata())
            .get();
        final String docId = createApiKeyResponse.getId();
        authenticateWithApiKey(docId, createApiKeyResponse.getKey());
        return Tuple.tuple(docId, createApiKeyResponse.getKey().toString());
    }

    private Map<String, Object> authenticateWithApiKey(String id, SecureString key) throws IOException {
        final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", "ApiKey " + getBase64EncodedApiKeyValue(id, key))
            .build();
        final TestSecurityClient securityClient = getSecurityClient(requestOptions);
        final Map<String, Object> response = securityClient.authenticate();

        final String authenticationTypeString = String.valueOf(response.get(User.Fields.AUTHENTICATION_TYPE.getPreferredName()));
        final Authentication.AuthenticationType authenticationType = Authentication.AuthenticationType.valueOf(
            authenticationTypeString.toUpperCase(Locale.ROOT)
        );
        assertThat(authenticationType, is(Authentication.AuthenticationType.API_KEY));

        assertThat(ObjectPath.evaluate(response, "api_key.id"), is(id));

        return response;
    }

    private String getBase64EncodedApiKeyValue(String id, SecureString key) {
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        return base64ApiKeyKeyValue;
    }

    private void assertApiKeyNotCreated(Client client, String keyName) throws ExecutionException, InterruptedException {
        new RefreshRequestBuilder(client, RefreshAction.INSTANCE).setIndices(SECURITY_MAIN_ALIAS).execute().get();
        assertEquals(
            0,
            client.execute(GetApiKeyAction.INSTANCE, GetApiKeyRequest.usingApiKeyName(keyName, false)).get().getApiKeyInfos().length
        );
    }

    private void verifyGetResponse(
        int expectedNumberOfApiKeys,
        List<CreateApiKeyResponse> responses,
        List<Map<String, Object>> metadatas,
        GetApiKeyResponse response,
        Set<String> validApiKeyIds,
        List<String> invalidatedApiKeyIds
    ) {
        verifyGetResponse(ES_TEST_ROOT_USER, expectedNumberOfApiKeys, responses, metadatas, response, validApiKeyIds, invalidatedApiKeyIds);
    }

    private void verifyGetResponse(
        String user,
        int expectedNumberOfApiKeys,
        List<CreateApiKeyResponse> responses,
        List<Map<String, Object>> metadatas,
        GetApiKeyResponse response,
        Set<String> validApiKeyIds,
        List<String> invalidatedApiKeyIds
    ) {
        verifyGetResponse(
            new String[] { user },
            expectedNumberOfApiKeys,
            responses,
            metadatas,
            response,
            validApiKeyIds,
            invalidatedApiKeyIds
        );
    }

    private void verifyGetResponse(
        String[] user,
        int expectedNumberOfApiKeys,
        List<CreateApiKeyResponse> responses,
        List<Map<String, Object>> metadatas,
        GetApiKeyResponse response,
        Set<String> validApiKeyIds,
        List<String> invalidatedApiKeyIds
    ) {
        assertThat(response.getApiKeyInfos().length, equalTo(expectedNumberOfApiKeys));
        List<String> expectedIds = responses.stream()
            .filter(o -> validApiKeyIds.contains(o.getId()))
            .map(o -> o.getId())
            .collect(Collectors.toList());
        List<String> actualIds = Arrays.stream(response.getApiKeyInfos())
            .filter(o -> o.isInvalidated() == false)
            .map(o -> o.getId())
            .collect(Collectors.toList());
        assertThat(actualIds, containsInAnyOrder(expectedIds.toArray(Strings.EMPTY_ARRAY)));
        List<String> expectedNames = responses.stream()
            .filter(o -> validApiKeyIds.contains(o.getId()))
            .map(o -> o.getName())
            .collect(Collectors.toList());
        List<String> actualNames = Arrays.stream(response.getApiKeyInfos())
            .filter(o -> o.isInvalidated() == false)
            .map(o -> o.getName())
            .collect(Collectors.toList());
        assertThat(actualNames, containsInAnyOrder(expectedNames.toArray(Strings.EMPTY_ARRAY)));
        Set<String> expectedUsernames = (validApiKeyIds.isEmpty()) ? Collections.emptySet() : Set.of(user);
        Set<String> actualUsernames = Arrays.stream(response.getApiKeyInfos())
            .filter(o -> o.isInvalidated() == false)
            .map(o -> o.getUsername())
            .collect(Collectors.toSet());
        assertThat(actualUsernames, containsInAnyOrder(expectedUsernames.toArray(Strings.EMPTY_ARRAY)));
        if (invalidatedApiKeyIds != null) {
            List<String> actualInvalidatedApiKeyIds = Arrays.stream(response.getApiKeyInfos())
                .filter(o -> o.isInvalidated())
                .map(o -> o.getId())
                .collect(Collectors.toList());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(actualInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
        }
        if (metadatas != null) {
            final HashMap<String, Map<String, Object>> idToMetadata = IntStream.range(0, responses.size())
                .collect(
                    (Supplier<HashMap<String, Map<String, Object>>>) HashMap::new,
                    (m, i) -> m.put(responses.get(i).getId(), metadatas.get(i)),
                    HashMap::putAll
                );
            for (ApiKey apiKey : response.getApiKeyInfos()) {
                final Map<String, Object> metadata = idToMetadata.get(apiKey.getId());
                assertThat(apiKey.getMetadata(), equalTo(metadata == null ? Map.of() : metadata));
            }
        }
    }

    private Tuple<CreateApiKeyResponse, Map<String, Object>> createApiKey(String user, TimeValue expiration) {
        final Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> res = createApiKeys(
            user,
            1,
            expiration,
            DEFAULT_API_KEY_ROLE_DESCRIPTOR.getClusterPrivileges()
        );
        return new Tuple<>(res.v1().get(0), res.v2().get(0));
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(int noOfApiKeys, TimeValue expiration) {
        return createApiKeys(ES_TEST_ROOT_USER, noOfApiKeys, expiration, DEFAULT_API_KEY_ROLE_DESCRIPTOR.getClusterPrivileges());
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        String user,
        int noOfApiKeys,
        TimeValue expiration,
        String... clusterPrivileges
    ) {
        final Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(user, TEST_PASSWORD_SECURE_STRING)
        );
        return createApiKeys(headers, noOfApiKeys, expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        String owningUser,
        String authenticatingUser,
        int noOfApiKeys,
        TimeValue expiration,
        String... clusterPrivileges
    ) {
        final Map<String, String> headers = Map.of(
            "Authorization",
            basicAuthHeaderValue(authenticatingUser, TEST_PASSWORD_SECURE_STRING),
            "es-security-runas-user",
            owningUser
        );
        return createApiKeys(headers, noOfApiKeys, expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        Map<String, String> headers,
        int noOfApiKeys,
        TimeValue expiration,
        String... clusterPrivileges
    ) {
        return createApiKeys(headers, noOfApiKeys, "test-key-", expiration, clusterPrivileges);
    }

    private Tuple<List<CreateApiKeyResponse>, List<Map<String, Object>>> createApiKeys(
        Map<String, String> headers,
        int noOfApiKeys,
        String namePrefix,
        TimeValue expiration,
        String... clusterPrivileges
    ) {
        List<Map<String, Object>> metadatas = new ArrayList<>(noOfApiKeys);
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        for (int i = 0; i < noOfApiKeys; i++) {
            final RoleDescriptor descriptor = new RoleDescriptor(DEFAULT_API_KEY_ROLE_DESCRIPTOR.getName(), clusterPrivileges, null, null);
            Client client = client().filterWithHeader(headers);
            final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
            metadatas.add(metadata);
            final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName(
                namePrefix + randomAlphaOfLengthBetween(5, 9) + i
            ).setExpiration(expiration).setRoleDescriptors(Collections.singletonList(descriptor)).setMetadata(metadata).get();
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
        createUserWithRunAsRole(Map.of("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING)));
    }

    private void createUserWithRunAsRole(Map<String, String> authHeaders) throws ExecutionException, InterruptedException {
        createNativeRealmUser("user_with_run_as_role", "run_as_role", SecuritySettingsSource.TEST_PASSWORD_HASHED, authHeaders);
    }

    private void createNativeRealmUser(
        final String username,
        final String role,
        final String passwordHashed,
        final Map<String, String> authHeaders
    ) throws ExecutionException, InterruptedException {
        final PutUserRequest putUserRequest = new PutUserRequest();
        putUserRequest.username(username);
        putUserRequest.roles(role);
        putUserRequest.passwordHash(passwordHashed.toCharArray());
        PlainActionFuture<PutUserResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(authHeaders);
        client.execute(PutUserAction.INSTANCE, putUserRequest, listener);
        final PutUserResponse putUserResponse = listener.get();
        assertTrue(putUserResponse.created());
    }

    private void updateUser(User user) throws ExecutionException, InterruptedException {
        final PutUserRequest putUserRequest = new PutUserRequest();
        putUserRequest.username(user.principal());
        putUserRequest.roles(user.roles());
        putUserRequest.metadata(user.metadata());
        putUserRequest.fullName(user.fullName());
        putUserRequest.email(user.email());
        final PlainActionFuture<PutUserResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
        );
        client.execute(PutUserAction.INSTANCE, putUserRequest, listener);
        final PutUserResponse putUserResponse = listener.get();
        assertFalse(putUserResponse.created());
    }

    private RoleDescriptor putRoleWithClusterPrivileges(final String nativeRealmRoleName, String... clusterPrivileges)
        throws InterruptedException, ExecutionException {
        final PutRoleRequest putRoleRequest = new PutRoleRequest();
        putRoleRequest.name(nativeRealmRoleName);
        putRoleRequest.cluster(clusterPrivileges);
        final PlainActionFuture<PutRoleResponse> roleListener = new PlainActionFuture<>();
        client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING)))
            .execute(PutRoleAction.INSTANCE, putRoleRequest, roleListener);
        assertNotNull(roleListener.get());
        return putRoleRequest.roleDescriptor();
    }

    private Client getClientForRunAsUser() {
        return client().filterWithHeader(
            Map.of(
                "Authorization",
                basicAuthHeaderValue("user_with_run_as_role", TEST_PASSWORD_SECURE_STRING),
                "es-security-runas-user",
                "user_with_manage_own_api_key_role"
            )
        );
    }

    private UpdateApiKeyResponse executeUpdateApiKey(final String username, final UpdateApiKeyRequest request) throws InterruptedException,
        ExecutionException {
        final var listener = new PlainActionFuture<UpdateApiKeyResponse>();
        final Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", basicAuthHeaderValue(username, TEST_PASSWORD_SECURE_STRING))
        );
        client.execute(UpdateApiKeyAction.INSTANCE, request, listener);
        return listener.get();
    }

    private void assertErrorMessage(final ElasticsearchSecurityException ese, String action, String userName, String apiKeyId) {
        assertThat(
            ese,
            throwableWithMessage(
                containsString("action [" + action + "] is unauthorized for API key id [" + apiKeyId + "] of user [" + userName + "]")
            )
        );
        assertThat(ese, throwableWithMessage(containsString(", this action is granted by the cluster privileges [")));
        assertThat(ese, throwableWithMessage(containsString("manage_api_key,manage_security,all]")));
    }

    private void assertErrorMessage(final ElasticsearchSecurityException ese, String action, String userName) {
        assertThat(ese, throwableWithMessage(containsString("action [" + action + "] is unauthorized for user [" + userName + "]")));
        assertThat(ese, throwableWithMessage(containsString(", this action is granted by the cluster privileges [")));
        assertThat(ese, throwableWithMessage(containsString("manage_api_key,manage_security,all]")));
    }
}
