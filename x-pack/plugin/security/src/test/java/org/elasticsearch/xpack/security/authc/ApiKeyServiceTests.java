/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyDoc;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyRoleDescriptors;
import org.elasticsearch.xpack.security.authc.ApiKeyService.CachedApiKeyHashResult;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApiKeyServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private XPackLicenseState licenseState;
    private Client client;
    private SecurityIndexManager securityIndex;

    @Before
    public void createThreadPool() {
        threadPool = Mockito.spy(
            new TestThreadPool("api key service tests",
                new FixedExecutorBuilder(Settings.EMPTY, SECURITY_CRYPTO_THREAD_POOL_NAME, 1, 1000,
                    "xpack.security.crypto.thread_pool", false))
        );
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    @Before
    public void setupMocks() {
        this.licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_API_KEY_SERVICE)).thenReturn(true);

        this.client = mock(Client.class);
        this.securityIndex = SecurityMocks.mockSecurityIndexManager();
    }

    public void testCreateApiKeyWillUseBulkAction() {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);
        final Authentication authentication = new Authentication(
            new User("alice", "superuser"),
            new RealmRef("file", "file", "node-1"),
            null);
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("key-1", null, null);
        when(client.prepareIndex(anyString())).thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.threadPool()).thenReturn(threadPool);
        service.createApiKey(authentication, createApiKeyRequest, Set.of(), new PlainActionFuture<>());
        verify(client).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any());
    }

    public void testGetCredentialsFromThreadContext() {
        ThreadContext threadContext = threadPool.getThreadContext();
        assertNull(ApiKeyService.getCredentialsFromHeader(threadContext));

        final String apiKeyAuthScheme = randomFrom("apikey", "apiKey", "ApiKey", "APikey", "APIKEY");
        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);
        String headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = ApiKeyService.getCredentialsFromHeader(threadContext);
            assertNotNull(creds);
            assertEquals(id, creds.getId());
            assertEquals(key, creds.getKey().toString());
        }

        // missing space
        headerValue = apiKeyAuthScheme + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = ApiKeyService.getCredentialsFromHeader(threadContext);
            assertNull(creds);
        }

        // missing colon
        headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> ApiKeyService.getCredentialsFromHeader(threadContext));
            assertEquals("invalid ApiKey value", e.getMessage());
        }
    }

    public void testAuthenticateWithApiKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        final User user;
        if (randomBoolean()) {
            user = new User("hulk", new String[] { "superuser" }, new User("authenticated_user", new String[] { "other" }));
        } else {
            user = new User("hulk", new String[] { "superuser" });
        }
        mockKeyDocument(service, id, key, user);

        final AuthenticationResult auth = tryAuthenticate(service, id, key);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(auth.getUser(), notNullValue());
        assertThat(auth.getUser().principal(), is("hulk"));
        assertThat(auth.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME), is("realm1"));
        assertThat(auth.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_TYPE), is("native"));
        assertThat(auth.getMetadata().get(ApiKeyService.API_KEY_ID_KEY), is(id));
        assertThat(auth.getMetadata().get(ApiKeyService.API_KEY_NAME_KEY), is("test"));
    }

    public void testAuthenticationIsSkippedIfLicenseDoesNotAllowIt() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        final User user;
        if (randomBoolean()) {
            user = new User(randomAlphaOfLength(6), new String[] { randomAlphaOfLength(12) }, new User("authenticated_user",
                    new String[] { "other" }));
        } else {
            user = new User(randomAlphaOfLength(6), new String[] { randomAlphaOfLength(12) });
        }
        mockKeyDocument(service, id, key, user);

        when(licenseState.checkFeature(Feature.SECURITY_API_KEY_SERVICE)).thenReturn(false);
        final AuthenticationResult auth = tryAuthenticate(service, id, key);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getUser(), nullValue());
    }

    public void testAuthenticationFailureWithInvalidatedApiKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        mockKeyDocument(service, id, key, new User("hulk", "superuser"), true, Duration.ofSeconds(3600));

        final AuthenticationResult auth = tryAuthenticate(service, id, key);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getUser(), nullValue());
        assertThat(auth.getMessage(), containsString("invalidated"));
    }

    public void testAuthenticationFailureWithInvalidCredentials() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String realKey = randomAlphaOfLength(16);
        final String wrongKey = "#" + realKey.substring(1);

        final User user;
        if (randomBoolean()) {
            user = new User("hulk", new String[] { "superuser" }, new User("authenticated_user", new String[] { "other" }));
        } else {
            user = new User("hulk", new String[] { "superuser" });
        }
        mockKeyDocument(service, id, realKey, user);

        final AuthenticationResult auth = tryAuthenticate(service, id, wrongKey);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getUser(), nullValue());
        assertThat(auth.getMessage(), containsString("invalid credentials"));
    }

    public void testAuthenticationFailureWithExpiredKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        mockKeyDocument(service, id, key, new User("hulk", "superuser"), false, Duration.ofSeconds(-1));

        final AuthenticationResult auth = tryAuthenticate(service, id, key);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getUser(), nullValue());
        assertThat(auth.getMessage(), containsString("expired"));
    }

    /**
     * We cache valid and invalid responses. This test verifies that we handle these correctly.
     */
    public void testMixingValidAndInvalidCredentials() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String realKey = randomAlphaOfLength(16);

        final User user;
        if (randomBoolean()) {
            user = new User("hulk", new String[] { "superuser" }, new User("authenticated_user", new String[] { "other" }));
        } else {
            user = new User("hulk", new String[] { "superuser" });
        }
        mockKeyDocument(service, id, realKey, user);

        for (int i = 0; i < 3; i++) {
            final String wrongKey = "=" + randomAlphaOfLength(14) + "@";
            AuthenticationResult auth = tryAuthenticate(service, id, wrongKey);
            assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
            assertThat(auth.getUser(), nullValue());
            assertThat(auth.getMessage(), containsString("invalid credentials"));

            auth = tryAuthenticate(service, id, realKey);
            assertThat(auth.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(auth.getUser(), notNullValue());
            assertThat(auth.getUser().principal(), is("hulk"));
        }
    }

    private void mockKeyDocument(ApiKeyService service, String id, String key, User user) throws IOException {
        mockKeyDocument(service, id, key, user, false, Duration.ofSeconds(3600));
    }

    private void mockKeyDocument(ApiKeyService service, String id, String key, User user, boolean invalidated,
                                 Duration expiry) throws IOException {
        final Authentication authentication;
        if (user.isRunAs()) {
            authentication = new Authentication(user, new RealmRef("authRealm", "test", "foo"),
                    new RealmRef("realm1", "native", "node01"), Version.CURRENT,
                    randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN, AuthenticationType.INTERNAL,
                            AuthenticationType.ANONYMOUS), Collections.emptyMap());
        } else {
            authentication = new Authentication(user, new RealmRef("realm1", "native", "node01"), null,
                    Version.CURRENT, randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN, AuthenticationType.INTERNAL,
                            AuthenticationType.ANONYMOUS), Collections.emptyMap());
        }
        XContentBuilder docSource = service.newDocument(new SecureString(key.toCharArray()), "test", authentication,
            Collections.singleton(SUPERUSER_ROLE_DESCRIPTOR), Instant.now(), Instant.now().plus(expiry), null,
            Version.CURRENT);
        if (invalidated) {
            Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(docSource), true, XContentType.JSON).v2();
            map.put("api_key_invalidated", true);
            docSource = XContentBuilder.builder(XContentType.JSON.xContent()).map(map);
        }
        SecurityMocks.mockGetRequest(client, id, BytesReference.bytes(docSource));
    }

    private AuthenticationResult tryAuthenticate(ApiKeyService service, String id, String key) throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final String header = "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
            threadContext.putHeader("Authorization", header);

            final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
            service.authenticateWithApiKeyIfPresent(threadContext, future);

            final AuthenticationResult auth = future.get();
            assertThat(auth, notNullValue());
            return auth;
        }
    }

    public void testValidateApiKey() throws Exception {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        ApiKeyService.ApiKeyCredentials creds =
            new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), is(emptyArray()));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(apiKeyDoc.roleDescriptorsBytes));
        assertThat(result.getMetadata().get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(apiKeyDoc.limitedByRoleDescriptorsBytes));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME), is("realm1"));

        apiKeyDoc = buildApiKeyDoc(hash, Clock.systemUTC().instant().plus(1L, ChronoUnit.HOURS).toEpochMilli(), false);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), is(emptyArray()));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(apiKeyDoc.roleDescriptorsBytes));
        assertThat(result.getMetadata().get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(apiKeyDoc.limitedByRoleDescriptorsBytes));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME), is("realm1"));

        apiKeyDoc = buildApiKeyDoc(hash, Clock.systemUTC().instant().minus(1L, ChronoUnit.HOURS).toEpochMilli(), false);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        apiKeyDoc = buildApiKeyDoc(hash, -1, true);
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());
    }

    public void testGetRolesForApiKeyNotInContext() throws Exception {
        Map<String, Object> superUserRdMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            superUserRdMap = XContentHelper.convertToMap(XContentType.JSON.xContent(),
                BytesReference.bytes(SUPERUSER_ROLE_DESCRIPTOR
                    .toXContent(builder, ToXContent.EMPTY_PARAMS, true))
                    .streamInput(),
                false);
        }
        Map<String, Object> authMetadata = new HashMap<>();
        authMetadata.put(ApiKeyService.API_KEY_ID_KEY, randomAlphaOfLength(12));
        authMetadata.put(API_KEY_ROLE_DESCRIPTORS_KEY,
            Collections.singletonMap(SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));
        authMetadata.put(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            Collections.singletonMap(SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_8_1),
            AuthenticationType.API_KEY, authMetadata);
        ApiKeyService service = createApiKeyService(Settings.EMPTY);

        PlainActionFuture<ApiKeyRoleDescriptors> roleFuture = new PlainActionFuture<>();
        service.getRoleForApiKey(authentication, roleFuture);
        ApiKeyRoleDescriptors result = roleFuture.get();
        assertThat(result.getRoleDescriptors().size(), is(1));
        assertThat(result.getRoleDescriptors().get(0).getName(), is("superuser"));
    }

    public void testGetRolesForApiKey() throws Exception {
        Map<String, Object> authMetadata = new HashMap<>();
        authMetadata.put(ApiKeyService.API_KEY_ID_KEY, randomAlphaOfLength(12));
        boolean emptyApiKeyRoleDescriptor = randomBoolean();
        final RoleDescriptor roleARoleDescriptor = new RoleDescriptor("a role", new String[] { "monitor" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("monitor").build() },
            null);
        Map<String, Object> roleARDMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            roleARDMap = XContentHelper.convertToMap(XContentType.JSON.xContent(),
                BytesReference.bytes(roleARoleDescriptor.toXContent(builder, ToXContent.EMPTY_PARAMS, true)).streamInput(), false);
        }
        authMetadata.put(API_KEY_ROLE_DESCRIPTORS_KEY,
            (emptyApiKeyRoleDescriptor) ? randomFrom(Arrays.asList(null, Collections.emptyMap()))
                : Collections.singletonMap("a role", roleARDMap));

        final RoleDescriptor limitedRoleDescriptor = new RoleDescriptor("limited role", new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build() },
            null);
        Map<String, Object> limitedRdMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            limitedRdMap = XContentHelper.convertToMap(XContentType.JSON.xContent(),
                BytesReference.bytes(limitedRoleDescriptor
                    .toXContent(builder, ToXContent.EMPTY_PARAMS, true))
                    .streamInput(),
                false);
        }
        authMetadata.put(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, Collections.singletonMap("limited role", limitedRdMap));

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_8_1),
            AuthenticationType.API_KEY, authMetadata);

        final NativePrivilegeStore privilegesStore = mock(NativePrivilegeStore.class);
        doAnswer(i -> {
                assertThat(i.getArguments().length, equalTo(3));
                final Object arg2 = i.getArguments()[2];
                assertThat(arg2, instanceOf(ActionListener.class));
                ActionListener<Collection<ApplicationPrivilege>> listener = (ActionListener<Collection<ApplicationPrivilege>>) arg2;
                listener.onResponse(Collections.emptyList());
                return null;
            }
        ).when(privilegesStore).getPrivileges(any(Collection.class), any(Collection.class), any(ActionListener.class));
        ApiKeyService service = createApiKeyService(Settings.EMPTY);

        PlainActionFuture<ApiKeyRoleDescriptors> roleFuture = new PlainActionFuture<>();
        service.getRoleForApiKey(authentication, roleFuture);
        ApiKeyRoleDescriptors result = roleFuture.get();
        if (emptyApiKeyRoleDescriptor) {
            assertNull(result.getLimitedByRoleDescriptors());
            assertThat(result.getRoleDescriptors().size(), is(1));
            assertThat(result.getRoleDescriptors().get(0).getName(), is("limited role"));
        } else {
            assertThat(result.getRoleDescriptors().size(), is(1));
            assertThat(result.getLimitedByRoleDescriptors().size(), is(1));
            assertThat(result.getRoleDescriptors().get(0).getName(), is("a role"));
            assertThat(result.getLimitedByRoleDescriptors().get(0).getName(), is("limited role"));
        }
    }

    public void testGetApiKeyIdAndRoleBytes() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(ApiKeyService.API_KEY_ID_KEY, apiKeyId);
        final BytesReference roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        final BytesReference limitedByRoleBytes = new BytesArray("{\"limitedBy role\": {\"cluster\": [\"all\"]}}");
        authMetadata.put(API_KEY_ROLE_DESCRIPTORS_KEY, roleBytes);
        authMetadata.put(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleBytes);

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            Version.CURRENT, AuthenticationType.API_KEY, authMetadata);
        ApiKeyService service = createApiKeyService(Settings.EMPTY);

        Tuple<String, BytesReference> apiKeyIdAndRoleBytes = service.getApiKeyIdAndRoleBytes(authentication, false);
        assertEquals(apiKeyId, apiKeyIdAndRoleBytes.v1());
        assertEquals(roleBytes, apiKeyIdAndRoleBytes.v2());
        apiKeyIdAndRoleBytes = service.getApiKeyIdAndRoleBytes(authentication, true);
        assertEquals(apiKeyId, apiKeyIdAndRoleBytes.v1());
        assertEquals(limitedByRoleBytes, apiKeyIdAndRoleBytes.v2());
    }

    public void testParseRoleDescriptors() {
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final String apiKeyId = randomAlphaOfLength(12);
        List<RoleDescriptor> roleDescriptors = service.parseRoleDescriptors(apiKeyId, null);
        assertTrue(roleDescriptors.isEmpty());

        BytesReference roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        roleDescriptors = service.parseRoleDescriptors(apiKeyId, roleBytes);
        assertEquals(1, roleDescriptors.size());
        assertEquals("a role", roleDescriptors.get(0).getName());
        assertArrayEquals(new String[] { "all" }, roleDescriptors.get(0).getClusterPrivileges());
        assertEquals(0, roleDescriptors.get(0).getIndicesPrivileges().length);
        assertEquals(0, roleDescriptors.get(0).getApplicationPrivileges().length);

        roleBytes = new BytesArray(
            "{\"reporting_user\":{\"cluster\":[],\"indices\":[],\"applications\":[],\"run_as\":[],\"metadata\":{\"_reserved\":true}," +
                "\"transient_metadata\":{\"enabled\":true}},\"superuser\":{\"cluster\":[\"all\"],\"indices\":[{\"names\":[\"*\"]," +
                "\"privileges\":[\"all\"],\"allow_restricted_indices\":true}],\"applications\":[{\"application\":\"*\"," +
                "\"privileges\":[\"*\"],\"resources\":[\"*\"]}],\"run_as\":[\"*\"],\"metadata\":{\"_reserved\":true}," +
                "\"transient_metadata\":{}}}\n");
        roleDescriptors = service.parseRoleDescriptors(apiKeyId, roleBytes);
        assertEquals(2, roleDescriptors.size());
        assertEquals(
            Set.of("reporting_user", "superuser"),
            roleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toSet()));
    }

    public void testApiKeyServiceDisabled() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), false).build();
        final ApiKeyService service = createApiKeyService(settings);

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> service.getApiKeys(randomAlphaOfLength(6), randomAlphaOfLength(8), null, null, new PlainActionFuture<>()));

        assertThat(e, instanceOf(FeatureNotEnabledException.class));
        // Older Kibana version looked for this exact text:
        assertThat(e, throwableWithMessage("api keys are not enabled"));
        // Newer Kibana versions will check the metadata for this string literal:
        assertThat(e.getMetadata(FeatureNotEnabledException.DISABLED_FEATURE_METADATA), contains("api_keys"));
    }

    public void testApiKeyCache() {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(true));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        final CachedApiKeyHashResult shouldBeSame = service.getFromCache(creds.getId());
        assertNotNull(shouldBeSame);
        assertThat(shouldBeSame, sameInstance(cachedApiKeyHashResult));

        apiKeyDoc = buildApiKeyDoc(hasher.hash(new SecureString("foobar".toCharArray())), -1, false);
        creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString("foobar1".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(false));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar2".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(false));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(true));
    }

    public void testAuthenticateWhileCacheBeingPopulated() throws Exception {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);

        ApiKeyService realService = createApiKeyService(Settings.EMPTY);
        ApiKeyService service  = Mockito.spy(realService);

        // Used to block the hashing of the first api-key secret so that we can guarantee
        // that a second api key authentication takes place while hashing is "in progress".
        final Semaphore hashWait = new Semaphore(0);
        final AtomicInteger hashCounter = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            hashCounter.incrementAndGet();
            hashWait.acquire();
            return invocationOnMock.callRealMethod();
        }).when(service).verifyKeyAgainstHash(any(String.class), any(ApiKeyCredentials.class), any(ActionListener.class));

        final ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        final PlainActionFuture<AuthenticationResult> future1 = new PlainActionFuture<>();

        // Call the top level authenticate... method because it has been known to be buggy in async situations
        writeCredentialsToThreadContext(creds);
        mockSourceDocument(creds.getId(), sourceMap);

        // This needs to be done in another thread, because we need it to not complete until we say so, but it should not block this test
        this.threadPool.generic().execute(() -> service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future1));

        // Wait for the first credential validation to get to the blocked state
        assertBusy(() -> assertThat(hashCounter.get(), equalTo(1)));
        if (future1.isDone()) {
            // We do this [ rather than assertFalse(isDone) ] so we can get a reasonable failure message
            fail("Expected authentication to be blocked, but was " + future1.actionGet());
        }

        // The second authentication should pass (but not immediately, but will not block)
        PlainActionFuture<AuthenticationResult> future2 = new PlainActionFuture<>();

        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future2);

        assertThat(hashCounter.get(), equalTo(1));
        if (future2.isDone()) {
            // We do this [ rather than assertFalse(isDone) ] so we can get a reasonable failure message
            fail("Expected authentication to be blocked, but was " + future2.actionGet());
        }

        hashWait.release();

        assertThat(future1.actionGet(TimeValue.timeValueSeconds(2)).isAuthenticated(), is(true));
        assertThat(future2.actionGet(TimeValue.timeValueMillis(100)).isAuthenticated(), is(true));

        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(true));
    }

    public void testApiKeyCacheDisabled() {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        final Settings settings = Settings.builder()
            .put(ApiKeyService.CACHE_TTL_SETTING.getKey(), "0s")
            .build();

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(settings);
        ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNull(cachedApiKeyHashResult);
    }

    public void testWillGetLookedUpByRealmNameIfExists() {
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("auth_by", "auth_by_type", "node");
        final Authentication.RealmRef lookedUpBy = new Authentication.RealmRef("looked_up_by", "looked_up_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, lookedUpBy);
        assertEquals("looked_up_by", ApiKeyService.getCreatorRealmName(authentication));
    }

    public void testWillGetLookedUpByRealmTypeIfExists() {
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("auth_by", "auth_by_type", "node");
        final Authentication.RealmRef lookedUpBy = new Authentication.RealmRef("looked_up_by", "looked_up_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, lookedUpBy);
        assertEquals("looked_up_by_type", ApiKeyService.getCreatorRealmType(authentication));
    }

    public void testAuthWillTerminateIfGetThreadPoolIsSaturated() throws ExecutionException, InterruptedException {
        final String apiKey = randomAlphaOfLength(16);
        final ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        writeCredentialsToThreadContext(creds);
        SecurityMocks.mockGetRequestException(client, new EsRejectedExecutionException("rejected"));
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future);
        final AuthenticationResult authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult.getStatus());
        assertThat(authenticationResult.getMessage(), containsString("server is too busy to respond"));
    }

    public void testAuthWillTerminateIfHashingThreadPoolIsSaturated() throws IOException, ExecutionException, InterruptedException {
        final String apiKey = randomAlphaOfLength(16);
        final ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        writeCredentialsToThreadContext(creds);

        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);
        mockSourceDocument(creds.getId(), sourceMap);
        final ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME)).thenReturn(mockExecutorService);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.onRejection(new EsRejectedExecutionException("rejected"));
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future);
        final AuthenticationResult authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult.getStatus());
        assertThat(authenticationResult.getMessage(), containsString("server is too busy to respond"));
    }

    public void testCachedApiKeyValidationWillNotBeBlockedByUnCachedApiKey() throws IOException, ExecutionException, InterruptedException {
        final String apiKey1 = randomAlphaOfLength(16);
        final ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey1.toCharArray()));
        writeCredentialsToThreadContext(creds);

        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey1.toCharArray()));
        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);
        mockSourceDocument(creds.getId(), sourceMap);

        // Authenticate the key once to cache it
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future);
        final AuthenticationResult authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.SUCCESS, authenticationResult.getStatus());

        // Now force the hashing thread pool to saturate so that any un-cached keys cannot be validated
        final ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME)).thenReturn(mockExecutorService);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.onRejection(new EsRejectedExecutionException("rejected"));
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        // A new API key trying to connect that must go through full hash computation
        final String apiKey2 = randomAlphaOfLength(16);
        final ApiKeyCredentials creds2 = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey2.toCharArray()));
        mockSourceDocument(creds2.getId(), buildApiKeySourceDoc(hasher.hash(new SecureString(apiKey2.toCharArray()))));
        final PlainActionFuture<AuthenticationResult> future2 = new PlainActionFuture<>();
        final ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext();
        writeCredentialsToThreadContext(creds2);
        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future2);
        final AuthenticationResult authenticationResult2 = future2.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult2.getStatus());
        assertThat(authenticationResult2.getMessage(), containsString("server is too busy to respond"));

        // The cached API key should not be affected
        mockSourceDocument(creds.getId(), sourceMap);
        final PlainActionFuture<AuthenticationResult> future3 = new PlainActionFuture<>();
        storedContext.restore();
        service.authenticateWithApiKeyIfPresent(threadPool.getThreadContext(), future3);
        final AuthenticationResult authenticationResult3 = future3.get();
        assertEquals(AuthenticationResult.Status.SUCCESS, authenticationResult3.getStatus());
    }

    public void testApiKeyDocDeserialization() throws IOException {
        final String apiKeyDocumentSource =
            "{\"doc_type\":\"api_key\",\"creation_time\":1591919944598,\"expiration_time\":1591919944599,\"api_key_invalidated\":false," +
                "\"api_key_hash\":\"{PBKDF2}10000$abc\",\"role_descriptors\":{\"a\":{\"cluster\":[\"all\"]}}," +
                "\"limited_by_role_descriptors\":{\"limited_by\":{\"cluster\":[\"all\"]," +
                "\"metadata\":{\"_reserved\":true},\"type\":\"role\"}}," +
                "\"name\":\"key-1\",\"version\":7000099," +
                "\"creator\":{\"principal\":\"admin\",\"metadata\":{\"foo\":\"bar\"},\"realm\":\"file1\",\"realm_type\":\"file\"}}";
        final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            new BytesArray(apiKeyDocumentSource),
            XContentType.JSON));
        assertEquals("api_key", apiKeyDoc.docType);
        assertEquals(1591919944598L, apiKeyDoc.creationTime);
        assertEquals(1591919944599L, apiKeyDoc.expirationTime);
        assertFalse(apiKeyDoc.invalidated);
        assertEquals("{PBKDF2}10000$abc", apiKeyDoc.hash);
        assertEquals("key-1", apiKeyDoc.name);
        assertEquals(7000099, apiKeyDoc.version);
        assertEquals(new BytesArray("{\"a\":{\"cluster\":[\"all\"]}}"), apiKeyDoc.roleDescriptorsBytes);
        assertEquals(new BytesArray("{\"limited_by\":{\"cluster\":[\"all\"],\"metadata\":{\"_reserved\":true},\"type\":\"role\"}}"),
            apiKeyDoc.limitedByRoleDescriptorsBytes);

        final Map<String, Object> creator = apiKeyDoc.creator;
        assertEquals("admin", creator.get("principal"));
        assertEquals("file1", creator.get("realm"));
        assertEquals("file", creator.get("realm_type"));
        assertEquals("bar", ((Map<String, Object>)creator.get("metadata")).get("foo"));
    }

    public void testApiKeyDocDeserializationWithNullValues() throws IOException {
        final String apiKeyDocumentSource =
            "{\"doc_type\":\"api_key\",\"creation_time\":1591919944598,\"expiration_time\":null,\"api_key_invalidated\":false," +
                "\"api_key_hash\":\"{PBKDF2}10000$abc\",\"role_descriptors\":{}," +
                "\"limited_by_role_descriptors\":{\"limited_by\":{\"cluster\":[\"all\"]}}," +
                "\"name\":null,\"version\":7000099," +
                "\"creator\":{\"principal\":\"admin\",\"metadata\":{},\"realm\":\"file1\"}}";
        final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            new BytesArray(apiKeyDocumentSource),
            XContentType.JSON));
        assertEquals(-1L, apiKeyDoc.expirationTime);
        assertNull(apiKeyDoc.name);
        assertEquals(new BytesArray("{}"), apiKeyDoc.roleDescriptorsBytes);
    }

    public static class Utils {

        private static final AuthenticationContextSerializer authenticationContextSerializer = new AuthenticationContextSerializer();

        public static Authentication createApiKeyAuthentication(ApiKeyService apiKeyService,
                                                                Authentication authentication,
                                                                Set<RoleDescriptor> userRoles,
                                                                List<RoleDescriptor> keyRoles,
                                                                Version version) throws Exception {
            XContentBuilder keyDocSource = apiKeyService.newDocument(new SecureString("secret".toCharArray()), "test", authentication,
                    userRoles, Instant.now(), Instant.now().plus(Duration.ofSeconds(3600)), keyRoles, Version.CURRENT);
            final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(
                XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(keyDocSource), XContentType.JSON));
            PlainActionFuture<AuthenticationResult> authenticationResultFuture = PlainActionFuture.newFuture();
            apiKeyService.validateApiKeyExpiration(apiKeyDoc, new ApiKeyService.ApiKeyCredentials("id",
                            new SecureString("pass".toCharArray())),
                    Clock.systemUTC(), authenticationResultFuture);

            AuthenticationResult authenticationResult = authenticationResultFuture.get();
            if (randomBoolean()) {
                // maybe remove realm name to simulate old API Key authentication
                assert authenticationResult.getStatus() == AuthenticationResult.Status.SUCCESS;
                Map<String, Object> authenticationResultMetadata = new HashMap<>(authenticationResult.getMetadata());
                authenticationResultMetadata.remove(ApiKeyService.API_KEY_CREATOR_REALM_NAME);
                authenticationResult = AuthenticationResult.success(authenticationResult.getUser(), authenticationResultMetadata);
            }
            if (randomBoolean()) {
                // simulate authentication with nameless API Key, see https://github.com/elastic/elasticsearch/issues/59484
                assert authenticationResult.getStatus() == AuthenticationResult.Status.SUCCESS;
                Map<String, Object> authenticationResultMetadata = new HashMap<>(authenticationResult.getMetadata());
                authenticationResultMetadata.remove(ApiKeyService.API_KEY_NAME_KEY);
                authenticationResult = AuthenticationResult.success(authenticationResult.getUser(), authenticationResultMetadata);
            }

            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            authenticationContextSerializer.writeToContext(
                    apiKeyService.createApiKeyAuthentication(authenticationResult, "node01"), threadContext);
            final CompletableFuture<Authentication> authFuture = new CompletableFuture<>();
            securityContext.executeAfterRewritingAuthentication((c) -> {
                try {
                    authFuture.complete(authenticationContextSerializer.readFromContext(threadContext));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, version);
            return authFuture.get();
        }

        public static Authentication createApiKeyAuthentication(ApiKeyService apiKeyService,
                                                                Authentication authentication) throws Exception {
            return createApiKeyAuthentication(apiKeyService, authentication,
                    Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[]{"manage"}, null, null)),
                    null, Version.CURRENT);
        }
    }

    private ApiKeyService createApiKeyService(Settings baseSettings) {
        final Settings settings = Settings.builder()
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(baseSettings)
            .build();
        return new ApiKeyService(settings, Clock.systemUTC(), client, licenseState, securityIndex,
            ClusterServiceUtils.createClusterService(threadPool), threadPool);
    }

    private Map<String, Object> buildApiKeySourceDoc(char[] hash) {
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("doc_type", "api_key");
        sourceMap.put("creation_time", Clock.systemUTC().instant().toEpochMilli());
        sourceMap.put("expiration_time", -1);
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("name", randomAlphaOfLength(12));
        sourceMap.put("version", 0);
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", "all")));
        sourceMap.put("limited_by_role_descriptors", Collections.singletonMap("limited role", Collections.singletonMap("cluster", "all")));
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);
        return sourceMap;
    }

    private void writeCredentialsToThreadContext(ApiKeyCredentials creds) {
        final String credentialString = creds.getId() + ":" + creds.getKey();
        this.threadPool.getThreadContext().putHeader("Authorization",
            "ApiKey " + Base64.getEncoder().encodeToString(credentialString.getBytes(StandardCharsets.US_ASCII)));
    }

    private void mockSourceDocument(String id, Map<String, Object> sourceMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(sourceMap);
            SecurityMocks.mockGetRequest(client, id, BytesReference.bytes(builder));
        }
    }

    private ApiKeyDoc buildApiKeyDoc(char[] hash, long expirationTime, boolean invalidated) {
        return new ApiKeyDoc(
            "api_key",
            Clock.systemUTC().instant().toEpochMilli(),
            expirationTime,
            invalidated,
            new String(hash),
            randomAlphaOfLength(12),
            0,
            new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}"),
            new BytesArray("{\"limited role\": {\"cluster\": [\"all\"]}}"),
            Map.of("principal", "test_user", "realm", "realm1", "realm_type", "realm_type1", "metadata", Map.of())
        );
    }
}
