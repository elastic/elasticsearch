/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyRoleDescriptors;
import org.elasticsearch.xpack.security.authc.ApiKeyService.CachedApiKeyHashResult;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;

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
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private XPackLicenseState licenseState;
    private Client client;
    private SecurityIndexManager securityIndex;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool("api key service tests");
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    @Before
    public void setupMocks() {
        this.licenseState = mock(XPackLicenseState.class);
        when(licenseState.isApiKeyServiceAllowed()).thenReturn(true);

        this.client = mock(Client.class);
        this.securityIndex = SecurityMocks.mockSecurityIndexManager();
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

        mockKeyDocument(service, id, key, new User("hulk", "superuser"));

        final AuthenticationResult auth = tryAuthenticate(service, id, key);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(auth.getUser(), notNullValue());
        assertThat(auth.getUser().principal(), is("hulk"));
    }

    public void testAuthenticationIsSkippedIfLicenseDoesNotAllowIt() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        mockKeyDocument(service, id, key, new User(randomAlphaOfLength(6), randomAlphaOfLength(12)));

        when(licenseState.isApiKeyServiceAllowed()).thenReturn(false);
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

        mockKeyDocument(service, id, realKey, new User("hulk", "superuser"));

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

        mockKeyDocument(service, id, realKey, new User("hulk", "superuser"));

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
        final Authentication authentication = new Authentication(user, new RealmRef("realm1", "native",
            "node01"), null, Version.CURRENT);
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

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("doc_type", "api_key");
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", "all")));
        sourceMap.put("limited_by_role_descriptors", Collections.singletonMap("limited role", Collections.singletonMap("cluster", "all")));
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("realm", "realm1");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        ApiKeyService.ApiKeyCredentials creds =
            new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(sourceMap.get("role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(sourceMap.get("limited_by_role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM), is("realm1"));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().plus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(sourceMap.get("role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(sourceMap.get("limited_by_role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM), is("realm1"));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().minus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        sourceMap.remove("expiration_time");
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        sourceMap.put("api_key_invalidated", true);
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
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
        authMetadata.put(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY,
            Collections.singletonMap(SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));
        authMetadata.put(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            Collections.singletonMap(SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            Version.CURRENT, AuthenticationType.API_KEY, authMetadata);
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
        authMetadata.put(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY,
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
        authMetadata.put(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, Collections.singletonMap("limited role", limitedRdMap));

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            Version.CURRENT, AuthenticationType.API_KEY, authMetadata);

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

    public void testApiKeyCache() {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("doc_type", "api_key");
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", "all")));
        sourceMap.put("limited_by_role_descriptors", Collections.singletonMap("limited role", Collections.singletonMap("cluster", "all")));
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(true));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        final CachedApiKeyHashResult shouldBeSame = service.getFromCache(creds.getId());
        assertNotNull(shouldBeSame);
        assertThat(shouldBeSame, sameInstance(cachedApiKeyHashResult));

        sourceMap.put("api_key_hash", new String(hasher.hash(new SecureString("foobar".toCharArray()))));
        creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString("foobar1".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(false));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar2".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(false));

        creds = new ApiKeyCredentials(creds.getId(), new SecureString("foobar".toCharArray()));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(true));
    }

    public void testApiKeyCacheDisabled() {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        final Settings settings = Settings.builder()
            .put(ApiKeyService.CACHE_TTL_SETTING.getKey(), "0s")
            .build();

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("doc_type", "api_key");
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", "all")));
        sourceMap.put("limited_by_role_descriptors", Collections.singletonMap("limited role", Collections.singletonMap("cluster", "all")));
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);

        ApiKeyService service = createApiKeyService(settings);
        ApiKeyCredentials creds = new ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), sourceMap, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNull(cachedApiKeyHashResult);
    }

    private ApiKeyService createApiKeyService(Settings settings) {
        return new ApiKeyService(settings, Clock.systemUTC(), client, licenseState, securityIndex,
            ClusterServiceUtils.createClusterService(threadPool), threadPool);
    }


}
