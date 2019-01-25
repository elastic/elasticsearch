/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.LimitedRole;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ApiKeyServiceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool("api key service tests");
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testGetCredentialsFromThreadContext() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
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

    public void testValidateApiKey() throws Exception {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", "all")));
        sourceMap.put("limited_by_role_descriptors", Collections.singletonMap("limited role", Collections.singletonMap("cluster", "all")));
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);

        ApiKeyService.ApiKeyCredentials creds =
            new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(sourceMap.get("role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
                equalTo(sourceMap.get("limited_by_role_descriptors")));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().plus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(sourceMap.get("role_descriptors")));
        assertThat(result.getMetadata().get(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
                equalTo(sourceMap.get("limited_by_role_descriptors")));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().minus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        sourceMap.remove("expiration_time");
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());
        
        sourceMap.put("api_key_invalidated", true);
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());
    }

    public void testGetRolesForApiKeyNotInContext() throws Exception {
        Map<String, Object> superUserRdMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            superUserRdMap = XContentHelper.convertToMap(XContentType.JSON.xContent(),
                BytesReference.bytes(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR
                    .toXContent(builder, ToXContent.EMPTY_PARAMS, true))
                    .streamInput(),
                false);
        }
        Map<String, Object> authMetadata = new HashMap<>();
        authMetadata.put(ApiKeyService.API_KEY_ID_KEY, randomAlphaOfLength(12));
        authMetadata.put(ApiKeyService.API_KEY_ROLE_DESCRIPTORS_KEY,
            Collections.singletonMap(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));
        authMetadata.put(ApiKeyService.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                Collections.singletonMap(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap));

        final Authentication authentication = new Authentication(new User("joe"), new RealmRef("apikey", "apikey", "node"), null,
            Version.CURRENT, AuthenticationType.API_KEY, authMetadata);
        CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        doAnswer(invocationOnMock -> {
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[2];
            Collection<RoleDescriptor> descriptors = (Collection<RoleDescriptor>) invocationOnMock.getArguments()[0];
            if (descriptors.size() != 1) {
                listener.onFailure(new IllegalStateException("descriptors was empty!"));
            } else if (descriptors.iterator().next().getName().equals("superuser")) {
                listener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
            } else {
                listener.onFailure(new IllegalStateException("unexpected role name " + descriptors.iterator().next().getName()));
            }
            return Void.TYPE;
        }).when(rolesStore).buildAndCacheRoleFromDescriptors(any(Collection.class), any(String.class), any(ActionListener.class));
        ApiKeyService service = new ApiKeyService(Settings.EMPTY, Clock.systemUTC(), null, null,
                ClusterServiceUtils.createClusterService(threadPool), rolesStore);

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        service.getRoleForApiKey(authentication, rolesStore, roleFuture);
        Role role = roleFuture.get();
        assertThat(role.names(), arrayContaining("superuser"));
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

        CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        doAnswer(invocationOnMock -> {
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[2];
            Collection<RoleDescriptor> descriptors = (Collection<RoleDescriptor>) invocationOnMock.getArguments()[0];
            if (descriptors.size() != 1) {
                listener.onFailure(new IllegalStateException("descriptors was empty!"));
            } else if (descriptors.iterator().next().getName().equals("a role")) {
                CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY),
                        privilegesStore, ActionListener.wrap(r -> listener.onResponse(r), listener::onFailure));
            } else if (descriptors.iterator().next().getName().equals("limited role")) {
                CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY),
                        privilegesStore, ActionListener.wrap(r -> listener.onResponse(r), listener::onFailure));
            } else {
                listener.onFailure(new IllegalStateException("unexpected role name " + descriptors.iterator().next().getName()));
            }
            return Void.TYPE;
        }).when(rolesStore).buildAndCacheRoleFromDescriptors(any(Collection.class), any(String.class), any(ActionListener.class));
        ApiKeyService service = new ApiKeyService(Settings.EMPTY, Clock.systemUTC(), null, null,
                ClusterServiceUtils.createClusterService(threadPool), rolesStore);

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        service.getRoleForApiKey(authentication, rolesStore, roleFuture);
        Role role = roleFuture.get();
        if (emptyApiKeyRoleDescriptor) {
            assertThat(role, instanceOf(Role.class));
            assertThat(role.names(), arrayContaining("limited role"));
        } else {
            assertThat(role, instanceOf(LimitedRole.class));
            LimitedRole limitedRole = (LimitedRole) role;
            assertThat(limitedRole.names(), arrayContaining("a role"));
            assertThat(limitedRole.limitedBy(), is(notNullValue()));
            assertThat(limitedRole.limitedBy().names(), arrayContaining("limited role"));
        }
    }
}
