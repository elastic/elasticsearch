/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class CloudIamRealmTests extends ESTestCase {
    public void testAuthenticateSuccessWithMockClient() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CloudIamRealm realm = createRealm(settings, threadPool);
            String header = buildSignedHeader("AKID", "mock", "abc", Instant.now().truncatedTo(ChronoUnit.SECONDS), null);
            threadPool.getThreadContext().putHeader("X-ES-IAM-Signed", header);
            AuthenticationToken token = realm.token(threadPool.getThreadContext());
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            AuthenticationResult<User> result = future.actionGet();
            assertThat(result.isAuthenticated(), is(true));
            assertThat(result.getValue().principal(), is("acs:ram::000000000000:user/AKID"));
        }
    }

    public void testAuthenticateInvalidHeaderTerminates() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CloudIamRealm realm = createRealm(settings, threadPool);
            String json = """
                {
                  "Action": "GetCallerIdentity",
                  "Version": "2015-04-01",
                  "AccessKeyId": "AKID",
                  "SignatureMethod": "HMAC-SHA1",
                  "SignatureVersion": "1.0",
                  "SignatureNonce": "abc",
                  "Timestamp": "2025-01-01T00:00:00Z"
                }
                """;
            String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
            threadPool.getThreadContext().putHeader("X-ES-IAM-Signed", header);
            AuthenticationToken token = realm.token(threadPool.getThreadContext());
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            AuthenticationResult<User> result = future.actionGet();
            assertThat(result.getStatus(), is(AuthenticationResult.Status.TERMINATE));
        }
    }

    public void testTokenMissingHeaderReturnsNull() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CloudIamRealm realm = createRealm(settings, threadPool);
            AuthenticationToken token = realm.token(threadPool.getThreadContext());
            assertThat(token, is((AuthenticationToken) null));
        }
    }

    public void testRejectsTimestampOutsideSkew() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettingsBuilder().put(
                CloudIamRealmSettings.ALLOWED_SKEW.getConcreteSettingForNamespace("iam1").getKey(),
                "1s"
            ).build();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.USER);
            CloudIamRealm realm = createRealm(settings, threadPool, null, iamClient);
            String header = buildSignedHeader(
                "AKID",
                "mock",
                "nonce",
                Instant.now().minus(10, ChronoUnit.MINUTES),
                null
            );
            CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            AuthenticationResult<User> result = future.actionGet();
            assertThat(result.getStatus(), is(AuthenticationResult.Status.TERMINATE));
            assertThat(iamClient.calls(), is(0));
        }
    }

    public void testRejectsReplayNonce() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.USER);
            CloudIamRealm realm = createRealm(settings, threadPool, null, iamClient);
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            String header = buildSignedHeader("AKID", "mock", "nonce", now, null);
            CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
            PlainActionFuture<AuthenticationResult<User>> future1 = new PlainActionFuture<>();
            realm.authenticate(token, future1);
            AuthenticationResult<User> result1 = future1.actionGet();
            assertThat(result1.isAuthenticated(), is(true));

            PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
            realm.authenticate(token, future2);
            AuthenticationResult<User> result2 = future2.actionGet();
            assertThat(result2.getStatus(), is(AuthenticationResult.Status.TERMINATE));
            assertThat(iamClient.calls(), is(1));
        }
    }

    public void testCachesSuccessfulAuthentication() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.USER);
            CloudIamRealm realm = createRealm(settings, threadPool, null, iamClient);
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            CloudIamToken token1 = CloudIamToken.fromHeaders(buildSignedHeader("AKID", "mock", "nonce-1", now, null), 8192);
            CloudIamToken token2 = CloudIamToken.fromHeaders(buildSignedHeader("AKID", "mock", "nonce-2", now, null), 8192);
            PlainActionFuture<AuthenticationResult<User>> future1 = new PlainActionFuture<>();
            realm.authenticate(token1, future1);
            assertThat(future1.actionGet().isAuthenticated(), is(true));

            PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
            realm.authenticate(token2, future2);
            assertThat(future2.actionGet().isAuthenticated(), is(true));
            assertThat(iamClient.calls(), is(1));
        }
    }

    public void testCachesFailedAuthentication() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettingsBuilder().put(
                CloudIamRealmSettings.NONCE_TTL.getConcreteSettingForNamespace("iam1").getKey(),
                "0s"
            ).build();
            CountingIamClient iamClient = new CountingIamClient(false, IamPrincipal.PrincipalType.USER);
            CloudIamRealm realm = createRealm(settings, threadPool, null, iamClient);
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            CloudIamToken token1 = CloudIamToken.fromHeaders(buildSignedHeader("AKID", "mock", "nonce-1", now, null), 8192);
            CloudIamToken token2 = CloudIamToken.fromHeaders(buildSignedHeader("AKID", "mock", "nonce-2", now, null), 8192);

            PlainActionFuture<AuthenticationResult<User>> future1 = new PlainActionFuture<>();
            realm.authenticate(token1, future1);
            assertThat(future1.actionGet().getStatus(), is(AuthenticationResult.Status.TERMINATE));

            PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
            realm.authenticate(token2, future2);
            assertThat(future2.actionGet().getStatus(), is(AuthenticationResult.Status.TERMINATE));
            assertThat(iamClient.calls(), is(1));
        }
    }

    public void testRejectsAssumedRoleWhenDisabled() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettings();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.ASSUMED_ROLE);
            CloudIamRealm realm = createRealm(settings, threadPool, null, iamClient);
            CloudIamToken token = CloudIamToken.fromHeaders(
                buildSignedHeader("AKID", "mock", "nonce", Instant.now().truncatedTo(ChronoUnit.SECONDS), null),
                8192
            );
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.TERMINATE));
        }
    }

    public void testRoleMappingResolvesRoles() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettingsBuilder().put(
                CloudIamRealmSettings.ROLE_MAPPING_ENABLED.getConcreteSettingForNamespace("iam1").getKey(),
                true
            ).build();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.USER);
            UserRoleMapper mapper = new UserRoleMapper() {
                @Override
                public void resolveRoles(UserData user, org.elasticsearch.action.ActionListener<Set<String>> listener) {
                    listener.onResponse(Set.of("read_only"));
                }

                @Override
                public void clearRealmCacheOnChange(org.elasticsearch.xpack.core.security.authc.support.CachingRealm realm) {}
            };
            CloudIamRealm realm = createRealm(settings, threadPool, mapper, iamClient);
            CloudIamToken token = CloudIamToken.fromHeaders(
                buildSignedHeader("AKID", "mock", "nonce", Instant.now().truncatedTo(ChronoUnit.SECONDS), null),
                8192
            );
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            AuthenticationResult<User> result = future.actionGet();
            assertThat(result.isAuthenticated(), is(true));
            assertThat(result.getValue().roles(), org.hamcrest.Matchers.arrayContaining("read_only"));
        }
    }

    public void testRoleMappingEmptyRolesTerminates() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            Settings settings = baseSettingsBuilder().put(
                CloudIamRealmSettings.ROLE_MAPPING_ENABLED.getConcreteSettingForNamespace("iam1").getKey(),
                true
            ).build();
            CountingIamClient iamClient = new CountingIamClient(true, IamPrincipal.PrincipalType.USER);
            UserRoleMapper mapper = new UserRoleMapper() {
                @Override
                public void resolveRoles(UserData user, org.elasticsearch.action.ActionListener<Set<String>> listener) {
                    listener.onResponse(Set.of());
                }

                @Override
                public void clearRealmCacheOnChange(org.elasticsearch.xpack.core.security.authc.support.CachingRealm realm) {}
            };
            CloudIamRealm realm = createRealm(settings, threadPool, mapper, iamClient);
            CloudIamToken token = CloudIamToken.fromHeaders(
                buildSignedHeader("AKID", "mock", "nonce", Instant.now().truncatedTo(ChronoUnit.SECONDS), null),
                8192
            );
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.TERMINATE));
        }
    }

    private CloudIamRealm createRealm(Settings settings, TestThreadPool threadPool) {
        return createRealm(settings, threadPool, null, new MockIamClient(createConfig(settings, threadPool)));
    }

    private CloudIamRealm createRealm(Settings settings, TestThreadPool threadPool, UserRoleMapper mapper, IamClient client) {
        RealmConfig config = createConfig(settings, threadPool);
        return new CloudIamRealm(config, threadPool, mapper, client);
    }

    private RealmConfig createConfig(Settings settings, TestThreadPool threadPool) {
        Environment env = TestEnvironment.newEnvironment(settings);
        RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(CloudIamRealmSettings.TYPE, "iam1");
        return new RealmConfig(id, settings, env, threadPool.getThreadContext());
    }

    private Settings baseSettings() {
        return baseSettingsBuilder().build();
    }

    private Settings.Builder baseSettingsBuilder() {
        return Settings.builder()
            .put("path.home", createTempDir())
            .put(RealmSettings.realmSettingPrefix(CloudIamRealmSettings.TYPE) + "iam1.order", 0)
            .put(
                CloudIamRealmSettings.AUTH_MODE.getConcreteSettingForNamespace("iam1").getKey(),
                "mock"
            )
            .put(
                CloudIamRealmSettings.ROLE_MAPPING_ENABLED.getConcreteSettingForNamespace("iam1").getKey(),
                false
            )
            .put(
                CloudIamRealmSettings.MOCK_SIGNATURE.getConcreteSettingForNamespace("iam1").getKey(),
                "mock"
            )
            .putList(
                CloudIamRealmSettings.MOCK_ROLES.getConcreteSettingForNamespace("iam1").getKey(),
                "read_only"
            )
            .put(
                CloudIamRealmSettings.ALLOWED_SKEW.getConcreteSettingForNamespace("iam1").getKey(),
                "10m"
            );
    }

    private String buildSignedHeader(String accessKeyId, String signature, String nonce, Instant timestamp, String sessionToken) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
            .append("\"Action\":\"GetCallerIdentity\",")
            .append("\"Version\":\"2015-04-01\",")
            .append("\"AccessKeyId\":\"").append(accessKeyId).append("\",")
            .append("\"Signature\":\"").append(signature).append("\",")
            .append("\"SignatureMethod\":\"HMAC-SHA1\",")
            .append("\"SignatureVersion\":\"1.0\",")
            .append("\"SignatureNonce\":\"").append(nonce).append("\",")
            .append("\"Timestamp\":\"").append(timestamp.toString()).append("\"");
        if (sessionToken != null) {
            builder.append(",\"SecurityToken\":\"").append(sessionToken).append("\"");
        }
        builder.append("}");
        return Base64.getEncoder().encodeToString(builder.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static class CountingIamClient implements IamClient {
        private final AtomicInteger calls = new AtomicInteger();
        private final boolean succeed;
        private final IamPrincipal.PrincipalType type;

        CountingIamClient(boolean succeed, IamPrincipal.PrincipalType type) {
            this.succeed = succeed;
            this.type = type;
        }

        @Override
        public void verify(CloudIamToken token, org.elasticsearch.action.ActionListener<IamPrincipal> listener) {
            calls.incrementAndGet();
            if (succeed) {
                String arn = switch (type) {
                    case ASSUMED_ROLE -> "acs:ram::000000000000:assumed-role/test/session";
                    case ROLE -> "acs:ram::000000000000:role/test";
                    default -> "acs:ram::000000000000:user/test";
                };
                listener.onResponse(new IamPrincipal(arn, "000000000000", "user", type));
            } else {
                listener.onFailure(new IllegalStateException("iam failure"));
            }
        }

        int calls() {
            return calls.get();
        }
    }
}
