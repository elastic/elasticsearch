/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ClearRealmsCacheTests extends SecurityIntegTestCase {

    private static String[] usernames;

    @BeforeClass
    public static void init() throws Exception {
        usernames = new String[randomIntBetween(5, 10)];
        for (int i = 0; i < usernames.length; i++) {
            usernames[i] = randomAlphaOfLength(6) + "_" + i;
        }
    }

    enum Scenario {

        EVICT_ALL() {

            @Override
            public void assertEviction(User prevUser, User newUser) {
                assertThat(prevUser, not(sameInstance(newUser)));
            }

            @Override
            public void executeRequest() throws Exception {
                executeTransportRequest(new ClearRealmCacheRequest());
            }
        },

        EVICT_SOME() {

            private final String[] evicted_usernames = randomSelection(usernames);
            {
                Arrays.sort(evicted_usernames);
            }

            @Override
            public void assertEviction(User prevUser, User newUser) {
                if (Arrays.stream(evicted_usernames).anyMatch(prevUser.principal()::equals)) {
                    assertThat(prevUser, not(sameInstance(newUser)));
                } else {
                    assertThat(prevUser, sameInstance(newUser));
                }
            }

            @Override
            public void executeRequest() throws Exception {
                executeTransportRequest(new ClearRealmCacheRequest().usernames(evicted_usernames));
            }
        },

        EVICT_ALL_HTTP() {

            @Override
            public void assertEviction(User prevUser, User newUser) {
                assertThat(prevUser, not(sameInstance(newUser)));
            }

            @Override
            public void executeRequest() throws Exception {
                executeHttpRequest("/_security/realm/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache",
                        Collections.<String, String>emptyMap());
            }
        },

        EVICT_SOME_HTTP() {

            private final String[] evicted_usernames = randomSelection(usernames);
            {
                Arrays.sort(evicted_usernames);
            }

            @Override
            public void assertEviction(User prevUser, User newUser) {
                if (Arrays.stream(evicted_usernames).anyMatch(prevUser.principal()::equals)) {
                    assertThat(prevUser, not(sameInstance(newUser)));
                } else {
                    assertThat(prevUser, sameInstance(newUser));
                }
            }

            @Override
            public void executeRequest() throws Exception {
                String path = "/_security/realm/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache";
                Map<String, String> params = Collections.singletonMap("usernames", String.join(",", evicted_usernames));
                executeHttpRequest(path, params);
            }
        };

        public abstract void assertEviction(User prevUser, User newUser);

        public abstract void executeRequest() throws Exception;

        static void executeTransportRequest(ClearRealmCacheRequest request) throws Exception {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            client().execute(ClearRealmCacheAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(ClearRealmCacheResponse response) {
                    assertThat(response.getNodes().size(), equalTo(internalCluster().getNodeNames().length));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    latch.countDown();
                }
            });

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("waiting for clear realms cache request too long");
            }

            if (error.get() != null) {
                fail("failed to clear realm caches" + error.get().getMessage());
            }
        }

        static void executeHttpRequest(String path, Map<String, String> params) throws Exception {
            Request request = new Request("POST", path);
            for (Map.Entry<String, String> param : params.entrySet()) {
                request.addParameter(param.getKey(), param.getValue());
            }
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                    new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertNotNull(response.getEntity());
            assertTrue(EntityUtils.toString(response.getEntity()).contains("cluster_name"));
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected String configRoles() {
        return SecuritySettingsSource.CONFIG_ROLE_ALLOW_ALL + "\n" +
                "r1:\n" +
                "  cluster: all\n";
    }

    @Override
    protected String configUsers() {
        StringBuilder builder = new StringBuilder(SecuritySettingsSource.CONFIG_STANDARD_USER);
        final String usersPasswdHashed =
            new String(getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        for (String username : usernames) {
            builder.append(username).append(":").append(usersPasswdHashed).append("\n");
        }
        return builder.toString();
    }

    @Override
    protected String configUsersRoles() {
        return SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES +
                "r1:" + Strings.arrayToCommaDelimitedString(usernames);
    }

    public void testEvictAll() throws Exception {
        testScenario(Scenario.EVICT_ALL);
    }

    public void testEvictSome() throws Exception {
        testScenario(Scenario.EVICT_SOME);
    }

    public void testEvictAllHttp() throws Exception {
        testScenario(Scenario.EVICT_ALL_HTTP);
    }

    public void testEvictSomeHttp() throws Exception {
        testScenario(Scenario.EVICT_SOME_HTTP);
    }

    private void testScenario(Scenario scenario) throws Exception {
        Map<String, UsernamePasswordToken> tokens = new HashMap<>();
        for (String user : usernames) {
            tokens.put(user, new UsernamePasswordToken(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        }

        List<Realm> realms = new ArrayList<>();
        for (Realms nodeRealms : internalCluster().getInstances(Realms.class)) {
            realms.add(nodeRealms.realm("file"));
        }

        // we authenticate each user on each of the realms to make sure they're all cached
        Map<String, Map<Realm, User>> users = new HashMap<>();
        for (Realm realm : realms) {
            for (String username : usernames) {
                PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
                realm.authenticate(tokens.get(username), future);
                User user = future.actionGet().getUser();
                assertThat(user, notNullValue());
                Map<Realm, User> realmToUser = users.get(username);
                if (realmToUser == null) {
                    realmToUser = new HashMap<>();
                    users.put(username, realmToUser);
                }
                realmToUser.put(realm, user);
            }
        }

        // all users should be cached now on all realms, lets verify

        for (String username : usernames) {
            for (Realm realm : realms) {
                PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
                realm.authenticate(tokens.get(username), future);
                User user = future.actionGet().getUser();
                assertThat(user, sameInstance(users.get(username).get(realm)));
            }
        }

        // now, lets run the scenario
        scenario.executeRequest();

        // now, user_a should have been evicted, but user_b should still be cached
        for (String username : usernames) {
            for (Realm realm : realms) {
                PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
                realm.authenticate(tokens.get(username), future);
                User user = future.actionGet().getUser();
                assertThat(user, notNullValue());
                scenario.assertEviction(users.get(username).get(realm), user);
            }
        }
    }

    // selects a random sub-set of the give values
    private static String[] randomSelection(String[] values) {
        List<String> list = new ArrayList<>();
        while (list.isEmpty()) {
            double base = randomDouble();
            for (String value : values) {
                if (randomDouble() < base) {
                    list.add(value);
                }
            }
        }
        return list.toArray(new String[list.size()]);
    }
}
