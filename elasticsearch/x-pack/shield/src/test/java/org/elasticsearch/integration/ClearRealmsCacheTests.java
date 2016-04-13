/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.shield.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.client.SecurityClient;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 *
 */
public class ClearRealmsCacheTests extends ShieldIntegTestCase {
    private static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("passwd".toCharArray())));

    private static String[] usernames;

    @BeforeClass
    public static void init() throws Exception {
        usernames = new String[randomIntBetween(5, 10)];
        for (int i = 0; i < usernames.length; i++) {
            usernames[i] = randomAsciiOfLength(6) + "_" + i;
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
                if (Arrays.binarySearch(evicted_usernames, prevUser.principal()) >= 0) {
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
                executeHttpRequest("/_shield/realm/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache",
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
                if (Arrays.binarySearch(evicted_usernames, prevUser.principal()) >= 0) {
                    assertThat(prevUser, not(sameInstance(newUser)));
                } else {
                    assertThat(prevUser, sameInstance(newUser));
                }
            }

            @Override
            public void executeRequest() throws Exception {
                String path = "/_shield/realm/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache";
                Map<String, String> params = Collections.singletonMap("usernames", String.join(",", evicted_usernames));
                executeHttpRequest(path, params);
            }
        };

        public abstract void assertEviction(User prevUser, User newUser);

        public abstract void executeRequest() throws Exception;

        static void executeTransportRequest(ClearRealmCacheRequest request) throws Exception {
            SecurityClient securityClient = securityClient(client());

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            securityClient.clearRealmCache(request, new ActionListener<ClearRealmCacheResponse>() {
                @Override
                public void onResponse(ClearRealmCacheResponse response) {
                    assertThat(response.getNodes().length, equalTo(internalCluster().getNodeNames().length));
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    error.set(e);
                    latch.countDown();
                }
            });

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("waiting for clear realms cache request too long");
            }

            if (error.get() != null) {
                fail("failed to clear realm caches" + error.get().getMessage());
            }
        }

        static void executeHttpRequest(String path, Map<String, String> params) throws Exception {
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpRequestBuilder requestBuilder = new HttpRequestBuilder(client)
                        .httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class))
                        .method("POST")
                        .path(path);
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    requestBuilder.addParam(entry.getKey(), entry.getValue());
                }
                requestBuilder.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                                new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
                HttpResponse response = requestBuilder.execute();
                assertThat(response.hasBody(), is(true));
                String body = response.getBody();
                assertThat(body.contains("cluster_name"), is(true));
            }
        }
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }
    @Override
    public boolean sslTransportEnabled() {
        return false;
    }

    @Override
    protected String configRoles() {
        return ShieldSettingsSource.CONFIG_ROLE_ALLOW_ALL + "\n" +
                "r1:\n" +
                "  cluster: all\n";
    }

    @Override
    protected String configUsers() {
        StringBuilder builder = new StringBuilder(ShieldSettingsSource.CONFIG_STANDARD_USER);
        for (String username : usernames) {
            builder.append(username).append(":").append(USERS_PASSWD_HASHED).append("\n");
        }
        return builder.toString();
    }

    @Override
    protected String configUsersRoles() {
        return ShieldSettingsSource.CONFIG_STANDARD_USER_ROLES +
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
            tokens.put(user, new UsernamePasswordToken(user, SecuredStringTests.build("passwd")));
        }

        List<Realm> realms = new ArrayList<>();
        for (Realms nodeRealms : internalCluster().getInstances(Realms.class)) {
            realms.add(nodeRealms.realm("file"));
        }

        // we authenticate each user on each of the realms to make sure they're all cached
        Map<String, Map<Realm, User>> users = new HashMap<>();
        for (Realm realm : realms) {
            for (String username : usernames) {
                User user = realm.authenticate(tokens.get(username));
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
                assertThat(realm.authenticate(tokens.get(username)), sameInstance(users.get(username).get(realm)));
            }
        }

        // now, lets run the scenario
        scenario.executeRequest();

        // now, user_a should have been evicted, but user_b should still be cached
        for (String username : usernames) {
            for (Realm realm : realms) {
                User user = realm.authenticate(tokens.get(username));
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
