/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheRequest;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheResponse;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.client.ShieldClient;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = SUITE)
public class ClearRealmsCacheTests extends ShieldIntegrationTest {

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
            public ClearRealmCacheRequest createRequest() {
                return new ClearRealmCacheRequest();
            }

            @Override
            public void assertEviction(User prevUser, User newUser) {
                assertThat(prevUser, not(sameInstance(newUser)));
            }
        },

        EVICT_SOME() {

            private final String[] evicted_usernames = randomSelection(usernames);
            {
                Arrays.sort(evicted_usernames);
            }

            @Override
            public ClearRealmCacheRequest createRequest() {
                return new ClearRealmCacheRequest().usernames(evicted_usernames);
            }

            @Override
            public void assertEviction(User prevUser, User newUser) {
                if (Arrays.binarySearch(evicted_usernames, prevUser.principal()) >= 0) {
                    assertThat(prevUser, not(sameInstance(newUser)));
                } else {
                    assertThat(prevUser, sameInstance(newUser));
                }
            }
        };

        public abstract ClearRealmCacheRequest createRequest();

        public abstract void assertEviction(User prevUser, User newUser);
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
            builder.append(username).append(":{plain}passwd\n");
        }
        return builder.toString();
    }

    @Override
    protected String configUsersRoles() {
        return ShieldSettingsSource.CONFIG_STANDARD_USER_ROLES +
                "r1:" + Strings.arrayToCommaDelimitedString(usernames);
    }

    @Test
    public void testEvictAll() throws Exception {
        testScenario(Scenario.EVICT_ALL);
    }

    @Test
    public void testEvictSome() throws Exception {
        testScenario(Scenario.EVICT_SOME);
    }

    private void testScenario(Scenario scenario) throws Exception {

        Map<String, UsernamePasswordToken> tokens = new HashMap<>();
        for (String user : usernames) {
            tokens.put(user, new UsernamePasswordToken(user, SecuredStringTests.build("passwd")));
        }

        List<Realm> realms = new ArrayList<>();
        for (Realms nodeRealms : internalCluster().getInstances(Realms.class)) {
            realms.add(nodeRealms.realm("esusers"));
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

        ShieldClient client = new ShieldClient(client());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        client.authc().clearRealmCache(scenario.createRequest(), new ActionListener<ClearRealmCacheResponse>() {
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
            logger.error("Failed to clear realm caches", error.get());
            fail("failed to clear realm caches");
        }

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
