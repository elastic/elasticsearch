/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CachingUsernamePasswordRealmTests extends ESTestCase {

    private Settings globalSettings;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    public void testSettings() throws Exception {
        String hashAlgo = randomFrom("bcrypt", "bcrypt4", "bcrypt5", "bcrypt6", "bcrypt7", "bcrypt8", "bcrypt9",
                                     "sha1", "ssha256", "md5", "clear_text", "noop");
        int maxUsers = randomIntBetween(10, 100);
        TimeValue ttl = TimeValue.timeValueMinutes(randomIntBetween(10, 20));
        Settings settings = Settings.builder()
                .put(CachingUsernamePasswordRealm.CACHE_HASH_ALGO_SETTING.getKey(), hashAlgo)
                .put(CachingUsernamePasswordRealm.CACHE_MAX_USERS_SETTING.getKey(), maxUsers)
                .put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING.getKey(), ttl)
                .build();

        RealmConfig config = new RealmConfig("test_realm", settings, globalSettings);
        CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
                listener.onResponse(new User("username", new String[] { "r1", "r2", "r3" }));
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };

        assertThat(realm.hasher, sameInstance(Hasher.resolve(hashAlgo)));
    }

    public void testAuthCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        SecuredString pass = SecuredStringTests.build("pass");
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future);
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(3));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future);
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(3));
        assertThat(realm.lookupInvocationCounter.intValue(), is(0));
    }

    public void testLookupCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("a", future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("b", future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("c", future);
        future.actionGet();

        assertThat(realm.lookupInvocationCounter.intValue(), is(3));
        future = new PlainActionFuture<>();
        realm.lookupUser("a", future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("b", future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("c", future);
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(0));
        assertThat(realm.lookupInvocationCounter.intValue(), is(3));
    }

    public void testLookupAndAuthCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        // lookup first
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("a", future);
        User lookedUp = future.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(0));
        assertThat(lookedUp.roles(), arrayContaining("lookupRole1", "lookupRole2"));

        // now authenticate
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", SecuredStringTests.build("pass")), future);
        User user = future.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(1));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user, not(sameInstance(lookedUp)));

        // authenticate a different user first
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", SecuredStringTests.build("pass")), future);
        user = future.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        //now lookup b
        future = new PlainActionFuture<>();
        realm.lookupUser("b", future);
        lookedUp = future.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user, sameInstance(lookedUp));
    }

    public void testCacheChangePassword(){
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);

        String user = "testUser";
        SecuredString pass1 = SecuredStringTests.build("pass");
        SecuredString pass2 = SecuredStringTests.build("password");

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass1), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass1), future);
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(1));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass2), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass2), future);
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testAuthenticateContract() throws Exception {
        Realm realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", SecuredStringTests.build("pass")), future);
        User user = future.actionGet();
        assertThat(user , nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", SecuredStringTests.build("pass")), future);
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage() , containsString("whatever exception"));
    }

    public void testLookupContract() throws Exception {
        Realm realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        User user = future.actionGet();
        assertThat(user , nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage() , containsString("lookup exception"));
    }

    public void testCacheConcurrency() throws Exception {
        final String username = "username";
        final SecuredString password = new SecuredString("changeme".toCharArray());
        final SecuredString randomPassword = new SecuredString(randomAlphaOfLength(password.length()).toCharArray());

        final String passwordHash = new String(Hasher.BCRYPT.hash(password));
        RealmConfig config = new RealmConfig("test_realm", Settings.EMPTY, globalSettings);
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
                // do something slow
                if (BCrypt.checkpw(token.credentials(), passwordHash)) {
                    listener.onResponse(new User(username, new String[]{"r1", "r2", "r3"}));
                } else {
                    listener.onResponse(null);
                }
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(20, 100);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            final boolean invalidPassword = randomBoolean();
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                        for (int i = 0; i < numberOfIterations; i++) {
                            UsernamePasswordToken token = new UsernamePasswordToken(username, invalidPassword ? randomPassword : password);

                            realm.authenticate(token, ActionListener.wrap((user) -> {
                                if (invalidPassword && user != null) {
                                    throw new RuntimeException("invalid password led to an authenticated user: " + user.toString());
                                } else if (invalidPassword == false && user == null) {
                                    throw new RuntimeException("proper password led to a null user!");
                                }
                            }, (e) -> {
                                logger.error("caught exception", e);
                                fail("unexpected exception");
                            }));
                        }

                    } catch (InterruptedException e) {}
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testUserLookupConcurrency() throws Exception {
        final String username = "username";

        RealmConfig config = new RealmConfig("test_realm", Settings.EMPTY, globalSettings);
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("authenticate should not be called!"));
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onResponse(new User(username, new String[]{"r1", "r2", "r3"}));
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween(numberOfProcessors, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(10000, 100000);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                        for (int i = 0; i < numberOfIterations; i++) {
                            realm.lookupUser(username, ActionListener.wrap((user) -> {
                                if (user == null) {
                                    throw new RuntimeException("failed to lookup user");
                                }
                            }, (e) -> {
                                logger.error("caught exception", e);
                                fail("unexpected exception");
                            }));
                        }

                    } catch (InterruptedException e) {}
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    static class FailingAuthenticationRealm extends CachingUsernamePasswordRealm {

        FailingAuthenticationRealm(Settings settings, Settings global) {
            super("failing", new RealmConfig("failing-test", settings, global));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
            listener.onResponse(null);
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            listener.onResponse(null);
        }
    }

    static class ThrowingAuthenticationRealm extends CachingUsernamePasswordRealm {

        ThrowingAuthenticationRealm(Settings settings, Settings globalSettings) {
            super("throwing", new RealmConfig("throwing-test", settings, globalSettings));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
            listener.onFailure(new RuntimeException("whatever exception"));
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            listener.onFailure(new RuntimeException("lookup exception"));
        }
    }

    static class AlwaysAuthenticateCachingRealm extends CachingUsernamePasswordRealm {

        public final AtomicInteger authInvocationCounter = new AtomicInteger(0);
        public final AtomicInteger lookupInvocationCounter = new AtomicInteger(0);

        AlwaysAuthenticateCachingRealm(Settings globalSettings) {
            super("always", new RealmConfig("always-test", Settings.EMPTY, globalSettings));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
            authInvocationCounter.incrementAndGet();
            listener.onResponse(new User(token.principal(), new String[] { "testRole1", "testRole2" }));
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            lookupInvocationCounter.incrementAndGet();
            listener.onResponse(new User(username, new String[] { "lookupRole1", "lookupRole2" }));
        }
    }

    static class LookupNotSupportedRealm extends CachingUsernamePasswordRealm {

        public final AtomicInteger authInvocationCounter = new AtomicInteger(0);
        public final AtomicInteger lookupInvocationCounter = new AtomicInteger(0);

        LookupNotSupportedRealm(Settings globalSettings) {
            super("lookup", new RealmConfig("lookup-notsupported-test", Settings.EMPTY, globalSettings));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
            authInvocationCounter.incrementAndGet();
            listener.onResponse(new User(token.principal(), new String[] { "testRole1", "testRole2" }));
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            lookupInvocationCounter.incrementAndGet();
            listener.onFailure(new UnsupportedOperationException("don't call lookup if lookup isn't supported!!!"));
        }
    }
}
