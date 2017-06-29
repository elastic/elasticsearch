/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.security.authc.IncomingRequest;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

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

        RealmConfig config = new RealmConfig("test_realm", settings, globalSettings, new ThreadContext(Settings.EMPTY));
        CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
                listener.onResponse(new User("username", new String[]{"r1", "r2", "r3"}));
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
        SecureString pass = new SecureString("pass");
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future, mock(IncomingRequest.class));
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(3));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future, mock(IncomingRequest.class));
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
        realm.authenticate(new UsernamePasswordToken("a", new SecureString("pass")), future, mock(IncomingRequest.class));
        User user = future.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(1));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user, not(sameInstance(lookedUp)));

        // authenticate a different user first
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", new SecureString("pass")), future, mock(IncomingRequest.class));
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

    public void testCacheChangePassword() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);

        String user = "testUser";
        SecureString pass1 = new SecureString("pass");
        SecureString pass2 = new SecureString("password");

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass1), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass1), future, mock(IncomingRequest.class));
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(1));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass2), future, mock(IncomingRequest.class));
        future.actionGet();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, pass2), future, mock(IncomingRequest.class));
        future.actionGet();

        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testCacheDisabledUser() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        realm.setUsersEnabled(false);

        String user = "testUser";
        SecureString password = new SecureString("password");

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future, mock(IncomingRequest.class));
        assertThat(future.actionGet().enabled(), equalTo(false));

        assertThat(realm.authInvocationCounter.intValue(), is(1));

        realm.setUsersEnabled(true);
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future, mock(IncomingRequest.class));
        future.actionGet();
        assertThat(future.actionGet().enabled(), equalTo(true));

        assertThat(realm.authInvocationCounter.intValue(), is(2));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future, mock(IncomingRequest.class));
        future.actionGet();
        assertThat(future.actionGet().enabled(), equalTo(true));

        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testCacheWithVeryLowTtlExpiresBetweenAuthenticateCalls() throws InterruptedException {
        TimeValue ttl = TimeValue.timeValueNanos(randomIntBetween(10, 100));
        Settings settings = Settings.builder()
                .put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING.getKey(), ttl)
                .build();
        RealmConfig config = new RealmConfig("test_cache_ttl", settings, globalSettings, new ThreadContext(Settings.EMPTY));
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(config);

        final UsernamePasswordToken authToken = new UsernamePasswordToken("the-user", new SecureString("the-password"));

        // authenticate
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        final User user1 = future.actionGet();
        assertThat(user1.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(realm.authInvocationCounter.intValue(), is(1));

        Thread.sleep(2);

        // authenticate
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        final User user2 = future.actionGet();
        assertThat(user2.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user2, not(sameInstance(user1)));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testReadsDoNotPreventCacheExpiry() throws InterruptedException {
        TimeValue ttl = TimeValue.timeValueMillis(250);
        Settings settings = Settings.builder()
                .put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING.getKey(), ttl)
                .build();
        RealmConfig config = new RealmConfig("test_cache_ttl", settings, globalSettings, new ThreadContext(Settings.EMPTY));
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(config);

        final UsernamePasswordToken authToken = new UsernamePasswordToken("the-user", new SecureString("the-password"));
        PlainActionFuture<User> future = new PlainActionFuture<>();

        // authenticate
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        final long start = System.currentTimeMillis();
        final User user1 = future.actionGet();
        assertThat(realm.authInvocationCounter.intValue(), is(1));

        // After 100 ms (from the original start time), authenticate (read from cache). We don't care about the result
        sleepUntil(start + 100);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        future.actionGet();

        // After 200 ms (from the original start time), authenticate (read from cache). We don't care about the result
        sleepUntil(start + 200);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        future.actionGet();

        // After 300 ms (from the original start time), authenticate again. The cache entry should have expired (despite the previous reads)
        sleepUntil(start + 300);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future, mock(IncomingRequest.class));
        final User user2 = future.actionGet();
        assertThat(user2, not(sameInstance(user1)));
        // Due to slow VMs etc, the cache might have expired more than once during the test, but we can accept that.
        // We have other tests that verify caching works - this test just checks that it expires even when there are repeated reads.
        assertThat(realm.authInvocationCounter.intValue(), greaterThan(1));
    }

    private void sleepUntil(long until) throws InterruptedException {
        final long sleep = until - System.currentTimeMillis();
        if (sleep > 0) {
            Thread.sleep(sleep);
        }
    }

    public void testAuthenticateContract() throws Exception {
        Realm realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future, mock(IncomingRequest.class));
        User user = future.actionGet();
        assertThat(user, nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future, mock(IncomingRequest.class));
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("whatever exception"));
    }

    public void testLookupContract() throws Exception {
        Realm realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        User user = future.actionGet();
        assertThat(user, nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("lookup exception"));
    }

    public void testCacheConcurrency() throws Exception {
        final String username = "username";
        final SecureString password = SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING;
        final SecureString randomPassword = new SecureString(randomAlphaOfLength(password.length()).toCharArray());

        final String passwordHash = new String(Hasher.BCRYPT.hash(password));
        RealmConfig config = new RealmConfig("test_realm", Settings.EMPTY, globalSettings, new ThreadContext(Settings.EMPTY));
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
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
                            }), mock(IncomingRequest.class));
                        }

                    } catch (InterruptedException e) {
                    }
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

        RealmConfig config = new RealmConfig("test_realm", Settings.EMPTY, globalSettings, new ThreadContext(Settings.EMPTY));
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
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

                    } catch (InterruptedException e) {
                    }
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
            super("failing", new RealmConfig("failing-test", settings, global, new ThreadContext(Settings.EMPTY)));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
            listener.onResponse(null);
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            listener.onResponse(null);
        }
    }

    static class ThrowingAuthenticationRealm extends CachingUsernamePasswordRealm {

        ThrowingAuthenticationRealm(Settings settings, Settings globalSettings) {
            super("throwing", new RealmConfig("throwing-test", settings, globalSettings, new ThreadContext(Settings.EMPTY)));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
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

        private boolean usersEnabled = true;

        AlwaysAuthenticateCachingRealm(Settings globalSettings) {
            this(new RealmConfig("always-test", Settings.EMPTY, globalSettings, new ThreadContext(Settings.EMPTY)));
        }

        AlwaysAuthenticateCachingRealm(RealmConfig config) {
            super("always", config);
        }

        void setUsersEnabled(boolean usersEnabled) {
            this.usersEnabled = usersEnabled;
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
            authInvocationCounter.incrementAndGet();
            final User user = new User(token.principal(), new String[]{"testRole1", "testRole2"}, null, null, emptyMap(), usersEnabled);
            listener.onResponse(user);
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            lookupInvocationCounter.incrementAndGet();
            listener.onResponse(new User(username, new String[]{"lookupRole1", "lookupRole2"}));
        }
    }

    static class LookupNotSupportedRealm extends CachingUsernamePasswordRealm {

        public final AtomicInteger authInvocationCounter = new AtomicInteger(0);
        public final AtomicInteger lookupInvocationCounter = new AtomicInteger(0);

        LookupNotSupportedRealm(Settings globalSettings) {
            super("lookup", new RealmConfig("lookup-notsupported-test", Settings.EMPTY, globalSettings, new ThreadContext(Settings.EMPTY)));
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener, IncomingRequest incomingRequest) {
            authInvocationCounter.incrementAndGet();
            listener.onResponse(new User(token.principal(), new String[]{"testRole1", "testRole2"}));
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            lookupInvocationCounter.incrementAndGet();
            listener.onFailure(new UnsupportedOperationException("don't call lookup if lookup isn't supported!!!"));
        }
    }
}
