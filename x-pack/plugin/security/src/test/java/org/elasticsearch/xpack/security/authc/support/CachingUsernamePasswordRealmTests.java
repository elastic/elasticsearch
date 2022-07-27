/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CachingUsernamePasswordRealmTests extends ESTestCase {

    private Settings globalSettings;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool("caching username password realm tests");
    }

    @After
    public void stop() {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    public void testCacheSettings() {
        List<String> availableCacheAlgos = Hasher.getAvailableAlgoCacheHash();
        if (inFipsJvm()) {
            availableCacheAlgos = availableCacheAlgos.stream().filter(name -> (name.startsWith("pbkdf2"))).collect(Collectors.toList());
        }
        String cachingHashAlgo = randomFrom(availableCacheAlgos);
        int maxUsers = randomIntBetween(10, 100);
        TimeValue ttl = TimeValue.timeValueMinutes(randomIntBetween(10, 20));
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING), cachingHashAlgo)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING), maxUsers)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), ttl)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();

        RealmConfig config = new RealmConfig(
            identifier,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                listener.onResponse(AuthenticationResult.success(new User("username", new String[] { "r1", "r2", "r3" })));
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };
        assertThat(realm.cacheHasher, sameInstance(Hasher.resolve(cachingHashAlgo)));
    }

    public void testCacheSizeWhenCacheDisabled() {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        final Settings settings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();

        final RealmConfig config = new RealmConfig(
            identifier,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                listener.onResponse(AuthenticationResult.success(new User("username", new String[] { "r1", "r2", "r3" })));
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };
        assertThat(realm.getCacheSize(), equalTo(-1));
    }

    public void testAuthCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool);
        SecureString pass = new SecureString("pass");
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(1));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(2));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(3));

        assertThat(realm.authInvocationCounter.intValue(), is(3));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(3));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(3));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("c", pass), future);
        future.actionGet();
        assertThat(realm.getCacheSize(), equalTo(3));

        assertThat(realm.authInvocationCounter.intValue(), is(3));
        assertThat(realm.lookupInvocationCounter.intValue(), is(0));
    }

    public void testLookupCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool);
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
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool);
        // lookup first
        PlainActionFuture<User> lookupFuture = new PlainActionFuture<>();
        realm.lookupUser("a", lookupFuture);
        User lookedUp = lookupFuture.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(0));
        assertThat(lookedUp.roles(), arrayContaining("lookupRole1", "lookupRole2"));

        // now authenticate
        PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("a", new SecureString("pass")), authFuture);
        AuthenticationResult<User> authResult = authFuture.actionGet();
        assertThat(authResult.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = authResult.getValue();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(1));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user, not(sameInstance(lookedUp)));

        // authenticate a different user first
        authFuture = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("b", new SecureString("pass")), authFuture);
        authResult = authFuture.actionGet();
        assertThat(authResult.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        user = authResult.getValue();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        // now lookup b
        lookupFuture = new PlainActionFuture<>();
        realm.lookupUser("b", lookupFuture);
        lookedUp = lookupFuture.actionGet();
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user, sameInstance(lookedUp));
    }

    public void testCacheChangePassword() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool);

        String user = "testUser";
        SecureString pass1 = new SecureString("pass");
        SecureString pass2 = new SecureString("password");

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
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

    public void testCacheDisabledUser() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool);
        realm.setUsersEnabled(false);

        String user = "testUser";
        SecureString password = new SecureString("password");

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future);
        assertThat(future.actionGet().getValue().enabled(), equalTo(false));

        assertThat(realm.authInvocationCounter.intValue(), is(1));

        realm.setUsersEnabled(true);
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future);
        future.actionGet();
        assertThat(future.actionGet().getValue().enabled(), equalTo(true));

        assertThat(realm.authInvocationCounter.intValue(), is(2));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken(user, password), future);
        future.actionGet();
        assertThat(future.actionGet().getValue().enabled(), equalTo(true));

        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testCacheWithVeryLowTtlExpiresBetweenAuthenticateCalls() throws InterruptedException {
        TimeValue ttl = TimeValue.timeValueNanos(randomIntBetween(10, 100));
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("caching", "test_cache_ttl");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), ttl)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(
            identifier,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(config, threadPool);

        final UsernamePasswordToken authToken = new UsernamePasswordToken("the-user", new SecureString("the-password"));

        // authenticate
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(authToken, future);
        final User user1 = future.actionGet().getValue();
        assertThat(user1.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(realm.authInvocationCounter.intValue(), is(1));

        Thread.sleep(2);

        // authenticate
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future);
        final User user2 = future.actionGet().getValue();
        assertThat(user2.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user2, not(sameInstance(user1)));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    public void testReadsDoNotPreventCacheExpiry() throws InterruptedException {
        TimeValue ttl = TimeValue.timeValueMillis(250);
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("caching", "test_cache_ttl");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(identifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), ttl)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(
            identifier,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(config, threadPool);

        final UsernamePasswordToken authToken = new UsernamePasswordToken("the-user", new SecureString("the-password"));
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();

        // authenticate
        realm.authenticate(authToken, future);
        final long start = System.currentTimeMillis();
        final User user1 = future.actionGet().getValue();
        assertThat(realm.authInvocationCounter.intValue(), is(1));

        // After 100 ms (from the original start time), authenticate (read from cache). We don't care about the result
        sleepUntil(start + 100);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future);
        future.actionGet();

        // After 200 ms (from the original start time), authenticate (read from cache). We don't care about the result
        sleepUntil(start + 200);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future);
        future.actionGet();

        // After 300 ms (from the original start time), authenticate again. The cache entry should have expired (despite the previous reads)
        sleepUntil(start + 300);
        future = new PlainActionFuture<>();
        realm.authenticate(authToken, future);
        final User user2 = future.actionGet().getValue();
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

    public void testAuthenticateContract() {
        Realm realm = new FailingAuthenticationRealm(globalSettings, threadPool);
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future);
        User user = future.actionGet().getValue();
        assertThat(user, nullValue());

        realm = new ThrowingAuthenticationRealm(globalSettings, threadPool);
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future);
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("whatever exception"));
    }

    public void testLookupContract() {
        Realm realm = new FailingAuthenticationRealm(globalSettings, threadPool);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        User user = future.actionGet();
        assertThat(user, nullValue());

        realm = new ThrowingAuthenticationRealm(globalSettings, threadPool);
        future = new PlainActionFuture<>();
        realm.lookupUser("user", future);
        RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("lookup exception"));
    }

    public void testReturnDifferentObjectFromCache() {
        final AtomicReference<User> userArg = new AtomicReference<>();
        final AtomicReference<AuthenticationResult<User>> result = new AtomicReference<>();
        Realm realm = new AlwaysAuthenticateCachingRealm(globalSettings, threadPool) {
            @Override
            protected void handleCachedAuthentication(User user, ActionListener<AuthenticationResult<User>> listener) {
                userArg.set(user);
                listener.onResponse(result.get());
            }
        };
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future);
        final AuthenticationResult<User> result1 = future.actionGet();
        assertThat(result1, notNullValue());
        assertThat(result1.getValue(), notNullValue());
        assertThat(result1.getValue().principal(), equalTo("user"));

        final AuthenticationResult<User> result2 = AuthenticationResult.success(new User("user"));
        result.set(result2);

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user", new SecureString("pass")), future);
        final AuthenticationResult<User> result3 = future.actionGet();
        assertThat(result3, sameInstance(result2));
        assertThat(userArg.get(), sameInstance(result1.getValue()));
    }

    public void testSingleAuthPerUserLimit() throws Exception {
        final String username = "username";
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        final AtomicInteger authCounter = new AtomicInteger(0);
        final Hasher pwdHasher = getFastStoredHashAlgoForTests();
        final String passwordHash = new String(pwdHasher.hash(password));
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                authCounter.incrementAndGet();
                // do something slow
                if (pwdHasher.verify(token.credentials(), passwordHash.toCharArray())) {
                    listener.onResponse(AuthenticationResult.success(new User(username, new String[] { "r1", "r2", "r3" })));
                } else {
                    listener.onFailure(new IllegalStateException("password auth should never fail"));
                }
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(20, 100);
        final CountDownLatch latch = new CountDownLatch(1 + numberOfThreads);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    for (int i1 = 0; i1 < numberOfIterations; i1++) {
                        UsernamePasswordToken token = new UsernamePasswordToken(username, password);

                        realm.authenticate(token, ActionListener.wrap((result) -> {
                            if (result.isAuthenticated() == false) {
                                throw new IllegalStateException("proper password led to an unauthenticated result: " + result);
                            }
                        }, (e) -> {
                            logger.error("caught exception", e);
                            fail("unexpected exception - " + e);
                        }));
                    }

                } catch (InterruptedException e) {
                    logger.error("thread was interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(1, authCounter.get());
    }

    public void testUnauthenticatedResultPropagatesWithSameCreds() throws Exception {
        final String username = "username";
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        final AtomicInteger authCounter = new AtomicInteger(0);
        final Hasher pwdHasher = getFastStoredHashAlgoForTests();
        final String passwordHash = new String(pwdHasher.hash(password));
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        final SecureString credsToUse = new SecureString(randomAlphaOfLength(14).toCharArray());

        // we use a bunch of different latches here, the first `latch` is used to ensure all threads have been started
        // before they start to execute. The `authWaitLatch` is there to ensure we have all threads waiting on the
        // listener before we auth otherwise we may run into a race condition where we auth and one of the threads is
        // not waiting on auth yet. Finally, the completedLatch is used to signal that each thread received a response!
        final CountDownLatch latch = new CountDownLatch(1 + numberOfThreads);
        final CountDownLatch authWaitLatch = new CountDownLatch(numberOfThreads);
        final CountDownLatch completedLatch = new CountDownLatch(numberOfThreads);
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                authCounter.incrementAndGet();
                authWaitLatch.countDown();
                try {
                    authWaitLatch.await();
                } catch (InterruptedException e) {
                    logger.info("authentication was interrupted", e);
                    Thread.currentThread().interrupt();
                }
                // do something slow
                if (pwdHasher.verify(token.credentials(), passwordHash.toCharArray())) {
                    listener.onFailure(new IllegalStateException("password auth should never succeed"));
                } else {
                    listener.onResponse(AuthenticationResult.unsuccessful("password verification failed", null));
                }
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    final UsernamePasswordToken token = new UsernamePasswordToken(username, credsToUse);

                    realm.authenticate(token, ActionListener.wrap((result) -> {
                        if (result.isAuthenticated()) {
                            completedLatch.countDown();
                            throw new IllegalStateException("invalid password led to an authenticated result: " + result);
                        }
                        assertThat(result.getMessage(), containsString("password verification failed"));
                        completedLatch.countDown();
                    }, (e) -> {
                        logger.error("caught exception", e);
                        completedLatch.countDown();
                        fail("unexpected exception - " + e);
                    }));
                    authWaitLatch.countDown();
                } catch (InterruptedException e) {
                    logger.error("thread was interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        completedLatch.await();
        assertEquals(1, authCounter.get());
    }

    public void testCacheConcurrency() throws Exception {
        final String username = "username";
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        final SecureString randomPassword = new SecureString(randomAlphaOfLength(password.length()).toCharArray());
        final Hasher localHasher = getFastStoredHashAlgoForTests();
        final String passwordHash = new String(localHasher.hash(password));
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                // do something slow
                if (localHasher.verify(token.credentials(), passwordHash.toCharArray())) {
                    listener.onResponse(AuthenticationResult.success(new User(username, new String[] { "r1", "r2", "r3" })));
                } else {
                    listener.onResponse(AuthenticationResult.unsuccessful("Incorrect password", null));
                }
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new UnsupportedOperationException("this method should not be called"));
            }
        };

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(20, 100);
        final CountDownLatch latch = new CountDownLatch(1 + numberOfThreads);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final boolean invalidPassword = randomBoolean();
            final int threadNum = i;
            threads.add(new Thread(() -> {
                threadPool.getThreadContext().putTransient("key", threadNum);
                try {
                    latch.countDown();
                    latch.await();
                    for (int i1 = 0; i1 < numberOfIterations; i1++) {
                        UsernamePasswordToken token = new UsernamePasswordToken(username, invalidPassword ? randomPassword : password);

                        realm.authenticate(token, ActionListener.wrap((result) -> {
                            assertThat(threadPool.getThreadContext().getTransient("key"), is(threadNum));
                            if (invalidPassword && result.isAuthenticated()) {
                                throw new RuntimeException("invalid password led to an authenticated user: " + result);
                            } else if (invalidPassword == false && result.isAuthenticated() == false) {
                                throw new RuntimeException("proper password led to an unauthenticated result: " + result);
                            }
                        }, (e) -> {
                            logger.error("caught exception", e);
                            fail("unexpected exception - " + e);
                        }));
                    }

                } catch (InterruptedException e) {
                    logger.error("thread was interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }));
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
        final AtomicInteger lookupCounter = new AtomicInteger(0);

        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("caching", "test_realm");
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        final CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm(config, threadPool) {
            @Override
            protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                listener.onFailure(new UnsupportedOperationException("authenticate should not be called!"));
            }

            @Override
            protected void doLookupUser(String username, ActionListener<User> listener) {
                lookupCounter.incrementAndGet();
                listener.onResponse(new User(username, new String[] { "r1", "r2", "r3" }));
            }
        };

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween(numberOfProcessors, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(10000, 100000);
        final CountDownLatch latch = new CountDownLatch(1 + numberOfThreads);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final int threadNum = i;
            threads.add(new Thread(() -> {
                try {
                    threadPool.getThreadContext().putTransient("key", threadNum);
                    latch.countDown();
                    latch.await();
                    for (int i1 = 0; i1 < numberOfIterations; i1++) {
                        realm.lookupUser(username, ActionListener.wrap((user) -> {
                            assertThat(threadPool.getThreadContext().getTransient("key"), is(threadNum));
                            if (user == null) {
                                throw new RuntimeException("failed to lookup user");
                            }
                        }, (e) -> {
                            logger.error("caught exception", e);
                            fail("unexpected exception");
                        }));
                    }

                } catch (InterruptedException e) {
                    logger.error("thread was interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(1, lookupCounter.get());
    }

    public void testAuthenticateDisabled() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("caching", "test_authentication_disabled");
        final Settings settings = Settings.builder()
            .put(getFullSettingKey(realmId, CachingUsernamePasswordRealmSettings.AUTHC_ENABLED_SETTING), false)
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final ThreadContext threadContext = new ThreadContext(settings);
        final RealmConfig config = new RealmConfig(realmId, settings, env, threadContext);
        final AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(config, threadPool);

        final UsernamePasswordToken token = new UsernamePasswordToken("phil", new SecureString("tahiti"));
        UsernamePasswordToken.putTokenHeader(threadContext, token);
        assertThat(realm.token(threadContext), nullValue());
        assertThat(realm.supports(token), equalTo(false));

        PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
        realm.authenticate(token, authFuture);
        final AuthenticationResult<User> authResult = authFuture.get();
        assertThat(authResult.isAuthenticated(), equalTo(false));
        assertThat(authResult.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));

        PlainActionFuture<User> lookupFuture = new PlainActionFuture<>();
        realm.lookupUser(token.principal(), lookupFuture);
        final User user = lookupFuture.get();
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(token.principal()));
    }

    static class FailingAuthenticationRealm extends CachingUsernamePasswordRealm {

        FailingAuthenticationRealm(Settings global, ThreadPool threadPool) {
            super(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier("caching", "failing-test"),
                    Settings.builder()
                        .put(global)
                        .put(getFullSettingKey(new RealmConfig.RealmIdentifier("caching", "failing-test"), RealmSettings.ORDER_SETTING), 0)
                        .build(),
                    TestEnvironment.newEnvironment(global),
                    threadPool.getThreadContext()
                ),
                threadPool
            );
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
            listener.onResponse(AuthenticationResult.notHandled());
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            listener.onResponse(null);
        }
    }

    static class ThrowingAuthenticationRealm extends CachingUsernamePasswordRealm {

        ThrowingAuthenticationRealm(Settings globalSettings, ThreadPool threadPool) {
            super(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier("caching", "throwing-test"),
                    Settings.builder()
                        .put(globalSettings)
                        .put(getFullSettingKey(new RealmConfig.RealmIdentifier("caching", "throwing-test"), RealmSettings.ORDER_SETTING), 0)
                        .build(),
                    TestEnvironment.newEnvironment(globalSettings),
                    threadPool.getThreadContext()
                ),
                threadPool
            );
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
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

        AlwaysAuthenticateCachingRealm(Settings globalSettings, ThreadPool threadPool) {
            this(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier("caching", "always-test"),
                    Settings.builder()
                        .put(globalSettings)
                        .put(getFullSettingKey(new RealmConfig.RealmIdentifier("caching", "always-test"), RealmSettings.ORDER_SETTING), 0)
                        .build(),
                    TestEnvironment.newEnvironment(globalSettings),
                    threadPool.getThreadContext()
                ),
                threadPool
            );
        }

        AlwaysAuthenticateCachingRealm(RealmConfig config, ThreadPool threadPool) {
            super(config, threadPool);
        }

        void setUsersEnabled(boolean usersEnabled) {
            this.usersEnabled = usersEnabled;
        }

        @Override
        protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
            authInvocationCounter.incrementAndGet();
            final User user = new User(token.principal(), new String[] { "testRole1", "testRole2" }, null, null, emptyMap(), usersEnabled);
            listener.onResponse(AuthenticationResult.success(user));
        }

        @Override
        protected void doLookupUser(String username, ActionListener<User> listener) {
            lookupInvocationCounter.incrementAndGet();
            listener.onResponse(new User(username, new String[] { "lookupRole1", "lookupRole2" }));
        }
    }
}
