/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SecondaryAuthenticationTests extends ESTestCase {

    private SecurityContext securityContext;
    private ThreadPool threadPool;

    @Before
    public void setupObjects() {
        this.threadPool = new TestThreadPool(getTestName());
        this.securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
    }

    @After
    public void cleanup() {
        this.threadPool.shutdownNow();
    }

    public void testCannotCreateObjectWithNullAuthentication() {
        expectThrows(NullPointerException.class, () -> new SecondaryAuthentication(securityContext, null, Map.of()));
    }

    public void testSynchronousExecuteInSecondaryContext() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("not-u2", "not-role2"))
                .realmRef(realm())
                .runAs()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, Map.of());

            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            var result = secondaryAuth.execute(original -> {
                assertThat(securityContext.getUser().principal(), equalTo("u2"));
                assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
                return "xyzzy";
            });
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            assertThat(result, equalTo("xyzzy"));
        });
    }

    public void testSecondaryContextCanBeRestored() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("not-u2", "not-role2"))
                .realmRef(realm())
                .runAs()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, Map.of());

            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            final AtomicReference<ThreadContext.StoredContext> secondaryContext = new AtomicReference<>();
            secondaryAuth.execute(storedContext -> {
                assertThat(securityContext.getUser().principal(), equalTo("u2"));
                assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
                secondaryContext.set(threadPool.getThreadContext().newStoredContext());
                storedContext.restore();
                assertThat(securityContext.getUser().principal(), equalTo("u1"));
                return null;
            });
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            secondaryContext.get().restore();
            assertThat(securityContext.getUser().principal(), equalTo("u2"));
        });
    }

    public void testWrapRunnable() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("not-u2", "not-role2"))
                .realmRef(realm())
                .runAs()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, Map.of());

            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            final Semaphore semaphore = new Semaphore(0);
            final Future<?> future = threadPool.generic().submit(secondaryAuth.wrap(() -> {
                try {
                    assertThat(securityContext.getUser().principal(), equalTo("u2"));
                    semaphore.acquire();
                    assertThat(securityContext.getUser().principal(), equalTo("u2"));
                    semaphore.acquire();
                    assertThat(securityContext.getUser().principal(), equalTo("u2"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }));
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            semaphore.release();
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            semaphore.release();
            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            // ensure that the runnable didn't throw any exceptions / assertions
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testPreserveSecondaryContextAcrossThreads() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("not-u2", "not-role2"))
                .realmRef(realm())
                .runAs()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, Map.of());

            assertThat(securityContext.getUser().principal(), equalTo("u1"));

            final AtomicReference<User> threadUser = new AtomicReference<>();
            final AtomicReference<User> listenerUser = new AtomicReference<>();

            final ThreadContext threadContext = threadPool.getThreadContext();
            secondaryAuth.execute(originalContext -> {
                assertThat(securityContext.getUser().principal(), equalTo("u2"));
                ActionListener<Void> listener = new ContextPreservingActionListener<>(
                    threadContext.newRestorableContext(false),
                    ActionListener.running(() -> listenerUser.set(securityContext.getUser()))
                );
                originalContext.restore();
                threadPool.generic().execute(() -> {
                    threadUser.set(securityContext.getUser());
                    listener.onResponse(null);
                });
                return null;
            });
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            try {
                assertBusy(() -> assertThat(listenerUser.get(), notNullValue()), 1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertThat(threadUser.get(), notNullValue());
            assertThat(threadUser.get().principal(), equalTo("u1"));
            assertThat(listenerUser.get().principal(), equalTo("u2"));
        });
    }

    public void testCapturedTransientsAreRestoredDuringExecute() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final Map<String, Object> transients = Map.of(
                "_secondary_auth_captured_string",
                "captured-value",
                "_secondary_auth_captured_number",
                42
            );
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, transients);

            var result = secondaryAuth.execute(original -> {
                assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
                assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_string"), equalTo("captured-value"));
                assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_number"), equalTo(42));
                return "ok";
            });
            assertThat(result, equalTo("ok"));

            assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_string"), nullValue());
            assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_number"), nullValue());
        });
    }

    public void testCapturedTransientsAreRestoredDuringWrap() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final Map<String, Object> transients = Map.of("_secondary_auth_captured_credential", "credential-value");
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, transients);

            final AtomicReference<Object> capturedTransient = new AtomicReference<>();
            Runnable wrapped = secondaryAuth.wrap(() -> {
                capturedTransient.set(threadPool.getThreadContext().getTransient("_secondary_auth_captured_credential"));
            });

            assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_credential"), nullValue());
            wrapped.run();
            assertThat(capturedTransient.get(), equalTo("credential-value"));
            assertThat(threadPool.getThreadContext().getTransient("_secondary_auth_captured_credential"), nullValue());
        });
    }

    public void testEmptyCapturedTransientsWorkCorrectly() {
        final User user1 = new User("u1", "role1");
        setUser(user1, () -> {
            final Authentication authentication2 = AuthenticationTestHelper.builder()
                .user(new User("u2", "role2"))
                .realmRef(realm())
                .build();
            final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2, Map.of());

            assertThat(secondaryAuth.getTransientHeaders(), equalTo(Map.of()));

            var result = secondaryAuth.execute(original -> {
                assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
                return "ok";
            });
            assertThat(result, equalTo("ok"));
        });
    }

    private Authentication.RealmRef realm() {
        return new Authentication.RealmRef(randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(2, 4), randomAlphaOfLength(12));
    }

    private void setUser(User user, Runnable runnable) {
        final Authentication authentication = AuthenticationTestHelper.builder().user(user).build();
        securityContext.executeWithAuthentication(authentication, ignored -> {
            runnable.run();
            return null;
        });
    }
}
