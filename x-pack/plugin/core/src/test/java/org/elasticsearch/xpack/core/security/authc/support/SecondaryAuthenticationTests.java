/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
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
        expectThrows(NullPointerException.class, () -> new SecondaryAuthentication(securityContext, null));
    }

    public void testSynchronousExecuteInSecondaryContext() {
        final User user1 = new User("u1", "role1");
        securityContext.setUser(user1, Version.CURRENT);
        assertThat(securityContext.getUser().principal(), equalTo("u1"));

        final Authentication authentication2 = new Authentication(new User("u2", "role2"), realm(), realm());
        final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2);

        assertThat(securityContext.getUser().principal(), equalTo("u1"));
        var result = secondaryAuth.execute(original -> {
            assertThat(securityContext.getUser().principal(), equalTo("u2"));
            assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
            return "xyzzy";
        });
        assertThat(securityContext.getUser().principal(), equalTo("u1"));
        assertThat(result, equalTo("xyzzy"));
    }

    public void testSecondaryContextCanBeRestored() {
        final User user1 = new User("u1", "role1");
        securityContext.setUser(user1, Version.CURRENT);
        assertThat(securityContext.getUser().principal(), equalTo("u1"));

        final Authentication authentication2 = new Authentication(new User("u2", "role2"), realm(), realm());
        final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2);

        assertThat(securityContext.getUser().principal(), equalTo("u1"));
        final AtomicReference<ThreadContext.StoredContext> secondaryContext = new AtomicReference<>();
        secondaryAuth.execute(storedContext -> {
            assertThat(securityContext.getUser().principal(), equalTo("u2"));
            assertThat(securityContext.getAuthentication(), sameInstance(authentication2));
            secondaryContext.set(threadPool.getThreadContext().newStoredContext(false));
            storedContext.restore();
            assertThat(securityContext.getUser().principal(), equalTo("u1"));
            return null;
        });
        assertThat(securityContext.getUser().principal(), equalTo("u1"));
        secondaryContext.get().restore();
        assertThat(securityContext.getUser().principal(), equalTo("u2"));
    }

    public void testWrapRunnable() throws Exception {
        final User user1 = new User("u1", "role1");
        securityContext.setUser(user1, Version.CURRENT);
        assertThat(securityContext.getUser().principal(), equalTo("u1"));

        final Authentication authentication2 = new Authentication(new User("u2", "role2"), realm(), realm());
        final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2);

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
        future.get(1, TimeUnit.SECONDS);
    }

    public void testPreserveSecondaryContextAcrossThreads() throws Exception {
        final User user1 = new User("u1", "role1");
        securityContext.setUser(user1, Version.CURRENT);
        assertThat(securityContext.getUser().principal(), equalTo("u1"));

        final Authentication authentication2 = new Authentication(new User("u2", "role2"), realm(), realm());
        final SecondaryAuthentication secondaryAuth = new SecondaryAuthentication(securityContext, authentication2);

        assertThat(securityContext.getUser().principal(), equalTo("u1"));

        final AtomicReference<User> threadUser = new AtomicReference<>();
        final AtomicReference<User> listenerUser = new AtomicReference<>();

        final ThreadContext threadContext = threadPool.getThreadContext();
        secondaryAuth.execute(originalContext -> {
            assertThat(securityContext.getUser().principal(), equalTo("u2"));
            ActionListener<Void> listener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(false),
                ActionListener.wrap(() -> listenerUser.set(securityContext.getUser())));
            originalContext.restore();
            threadPool.generic().execute(() -> {
                threadUser.set(securityContext.getUser());
                listener.onResponse(null);
            });
            return null;
        });
        assertThat(securityContext.getUser().principal(), equalTo("u1"));
        assertBusy(() -> assertThat(listenerUser.get(), notNullValue()), 1, TimeUnit.SECONDS);
        assertThat(threadUser.get(), notNullValue());
        assertThat(threadUser.get().principal(), equalTo("u1"));
        assertThat(listenerUser.get().principal(), equalTo("u2"));
    }

    private Authentication.RealmRef realm() {
        return new Authentication.RealmRef(randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(2, 4), randomAlphaOfLength(12));
    }

}
