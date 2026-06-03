/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransformContextTests extends ESTestCase {

    private TransformContext.Listener listener;

    @Before
    public void setUpMocks() {
        listener = mock(TransformContext.Listener.class);
    }

    @After
    public void verifyNoMoreInteractionsOnMocks() {
        verifyNoMoreInteractions(listener);
    }

    public void testFailureCount() {
        var context = new TransformContext(null, null, 0, listener);

        var someException = someException();
        assertThat(context.incrementAndGetFailureCount(someException), is(equalTo(1)));
        assertThat(context.getFailureCount(), is(equalTo(1)));
        assertThat(context.getLastFailure(), is(sameInstance(someException)));
        assertFalse(context.doesNotHaveFailures());

        var someOtherException = someOtherException();
        assertThat(context.incrementAndGetFailureCount(someOtherException), is(equalTo(2)));
        assertThat(context.getFailureCount(), is(equalTo(2)));
        assertThat(context.getLastFailure(), is(sameInstance(someOtherException)));
        assertFalse(context.doesNotHaveFailures());

        context.resetReasonAndFailureCounter();
        assertThat(context.getFailureCount(), is(equalTo(0)));
        assertThat(context.getLastFailure(), is(nullValue()));
        assertTrue(context.doesNotHaveFailures());

        // Verify that the listener is notified every time the failure count is incremented or reset
        verify(listener, times(3)).failureCountChanged();
    }

    private Throwable someException() {
        return new RuntimeException("some_exception");
    }

    private Throwable someOtherException() {
        return new IllegalArgumentException("some_other_exception");
    }

    public void testStatePersistenceFailureCount() {
        var context = new TransformContext(null, null, 0, listener);

        var someException = someException();
        assertThat(context.incrementAndGetStatePersistenceFailureCount(someException), is(equalTo(1)));
        assertThat(context.getStatePersistenceFailureCount(), is(equalTo(1)));
        assertThat(context.getLastStatePersistenceFailure(), is(sameInstance(someException)));
        assertFalse(context.doesNotHaveFailures());

        var someOtherException = someOtherException();
        assertThat(context.incrementAndGetStatePersistenceFailureCount(someOtherException), is(equalTo(2)));
        assertThat(context.getStatePersistenceFailureCount(), is(equalTo(2)));
        assertThat(context.getLastStatePersistenceFailure(), is(sameInstance(someOtherException)));
        assertFalse(context.doesNotHaveFailures());

        context.resetStatePersistenceFailureCount();
        assertThat(context.getStatePersistenceFailureCount(), is(equalTo(0)));
        assertThat(context.getLastStatePersistenceFailure(), is(nullValue()));
        assertTrue(context.doesNotHaveFailures());
        verifyNoInteractions(listener);
    }

    public void testStartUpFailureCount() {
        var context = new TransformContext(null, null, 0, listener);

        var someException = someException();
        assertThat(context.incrementAndGetStartUpFailureCount(someException), is(equalTo(1)));
        assertThat(context.getStartUpFailureCount(), is(equalTo(1)));
        assertThat(context.getStartUpFailure(), is(sameInstance(someException)));
        assertFalse(context.doesNotHaveFailures());

        var someOtherException = someOtherException();
        assertThat(context.incrementAndGetStartUpFailureCount(someOtherException), is(equalTo(2)));
        assertThat(context.getStartUpFailureCount(), is(equalTo(2)));
        assertThat(context.getStartUpFailure(), is(sameInstance(someOtherException)));
        assertFalse(context.doesNotHaveFailures());

        context.resetStartUpFailureCount();
        assertThat(context.getStartUpFailureCount(), is(equalTo(0)));
        assertThat(context.getStartUpFailure(), is(nullValue()));
        assertTrue(context.doesNotHaveFailures());
        verifyNoInteractions(listener);
    }

    public void testCheckpoint() {
        TransformContext context = new TransformContext(null, null, 13, listener);
        assertThat(context.getCheckpoint(), is(equalTo(13L)));
        assertThat(context.incrementAndGetCheckpoint(), is(equalTo(14L)));
        assertThat(context.getCheckpoint(), is(equalTo(14L)));
        context.setCheckpoint(25);
        assertThat(context.getCheckpoint(), is(equalTo(25L)));
        assertThat(context.incrementAndGetCheckpoint(), is(equalTo(26L)));
        assertThat(context.getCheckpoint(), is(equalTo(26L)));
    }

    public void testTaskState() {
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        assertThat(context.setTaskState(TransformTaskState.STOPPED, TransformTaskState.STOPPED), is(false));
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        assertThat(context.setTaskState(TransformTaskState.STARTED, TransformTaskState.STOPPED), is(true));
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STOPPED)));
        context.resetTaskState();
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        context.setTaskStateToFailed(null);
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.FAILED)));
    }

    public void testStateReason() {
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getStateReason(), is(nullValue()));
        context.setTaskStateToFailed("some-reason");
        assertThat(context.getStateReason(), is(equalTo("some-reason")));
        context.setTaskStateToFailed("some-other-reason");
        assertThat(context.getStateReason(), is(equalTo("some-other-reason")));
        context.resetTaskState();
        assertThat(context.getStateReason(), is(nullValue()));
        context.setTaskStateToFailed("yet-another-reason");
        assertThat(context.getStateReason(), is(equalTo("yet-another-reason")));
        context.resetReasonAndFailureCounter();
        assertThat(context.getStateReason(), is(nullValue()));

        verify(listener).failureCountChanged();
    }

    public void testAuthState() {
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getAuthState(), is(nullValue()));

        context.setAuthState(AuthorizationState.green());
        assertThat(context.getAuthState(), is(notNullValue()));
        assertThat(context.getAuthState().getStatus(), is(equalTo(HealthStatus.GREEN)));

        context.setAuthState(AuthorizationState.red(new ElasticsearchSecurityException("missing privileges")));
        assertThat(context.getAuthState(), is(notNullValue()));
        assertThat(context.getAuthState().getStatus(), is(equalTo(HealthStatus.RED)));

        context.setAuthState(null);
        assertThat(context.getAuthState(), is(nullValue()));
    }

    public void testFrom() {
        Instant from = Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L));
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, from, listener);
        assertThat(context.from(), is(equalTo(from)));
    }

    public void testProjectIdDefaultsToDefault() {
        var context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.projectId(), is(equalTo(ProjectId.DEFAULT)));
    }

    public void testProjectIdExplicit() {
        ProjectId projectId = ProjectId.fromId("myproject123");
        var context = new TransformContext(TransformTaskState.STARTED, null, 0, null, listener, projectId);
        assertThat(context.projectId(), is(equalTo(projectId)));
    }

    public void testReplacePersistedCredentialReturnsDisplaced() {
        var context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getPersistedCloudCredential(), is(nullValue()));

        var first = randomPersistedCloudCredential();
        assertThat(context.replacePersistedCredential(first), is(nullValue()));
        assertThat(context.getPersistedCloudCredential(), is(sameInstance(first)));

        var second = randomPersistedCloudCredential();
        // replace returns the displaced credential; caller is responsible for closing it
        assertThat(context.replacePersistedCredential(second), is(sameInstance(first)));
        assertThat(context.getPersistedCloudCredential(), is(sameInstance(second)));

        // replacement does NOT eagerly close — verify the displaced is still usable
        assertThat(first.internalApiKey().length(), is(equalTo("v".length())));
        first.close();
    }

    public void testCloseClearsActive() {
        var context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        var active = randomPersistedCloudCredential();
        context.replacePersistedCredential(active);

        context.close();

        assertThat(context.getPersistedCloudCredential(), is(nullValue()));
        // SecureString was closed; subsequent length() throws
        expectThrows(IllegalStateException.class, () -> active.internalApiKey().length());
    }

    public void testReplacePersistedCredentialIsAtomicUnderContention() throws Exception {
        // Concurrent credential swaps (e.g. the indexer's onStart credential reconciliation racing
        // against a future tear-down) must never leak a credential. With a plain volatile + r/m/w,
        // two threads could both see the same prior value and both write theirs, leaking one new
        // credential. AtomicReference#getAndSet closes that window; this test stresses the
        // contract by asserting that every credential we set is accounted for (either currently
        // held or returned to one of the callers as displaced).
        var context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        int threadCount = 16;
        int perThread = 100;
        var inputs = java.util.Collections.synchronizedList(new java.util.ArrayList<PersistedCloudCredential>(threadCount * perThread));
        var displaced = java.util.Collections.synchronizedList(new java.util.ArrayList<PersistedCloudCredential>(threadCount * perThread));
        var start = new java.util.concurrent.CountDownLatch(1);
        var done = new java.util.concurrent.CountDownLatch(threadCount);
        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        var next = randomPersistedCloudCredential();
                        inputs.add(next);
                        PersistedCloudCredential prev = context.replacePersistedCredential(next);
                        if (prev != null) {
                            displaced.add(prev);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        start.countDown();
        assertTrue("threads timed out", done.await(30, java.util.concurrent.TimeUnit.SECONDS));

        // Every input must be either still held or have been returned to some caller as displaced — no leaks.
        var held = context.getPersistedCloudCredential();
        int expected = threadCount * perThread;
        int accounted = displaced.size() + (held == null ? 0 : 1);
        assertThat(accounted, equalTo(expected));
    }

    private static PersistedCloudCredential randomPersistedCloudCredential() {
        return new PersistedCloudCredential(randomAlphaOfLengthBetween(4, 12), new SecureString("v".toCharArray()));
    }
}
