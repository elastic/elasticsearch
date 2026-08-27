/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SplitSourceServiceTests extends ESTestCase {
    AtomicLong nowInMillis = new AtomicLong();

    public void testHandoffReleasesPermitsWhenIndexIsGone() throws Exception {
        final var permitsClosed = new AtomicInteger();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool())) {
            final var splitSourceService = new SplitSourceService(null, clusterService, null, null, null, null, null, Settings.EMPTY);
            final var goneIndex = new Index("gone", "gone-uuid");
            final var handoff = new PlainActionFuture<ActionResponse>();

            splitSourceService.waitForHandoffSuccessOrFailure(
                new ShardId(goneIndex, 1),
                new ShardId(goneIndex, 0),
                1L,
                1L,
                new AtomicBoolean(true),
                permitsClosed::incrementAndGet,
                handoff
            );

            expectThrows(IndexNotFoundException.class, handoff::actionGet);
        }
        assertEquals(1, permitsClosed.get());
    }

    // test that a RefCountingAcquirer will only acquire the resource once if multiple acquirers arrive while the resource is held
    public void testRefCountedAcquirerAcquiresAndReleasesOnce() throws Exception {
        final var numAcquirers = randomIntBetween(1, 10);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();

        var acquirersArrived = new CountDownLatch(numAcquirers);
        // waits for all acquirers to enter RefCountedAcquirer.acquire, then returns the RefCountedAcquirer
        // a releasable that counts the number of times it has ever been fired
        Consumer<ActionListener<Releasable>> acquirer = listener -> new Thread(() -> {
            acquired.incrementAndGet();
            logger.info("acquiring {}", acquired.get());
            try {
                acquirersArrived.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            listener.onResponse(() -> {
                released.incrementAndGet();
                logger.info("releasing {}", released.get());
            });
        }).start();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(
            acquirer,
            nowInMillis::incrementAndGet,
            duration -> assertEquals(1, duration)
        );

        var threads = new Thread[numAcquirers];
        // creates numAcquirers threads that will enter acquire and then complete
        var acquirersAcquired = new CountDownLatch(numAcquirers);
        for (int i = 0; i < numAcquirers; i++) {
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ignored) {}
                refCountedAcquirer.acquire(runAndRelease(acquirersAcquired::countDown));
                acquirersArrived.countDown();
            });
            threads[i].start();
        }

        for (int i = 0; i < numAcquirers; i++) {
            threads[i].join();
        }
        acquirersAcquired.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);

        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });
    }

    // test that a RefCountingAcquirer will acquire the provided resource again after it has released it
    public void testRefCountedAcquirerCanReacquire() throws Exception {
        // counts the number of times it's been acquired and released
        final var acquired = new AtomicInteger();
        final var released = new AtomicInteger();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            listener.onResponse(released::incrementAndGet);
        }, nowInMillis::incrementAndGet, duration -> assertEquals(1, duration));

        var acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });

        acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(2));
            assertThat(released.get(), equalTo(2));
        });
    }

    // test that a RefCountingAcquirer releases its resource if it acquires it
    public void testRefCountedAcquirer() throws Exception {
        AtomicInteger acquireCount = new AtomicInteger();
        AtomicInteger releaseCount = new AtomicInteger();

        Consumer<ActionListener<Releasable>> acquirer = (listener) -> new Thread(() -> {
            try {
                Thread.sleep(randomIntBetween(1, 100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            acquireCount.incrementAndGet();
            listener.onResponse(releaseCount::incrementAndGet);
        }).start();

        SplitSourceService.RefCountedAcquirer refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(
            acquirer,
            nowInMillis::incrementAndGet,
            duration -> assertEquals(1, duration)
        );

        int numThreads = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numThreads];
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final var tid = i;
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                logger.info("acquiring {}", tid);
                refCountedAcquirer.acquire(runAndRelease(latch::countDown));
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        latch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> assertThat(releaseCount.get(), equalTo(acquireCount.get())));
    }

    // verify that if resource acquisition fails, the refcount is still decremented properly
    // we do this by verifying that we attempt to acquire again, which we only do when the refcount is at 0
    public void testReleaseWhenAcquireFails() {
        // increments when acquisition is attempted
        AtomicInteger acquired = new AtomicInteger();
        // will only increment if acquire is called with resource successfully held
        AtomicInteger withResource = new AtomicInteger();
        SplitSourceService.RefCountedAcquirer acquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            throw new IllegalStateException("oops");
        }, nowInMillis::incrementAndGet, duration -> assertEquals(1, duration));

        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        // attempted, then dropped ref, then attempted again
        assertEquals(acquired.get(), 2);
        // never actually obtained the resource
        assertEquals(withResource.get(), 0);
    }

    public void testSetupTargetShardFailsWhenSplitIsNoLongerInProgress() {
        var indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true).build())
            .build();

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        // The null args are not reached before the request is rejected.
        var splitSourceService = new SplitSourceService(
            null,
            clusterService,
            null,
            mock(StatelessCommitService.class),
            null,
            null,
            null,
            Settings.EMPTY
        );

        var exception = expectThrows(
            StaleSplitRequestException.class,
            () -> splitSourceService.setupTargetShard(null, new ShardId(indexMetadata.getIndex(), 0), 1L, 1L, ActionListener.noop())
        );
        assertThat(exception.getMessage(), containsString("No split is in progress"));
    }

    private ActionListener<Releasable> runAndRelease(Runnable runnable) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    runnable.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("acquiring failed", e);
            }
        };
    }
}
