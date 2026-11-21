/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that every listener waiting on a cluster state consistency check is completed, including when the lease cannot be read. Callers
 * such as the stale commit cleaner hold a permit until their delayed listener completes, so a listener that is never completed stops
 * blob deletion on the node.
 */
public class StatelessClusterConsistencyServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void createServices() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void closeServices() {
        clusterService.close();
        terminate(threadPool);
    }

    /**
     * An election strategy that fails a given number of lease reads before answering with a lease that matches the applied cluster state.
     * The real strategy reads the lease from the object store, which these tests replace to control when the read fails.
     */
    private class FailingLeaseReadElectionStrategy extends StatelessElectionStrategy {

        private final AtomicInteger leaseReadsToFail;
        private final Exception failure;

        FailingLeaseReadElectionStrategy(int leaseReadsToFail, Exception failure) {
            super(() -> { throw new AssertionError("the lease is not read from a blob container in this test"); }, threadPool);
            this.leaseReadsToFail = new AtomicInteger(leaseReadsToFail);
            this.failure = failure;
        }

        @Override
        public void readLease(ActionListener<Optional<StatelessLease>> listener) {
            // Complete on another thread, as the real implementation does
            threadPool.generic().execute(() -> {
                if (leaseReadsToFail.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
                    listener.onFailure(failure);
                } else {
                    final var state = clusterService.state();
                    listener.onResponse(
                        Optional.of(
                            new StatelessLease(
                                state.term(),
                                state.nodes().getNodeLeftGeneration(),
                                StatelessLease.getProjectsMarkedForDeletionGeneration(state)
                            )
                        )
                    );
                }
            });
        }
    }

    private StatelessClusterConsistencyService createConsistencyService(StatelessElectionStrategy electionStrategy, TimeValue interval) {
        return new StatelessClusterConsistencyService(
            clusterService,
            electionStrategy,
            threadPool,
            Settings.builder()
                .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), interval)
                .build()
        );
    }

    private static List<SubscribableListener<Void>> addDelayedChecks(StatelessClusterConsistencyService consistencyService) {
        final var delayedChecks = new ArrayList<SubscribableListener<Void>>();
        for (int i = randomIntBetween(1, 8); i > 0; i--) {
            final var delayedCheck = new SubscribableListener<Void>();
            consistencyService.delayedEnsureClusterStateConsistentWithRootBlob(delayedCheck);
            delayedChecks.add(delayedCheck);
        }
        return delayedChecks;
    }

    /**
     * An immediate check picks up the delayed listeners that were queued before it. If its lease read fails, the delayed listeners must
     * fail too since nothing else will complete them.
     */
    public void testLeaseReadFailureFailsDelayedListenersOfImmediateCheck() {
        final var failure = new IOException("simulated lease read failure");
        // The service is not started, so the background monitor cannot pick up the delayed listeners
        final var consistencyService = createConsistencyService(
            new FailingLeaseReadElectionStrategy(1, failure),
            TimeValue.timeValueMinutes(1)
        );

        final var delayedChecks = addDelayedChecks(consistencyService);
        final var immediateCheck = new SubscribableListener<Void>();
        consistencyService.ensureClusterStateConsistentWithRootBlob(immediateCheck, TimeValue.MAX_VALUE);

        assertThat(safeAwaitFailure(immediateCheck), sameInstance(failure));
        for (var delayedCheck : delayedChecks) {
            assertThat(safeAwaitFailure(delayedCheck), sameInstance(failure));
        }
    }

    /**
     * The background monitor runs the check for delayed listeners with a listener of its own that ignores the outcome. A failed lease
     * read must still fail the delayed listeners so that their owners can retry, and a later check must succeed.
     */
    public void testLeaseReadFailureFailsDelayedListenersOfBackgroundMonitor() {
        final var failure = new IOException("simulated lease read failure");
        final var consistencyService = createConsistencyService(
            new FailingLeaseReadElectionStrategy(1, failure),
            TimeValue.timeValueMillis(100)
        );
        consistencyService.start();
        try {
            for (var delayedCheck : addDelayedChecks(consistencyService)) {
                assertThat(safeAwaitFailure(delayedCheck), sameInstance(failure));
            }
            // The lease can be read again, so listeners that are queued afterwards, as a retry would, complete successfully
            for (var delayedCheck : addDelayedChecks(consistencyService)) {
                safeAwait(delayedCheck);
            }
        } finally {
            consistencyService.close();
        }
    }

    /**
     * Without failures, an immediate check completes the delayed listeners that were queued before it.
     */
    public void testDelayedListenersCompleteWithImmediateCheck() {
        final var consistencyService = createConsistencyService(
            new FailingLeaseReadElectionStrategy(0, new IOException("unused")),
            TimeValue.timeValueMinutes(1)
        );

        final var delayedChecks = addDelayedChecks(consistencyService);
        final var immediateCheck = new SubscribableListener<Void>();
        consistencyService.ensureClusterStateConsistentWithRootBlob(immediateCheck, TimeValue.MAX_VALUE);

        safeAwait(immediateCheck);
        delayedChecks.forEach(ESTestCase::safeAwait);
    }
}
