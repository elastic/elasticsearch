/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.LocalCheckpointTracker.BIT_SET_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class LocalCheckpointTrackerTests extends ESTestCase {

    private LocalCheckpointTracker tracker;

    public static LocalCheckpointTracker createEmptyTracker() {
        return new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        tracker = createEmptyTracker();
    }

    public void testSimplePrimaryProcessed() {
        long seqNo1, seqNo2;
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        tracker.markSeqNoAsProcessed(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(0L), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(1)), equalTo(false));
        seqNo1 = tracker.generateSeqNo();
        seqNo2 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(1L));
        assertThat(seqNo2, equalTo(2L));
        tracker.markSeqNoAsProcessed(seqNo2);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(seqNo1), equalTo(false));
        assertThat(tracker.hasProcessed(seqNo2), equalTo(true));
        tracker.markSeqNoAsProcessed(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(2L));
        assertThat(tracker.hasProcessed(between(0, 2)), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(3)), equalTo(false));
        assertThat(tracker.getPersistedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.getMaxSeqNo(), equalTo(2L));
    }

    public void testSimplePrimaryPersisted() {
        long seqNo1, seqNo2;
        assertThat(tracker.getPersistedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        tracker.markSeqNoAsPersisted(seqNo1);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        seqNo1 = tracker.generateSeqNo();
        seqNo2 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(1L));
        assertThat(seqNo2, equalTo(2L));
        tracker.markSeqNoAsPersisted(seqNo2);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        tracker.markSeqNoAsPersisted(seqNo1);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(2L));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.getMaxSeqNo(), equalTo(2L));
    }

    public void testSimpleReplicaProcessed() {
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.hasProcessed(randomNonNegativeLong()), equalTo(false));
        tracker.markSeqNoAsProcessed(0L);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(0), equalTo(true));
        tracker.markSeqNoAsProcessed(2L);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(1L), equalTo(false));
        assertThat(tracker.hasProcessed(2L), equalTo(true));
        tracker.markSeqNoAsProcessed(1L);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(2L));
        assertThat(tracker.hasProcessed(between(0, 2)), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(3)), equalTo(false));
        assertThat(tracker.getPersistedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.getMaxSeqNo(), equalTo(2L));
    }

    public void testSimpleReplicaPersisted() {
        assertThat(tracker.getPersistedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.hasProcessed(randomNonNegativeLong()), equalTo(false));
        tracker.markSeqNoAsPersisted(0L);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        tracker.markSeqNoAsPersisted(2L);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        tracker.markSeqNoAsPersisted(1L);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(2L));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.getMaxSeqNo(), equalTo(2L));
    }

    public void testLazyInitialization() {
        /*
         * Previously this would allocate the entire chain of bit sets to the one for the sequence number being marked; for very large
         * sequence numbers this could lead to excessive memory usage resulting in out of memory errors.
         */
        long seqNo = randomNonNegativeLong();
        tracker.markSeqNoAsProcessed(seqNo);
        assertThat(tracker.processedSeqNo.size(), equalTo(1));
        assertThat(tracker.hasProcessed(seqNo), equalTo(true));
        assertThat(tracker.hasProcessed(randomValueOtherThan(seqNo, ESTestCase::randomNonNegativeLong)), equalTo(false));
        assertThat(tracker.processedSeqNo.size(), equalTo(1));
    }

    public void testSimpleOverFlow() {
        List<Long> seqNoList = new ArrayList<>();
        final boolean aligned = randomBoolean();
        final int maxOps = BIT_SET_SIZE * randomIntBetween(1, 5) + (aligned ? 0 : randomIntBetween(1, BIT_SET_SIZE - 1));

        for (long i = 0; i < maxOps; i++) {
            seqNoList.add(i);
        }
        Collections.shuffle(seqNoList, random());
        for (Long seqNo : seqNoList) {
            tracker.markSeqNoAsProcessed(seqNo);
        }
        assertThat(tracker.processedCheckpoint.get(), equalTo(maxOps - 1L));
        assertThat(tracker.processedSeqNo.size(), equalTo(aligned ? 0 : 1));
        if (aligned == false) {
            assertThat(tracker.processedSeqNo.keySet().iterator().next(), equalTo(tracker.processedCheckpoint.get() / BIT_SET_SIZE));
        }
        assertThat(tracker.hasProcessed(randomFrom(seqNoList)), equalTo(true));
        final long notCompletedSeqNo = randomValueOtherThanMany(seqNoList::contains, ESTestCase::randomNonNegativeLong);
        assertThat(tracker.hasProcessed(notCompletedSeqNo), equalTo(false));
    }

    public void testConcurrentPrimary() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final int opsPerThread = randomIntBetween(10, 20);
        final int maxOps = opsPerThread * threads.length;
        final long unFinishedSeq = randomIntBetween(0, maxOps - 2); // make sure we always index the last seqNo to simplify maxSeq checks
        logger.info("--> will run [{}] threads, maxOps [{}], unfinished seq no [{}]", threads.length, maxOps, unFinishedSeq);
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        for (int t = 0; t < threads.length; t++) {
            final int threadId = t;
            threads[t] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new ElasticsearchException("failure in background thread", e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long seqNo = tracker.generateSeqNo();
                        logger.info("[t{}] started   [{}]", threadId, seqNo);
                        if (seqNo != unFinishedSeq) {
                            tracker.markSeqNoAsProcessed(seqNo);
                            logger.info("[t{}] completed [{}]", threadId, seqNo);
                        }
                    }
                }
            }, "testConcurrentPrimary_" + threadId);
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(tracker.getMaxSeqNo(), equalTo(maxOps - 1L));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(unFinishedSeq - 1L));
        tracker.markSeqNoAsProcessed(unFinishedSeq);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(maxOps - 1L));
        assertThat(tracker.processedSeqNo.size(), is(oneOf(0, 1)));
        if (tracker.processedSeqNo.size() == 1) {
            assertThat(tracker.processedSeqNo.keySet().iterator().next(), equalTo(tracker.processedCheckpoint.get() / BIT_SET_SIZE));
        }
    }

    public void testConcurrentReplica() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final int opsPerThread = randomIntBetween(10, 20);
        final int maxOps = opsPerThread * threads.length;
        final long unFinishedSeq = randomIntBetween(0, maxOps - 2); // make sure we always index the last seqNo to simplify maxSeq checks
        Set<Integer> seqNos = IntStream.range(0, maxOps).boxed().collect(Collectors.toSet());

        final Integer[][] seqNoPerThread = new Integer[threads.length][];
        for (int t = 0; t < threads.length - 1; t++) {
            int size = Math.min(seqNos.size(), randomIntBetween(opsPerThread - 4, opsPerThread + 4));
            seqNoPerThread[t] = randomSubsetOf(size, seqNos).toArray(new Integer[size]);
            seqNos.removeAll(Arrays.asList(seqNoPerThread[t]));
        }
        seqNoPerThread[threads.length - 1] = seqNos.toArray(new Integer[seqNos.size()]);
        logger.info("--> will run [{}] threads, maxOps [{}], unfinished seq no [{}]", threads.length, maxOps, unFinishedSeq);
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        for (int t = 0; t < threads.length; t++) {
            final int threadId = t;
            threads[t] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new ElasticsearchException("failure in background thread", e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    Integer[] ops = seqNoPerThread[threadId];
                    for (int seqNo : ops) {
                        if (seqNo != unFinishedSeq) {
                            tracker.markSeqNoAsProcessed(seqNo);
                            logger.info("[t{}] completed [{}]", threadId, seqNo);
                        }
                    }
                }
            }, "testConcurrentReplica_" + threadId);
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(tracker.getMaxSeqNo(), equalTo(maxOps - 1L));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(unFinishedSeq - 1L));
        assertThat(tracker.hasProcessed(unFinishedSeq), equalTo(false));
        tracker.markSeqNoAsProcessed(unFinishedSeq);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(maxOps - 1L));
        assertThat(tracker.hasProcessed(unFinishedSeq), equalTo(true));
        assertThat(tracker.hasProcessed(randomLongBetween(maxOps, Long.MAX_VALUE)), equalTo(false));
        assertThat(tracker.processedSeqNo.size(), is(oneOf(0, 1)));
        if (tracker.processedSeqNo.size() == 1) {
            assertThat(tracker.processedSeqNo.keySet().iterator().next(), equalTo(tracker.processedCheckpoint.get() / BIT_SET_SIZE));
        }
    }

    public void testContains() {
        final long maxSeqNo = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, 100);
        final long localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, maxSeqNo);
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
        if (localCheckpoint >= 0) {
            assertThat(tracker.hasProcessed(randomLongBetween(0, localCheckpoint)), equalTo(true));
        }
        assertThat(tracker.hasProcessed(randomLongBetween(localCheckpoint + 1, Long.MAX_VALUE)), equalTo(false));
        final int numOps = between(1, 100);
        final List<Long> seqNos = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            long seqNo = randomLongBetween(0, 1000);
            seqNos.add(seqNo);
            tracker.markSeqNoAsProcessed(seqNo);
        }
        final long seqNo = randomNonNegativeLong();
        assertThat(tracker.hasProcessed(seqNo), equalTo(seqNo <= localCheckpoint || seqNos.contains(seqNo)));
    }
}
