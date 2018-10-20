/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.LocalCheckpointTracker.BIT_SET_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;

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

    public void testSimplePrimary() {
        long seqNo1, seqNo2;
        assertThat(tracker.getCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        tracker.markSeqNoAsCompleted(seqNo1);
        assertThat(tracker.getCheckpoint(), equalTo(0L));
        assertThat(tracker.contains(0L), equalTo(true));
        assertThat(tracker.contains(atLeast(1)), equalTo(false));
        seqNo1 = tracker.generateSeqNo();
        seqNo2 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(1L));
        assertThat(seqNo2, equalTo(2L));
        tracker.markSeqNoAsCompleted(seqNo2);
        assertThat(tracker.getCheckpoint(), equalTo(0L));
        assertThat(tracker.contains(seqNo1), equalTo(false));
        assertThat(tracker.contains(seqNo2), equalTo(true));
        tracker.markSeqNoAsCompleted(seqNo1);
        assertThat(tracker.getCheckpoint(), equalTo(2L));
        assertThat(tracker.contains(between(0, 2)), equalTo(true));
        assertThat(tracker.contains(atLeast(3)), equalTo(false));
    }

    public void testSimpleReplica() {
        assertThat(tracker.getCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        assertThat(tracker.contains(randomNonNegativeLong()), equalTo(false));
        tracker.markSeqNoAsCompleted(0L);
        assertThat(tracker.getCheckpoint(), equalTo(0L));
        assertThat(tracker.contains(0), equalTo(true));
        tracker.markSeqNoAsCompleted(2L);
        assertThat(tracker.getCheckpoint(), equalTo(0L));
        assertThat(tracker.contains(1L), equalTo(false));
        assertThat(tracker.contains(2L), equalTo(true));
        tracker.markSeqNoAsCompleted(1L);
        assertThat(tracker.getCheckpoint(), equalTo(2L));
        assertThat(tracker.contains(between(0, 2)), equalTo(true));
        assertThat(tracker.contains(atLeast(3)), equalTo(false));
    }

    public void testLazyInitialization() {
        /*
         * Previously this would allocate the entire chain of bit sets to the one for the sequence number being marked; for very large
         * sequence numbers this could lead to excessive memory usage resulting in out of memory errors.
         */
        long seqNo = randomNonNegativeLong();
        tracker.markSeqNoAsCompleted(seqNo);
        assertThat(tracker.processedSeqNo.size(), equalTo(1));
        assertThat(tracker.contains(seqNo), equalTo(true));
        assertThat(tracker.contains(randomValueOtherThan(seqNo, ESTestCase::randomNonNegativeLong)), equalTo(false));
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
            tracker.markSeqNoAsCompleted(seqNo);
        }
        assertThat(tracker.checkpoint, equalTo(maxOps - 1L));
        assertThat(tracker.processedSeqNo.size(), equalTo(aligned ? 0 : 1));
        if (aligned == false) {
            assertThat(tracker.processedSeqNo.keys().iterator().next().value, equalTo(tracker.checkpoint / BIT_SET_SIZE));
        }
        assertThat(tracker.contains(randomFrom(seqNoList)), equalTo(true));
        final long notCompletedSeqNo = randomValueOtherThanMany(seqNoList::contains, ESTestCase::randomNonNegativeLong);
        assertThat(tracker.contains(notCompletedSeqNo), equalTo(false));
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
                            tracker.markSeqNoAsCompleted(seqNo);
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
        assertThat(tracker.getCheckpoint(), equalTo(unFinishedSeq - 1L));
        tracker.markSeqNoAsCompleted(unFinishedSeq);
        assertThat(tracker.getCheckpoint(), equalTo(maxOps - 1L));
        assertThat(tracker.processedSeqNo.size(), isOneOf(0, 1));
        if (tracker.processedSeqNo.size() == 1) {
            assertThat(tracker.processedSeqNo.keys().iterator().next().value, equalTo(tracker.checkpoint / BIT_SET_SIZE));
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
                            tracker.markSeqNoAsCompleted(seqNo);
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
        assertThat(tracker.getCheckpoint(), equalTo(unFinishedSeq - 1L));
        assertThat(tracker.contains(unFinishedSeq), equalTo(false));
        tracker.markSeqNoAsCompleted(unFinishedSeq);
        assertThat(tracker.getCheckpoint(), equalTo(maxOps - 1L));
        assertThat(tracker.contains(unFinishedSeq), equalTo(true));
        assertThat(tracker.contains(randomLongBetween(maxOps, Long.MAX_VALUE)), equalTo(false));
        assertThat(tracker.processedSeqNo.size(), isOneOf(0, 1));
        if (tracker.processedSeqNo.size() == 1) {
            assertThat(tracker.processedSeqNo.keys().iterator().next().value, equalTo(tracker.checkpoint / BIT_SET_SIZE));
        }
    }

    public void testWaitForOpsToComplete() throws BrokenBarrierException, InterruptedException {
        final int seqNo = randomIntBetween(0, 32);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean complete = new AtomicBoolean();
        final Thread thread = new Thread(() -> {
            try {
                // sychronize starting with the test thread
                barrier.await();
                tracker.waitForOpsToComplete(seqNo);
                complete.set(true);
                // synchronize with the test thread checking if we are no longer waiting
                barrier.await();
            } catch (BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        // synchronize starting with the waiting thread
        barrier.await();

        final List<Integer> elements = IntStream.rangeClosed(0, seqNo).boxed().collect(Collectors.toList());
        Randomness.shuffle(elements);
        for (int i = 0; i < elements.size() - 1; i++) {
            tracker.markSeqNoAsCompleted(elements.get(i));
            assertFalse(complete.get());
        }

        tracker.markSeqNoAsCompleted(elements.get(elements.size() - 1));
        // synchronize with the waiting thread to mark that it is complete
        barrier.await();
        assertTrue(complete.get());

        thread.join();
    }

    public void testContains() {
        final long maxSeqNo = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, 100);
        final long localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, maxSeqNo);
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
        if (localCheckpoint >= 0) {
            assertThat(tracker.contains(randomLongBetween(0, localCheckpoint)), equalTo(true));
        }
        assertThat(tracker.contains(randomLongBetween(localCheckpoint + 1, Long.MAX_VALUE)), equalTo(false));
        final int numOps = between(1, 100);
        final List<Long> seqNos = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            long seqNo = randomLongBetween(0, 1000);
            seqNos.add(seqNo);
            tracker.markSeqNoAsCompleted(seqNo);
        }
        final long seqNo = randomNonNegativeLong();
        assertThat(tracker.contains(seqNo), equalTo(seqNo <= localCheckpoint || seqNos.contains(seqNo)));
    }
}
