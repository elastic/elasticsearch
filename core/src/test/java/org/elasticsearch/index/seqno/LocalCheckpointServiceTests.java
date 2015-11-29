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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class LocalCheckpointServiceTests extends ESTestCase {

    LocalCheckpointService checkpointService;

    final int SMALL_INDEX_LAG_THRESHOLD = 10;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        checkpointService = getCheckpointService(SMALL_INDEX_LAG_THRESHOLD, LocalCheckpointService.DEFAULT_INDEX_LAG_MAX_WAIT);
    }

    protected LocalCheckpointService getCheckpointService(int thresholdLag, TimeValue thresholdDelay) {
        return new LocalCheckpointService(
                new ShardId("test", 0),
                IndexSettingsModule.newIndexSettings("test",
                        Settings.builder()
                                .put(LocalCheckpointService.SETTINGS_INDEX_LAG_THRESHOLD, thresholdLag)
                                .put(LocalCheckpointService.SETTINGS_INDEX_LAG_MAX_WAIT, thresholdDelay)
                                .build()
                ));
    }

    public void testSimplePrimary() {
        long seqNo1, seqNo2;
        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));
        seqNo1 = checkpointService.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        checkpointService.markSeqNoAsCompleted(seqNo1);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        seqNo1 = checkpointService.generateSeqNo();
        seqNo2 = checkpointService.generateSeqNo();
        assertThat(seqNo1, equalTo(1L));
        assertThat(seqNo2, equalTo(2L));
        checkpointService.markSeqNoAsCompleted(seqNo2);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        checkpointService.markSeqNoAsCompleted(seqNo1);
        assertThat(checkpointService.getCheckpoint(), equalTo(2L));
    }

    public void testSimpleReplica() {
        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));
        checkpointService.markSeqNoAsCompleted(0L);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        checkpointService.markSeqNoAsCompleted(2L);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        checkpointService.markSeqNoAsCompleted(1L);
        assertThat(checkpointService.getCheckpoint(), equalTo(2L));
    }

    public void testIndexThrottleSuccessPrimary() throws Exception {
        LocalCheckpointService checkpoint = getCheckpointService(3, TimeValue.timeValueHours(1));
        final long seq1 = checkpoint.generateSeqNo();
        final long seq2 = checkpoint.generateSeqNo();
        final long seq3 = checkpoint.generateSeqNo();
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final AtomicBoolean threadDone = new AtomicBoolean(false);
        Thread backgroundThread = new Thread(() -> {
            threadStarted.countDown();
            checkpoint.generateSeqNo();
            threadDone.set(true);
        }, "testIndexDelayPrimary");
        backgroundThread.start();
        logger.info("--> waiting for thread to start");
        threadStarted.await();
        assertFalse("background thread finished but should have waited", threadDone.get());
        checkpoint.markSeqNoAsCompleted(seq2);
        assertFalse("background thread finished but should have waited (seq2 completed)", threadDone.get());
        checkpoint.markSeqNoAsCompleted(seq1);
        logger.info("--> waiting for thread to stop");
        assertBusy(() -> {
            assertTrue("background thread should finished after finishing seq1", threadDone.get());
        });
    }

    public void testIndexThrottleTimeoutPrimary() throws Exception {
        LocalCheckpointService checkpoint = getCheckpointService(2, TimeValue.timeValueMillis(100));
        checkpoint.generateSeqNo();
        checkpoint.generateSeqNo();
        try {
            checkpoint.generateSeqNo();
            fail("index operation should time out due to a large lag");
        } catch (EsRejectedExecutionException e) {
            // OK!
        }
    }

    public void testIndexThrottleSuccessReplica() throws Exception {
        LocalCheckpointService checkpoint = getCheckpointService(3, TimeValue.timeValueHours(1));
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final AtomicBoolean threadDone = new AtomicBoolean(false);
        checkpoint.markSeqNoAsCompleted(1);
        Thread backgroundThread = new Thread(() -> {
            threadStarted.countDown();
            checkpoint.markSeqNoAsCompleted(3);
            threadDone.set(true);
        }, "testIndexDelayReplica");
        backgroundThread.start();
        logger.info("--> waiting for thread to start");
        threadStarted.await();
        assertFalse("background thread finished but should have waited", threadDone.get());
        checkpoint.markSeqNoAsCompleted(0);
        logger.info("--> waiting for thread to stop");
        assertBusy(() -> {
            assertTrue("background thread should finished after finishing seq1", threadDone.get());
        });
    }

    public void testIndexThrottleTimeoutReplica() throws Exception {
        LocalCheckpointService checkpoint = getCheckpointService(1, TimeValue.timeValueMillis(100));
        try {
            checkpoint.markSeqNoAsCompleted(1L);
            fail("index operation should time out due to a large lag");
        } catch (EsRejectedExecutionException e) {
            // OK!
        }
        checkpoint.markSeqNoAsCompleted(0L);
        try {
            checkpoint.markSeqNoAsCompleted(2L);
            fail("index operation should time out due to a large lag");
        } catch (EsRejectedExecutionException e) {
            // OK!
        }

    }

    public void testConcurrentPrimary() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final int opsPerThread = randomIntBetween(10, 20);
        final int maxOps = opsPerThread * threads.length;
        final long unFinisshedSeq = randomIntBetween(maxOps - SMALL_INDEX_LAG_THRESHOLD, maxOps - 2); // make sure we won't be blocked
        logger.info("--> will run [{}] threads, maxOps [{}], unfinished seq no [{}]", threads.length, maxOps, unFinisshedSeq);
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        for (int t = 0; t < threads.length; t++) {
            final int threadId = t;
            threads[t] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    throw new ElasticsearchException("failure in background thread", t);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long seqNo = checkpointService.generateSeqNo();
                        logger.info("[t{}] started   [{}]", threadId, seqNo);
                        if (seqNo != unFinisshedSeq) {
                            checkpointService.markSeqNoAsCompleted(seqNo);
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
        assertThat(checkpointService.getMaxSeqNo(), equalTo(maxOps - 1L));
        assertThat(checkpointService.getCheckpoint(), equalTo(unFinisshedSeq - 1L));
        checkpointService.markSeqNoAsCompleted(unFinisshedSeq);
        assertThat(checkpointService.getCheckpoint(), equalTo(maxOps - 1L));
    }

    public void testConcurrentReplica() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final int opsPerThread = randomIntBetween(10, 20);
        final int maxOps = opsPerThread * threads.length;
        final long unFinisshedSeq = randomIntBetween(maxOps - SMALL_INDEX_LAG_THRESHOLD, maxOps - 2); // make sure we won't be blocked
        Set<Integer> seqNoList = new HashSet<>();
        for (int i = 0; i < maxOps; i++) {
            seqNoList.add(i);
        }

        final Integer[][] seqNoPerThread = new Integer[threads.length][];
        for (int t = 0; t < threads.length - 1; t++) {
            int size = Math.min(seqNoList.size(), randomIntBetween(opsPerThread - 4, opsPerThread + 4));
            seqNoPerThread[t] = randomSubsetOf(size, seqNoList).toArray(new Integer[size]);
            Arrays.sort(seqNoPerThread[t]);
            seqNoList.removeAll(Arrays.asList(seqNoPerThread[t]));
        }
        seqNoPerThread[threads.length - 1] = seqNoList.toArray(new Integer[seqNoList.size()]);
        logger.info("--> will run [{}] threads, maxOps [{}], unfinished seq no [{}]", threads.length, maxOps, unFinisshedSeq);
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        for (int t = 0; t < threads.length; t++) {
            final int threadId = t;
            threads[t] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    throw new ElasticsearchException("failure in background thread", t);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    Integer[] ops = seqNoPerThread[threadId];
                    for (int seqNo : ops) {
                        if (seqNo != unFinisshedSeq) {
                            checkpointService.markSeqNoAsCompleted(seqNo);
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
        assertThat(checkpointService.getMaxSeqNo(), equalTo(maxOps - 1L));
        assertThat(checkpointService.getCheckpoint(), equalTo(unFinisshedSeq - 1L));
        checkpointService.markSeqNoAsCompleted(unFinisshedSeq);
        assertThat(checkpointService.getCheckpoint(), equalTo(maxOps - 1L));
    }

}