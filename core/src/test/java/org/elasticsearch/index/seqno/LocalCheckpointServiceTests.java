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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;

public class LocalCheckpointServiceTests extends ESTestCase {

    private LocalCheckpointService checkpointService;

    private final int SMALL_CHUNK_SIZE = 4;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        checkpointService = getCheckpointService();
    }

    private LocalCheckpointService getCheckpointService() {
        return new LocalCheckpointService(
                new ShardId("test", "_na_", 0),
                IndexSettingsModule.newIndexSettings("test",
                        Settings.builder()
                                .put(LocalCheckpointService.SETTINGS_BIT_ARRAYS_SIZE.getKey(), SMALL_CHUNK_SIZE)
                                .build()),
                SequenceNumbersService.NO_OPS_PERFORMED,
                SequenceNumbersService.NO_OPS_PERFORMED);
    }

    public void testSimplePrimary() {
        long seqNo1, seqNo2;
        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.NO_OPS_PERFORMED));
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
        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.NO_OPS_PERFORMED));
        checkpointService.markSeqNoAsCompleted(0L);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        checkpointService.markSeqNoAsCompleted(2L);
        assertThat(checkpointService.getCheckpoint(), equalTo(0L));
        checkpointService.markSeqNoAsCompleted(1L);
        assertThat(checkpointService.getCheckpoint(), equalTo(2L));
    }

    public void testSimpleOverFlow() {
        List<Integer> seqNoList = new ArrayList<>();
        final boolean aligned = randomBoolean();
        final int maxOps = SMALL_CHUNK_SIZE * randomIntBetween(1, 5) + (aligned ? 0 : randomIntBetween(1, SMALL_CHUNK_SIZE - 1));

        for (int i = 0; i < maxOps; i++) {
            seqNoList.add(i);
        }
        Collections.shuffle(seqNoList, random());
        for (Integer seqNo : seqNoList) {
            checkpointService.markSeqNoAsCompleted(seqNo);
        }
        assertThat(checkpointService.checkpoint, equalTo(maxOps - 1L));
        assertThat(checkpointService.processedSeqNo.size(), equalTo(aligned ? 0 : 1));
        assertThat(checkpointService.firstProcessedSeqNo, equalTo(((long) maxOps / SMALL_CHUNK_SIZE) * SMALL_CHUNK_SIZE));
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
                        long seqNo = checkpointService.generateSeqNo();
                        logger.info("[t{}] started   [{}]", threadId, seqNo);
                        if (seqNo != unFinishedSeq) {
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
        assertThat(checkpointService.getCheckpoint(), equalTo(unFinishedSeq - 1L));
        checkpointService.markSeqNoAsCompleted(unFinishedSeq);
        assertThat(checkpointService.getCheckpoint(), equalTo(maxOps - 1L));
        assertThat(checkpointService.processedSeqNo.size(), isOneOf(0, 1));
        assertThat(checkpointService.firstProcessedSeqNo, equalTo(((long) maxOps / SMALL_CHUNK_SIZE) * SMALL_CHUNK_SIZE));
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
                            checkpointService.markSeqNoAsCompleted(seqNo);
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
        assertThat(checkpointService.getMaxSeqNo(), equalTo(maxOps - 1L));
        assertThat(checkpointService.getCheckpoint(), equalTo(unFinishedSeq - 1L));
        checkpointService.markSeqNoAsCompleted(unFinishedSeq);
        assertThat(checkpointService.getCheckpoint(), equalTo(maxOps - 1L));
        assertThat(checkpointService.firstProcessedSeqNo, equalTo(((long) maxOps / SMALL_CHUNK_SIZE) * SMALL_CHUNK_SIZE));
    }

}
