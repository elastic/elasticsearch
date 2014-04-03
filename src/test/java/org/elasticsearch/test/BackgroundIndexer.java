package org.elasticsearch.test;/*
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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.recovery.RecoveryWhileUnderLoadTests;
import org.junit.Assert;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class BackgroundIndexer implements AutoCloseable {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadTests.class);

    final Thread[] writers;
    final CountDownLatch stopLatch;
    final CopyOnWriteArrayList<Throwable> failures;
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong idGenerator = new AtomicLong();
    final AtomicLong indexCounter = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(1);

    public BackgroundIndexer(String index, String type, Client client) {
        this(index, type, client, RandomizedTest.scaledRandomIntBetween(2, 5));
    }

    public BackgroundIndexer(String index, String type, Client client, int writerCount) {
        this(index, type, client, writerCount, true);
    }

    public BackgroundIndexer(final String index, final String type, final Client client, final int writerCount, boolean autoStart) {

        failures = new CopyOnWriteArrayList<>();
        writers = new Thread[writerCount];
        stopLatch = new CountDownLatch(writers.length);
        logger.info("--> starting {} indexing threads", writerCount);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final boolean batch = RandomizedTest.getRandom().nextBoolean();
            writers[i] = new Thread() {
                @Override
                public void run() {
                    long id = -1;
                    try {
                        startLatch.await();
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            if (batch) {
                                int batchSize = RandomizedTest.getRandom().nextInt(20) + 1;
                                BulkRequestBuilder bulkRequest = client.prepareBulk();
                                for (int i = 0; i < batchSize; i++) {
                                    id = idGenerator.incrementAndGet();
                                    bulkRequest.add(client.prepareIndex(index, type, Long.toString(id)).setSource("test", "value" + id));
                                }
                                BulkResponse bulkResponse = bulkRequest.get();
                                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                    if (!bulkItemResponse.isFailed()) {
                                        indexCounter.incrementAndGet();
                                    } else {
                                        throw new ElasticsearchException("bulk request failure, id: ["
                                                + bulkItemResponse.getFailure().getId() + "] message: " + bulkItemResponse.getFailure().getMessage());
                                    }
                                }

                            } else {
                                id = idGenerator.incrementAndGet();
                                client.prepareIndex(index, type, Long.toString(id) + "-" + indexerId).setSource("test", "value" + id).get();
                                indexCounter.incrementAndGet();
                            }
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Throwable e) {
                        failures.add(e);
                        logger.warn("**** failed indexing thread {} on doc id {}", e, indexerId, id);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        if (autoStart) {
            startLatch.countDown();
        }
    }

    public void start() {
        startLatch.countDown();
    }

    public void stop() throws InterruptedException {
        if (stop.get()) {
            return;
        }
        stop.set(true);

        Assert.assertThat("timeout while waiting for indexing threads to stop", stopLatch.await(6, TimeUnit.MINUTES), equalTo(true));
        assertNoFailures();
    }

    public long totalIndexedDocs() {
        return indexCounter.get();
    }

    public Throwable[] getFailures() {
        return failures.toArray(new Throwable[failures.size()]);
    }

    public void assertNoFailures() {
        Assert.assertThat(failures, emptyIterable());
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
