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
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Assert;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class BackgroundIndexer implements AutoCloseable {

    private final ESLogger logger = Loggers.getLogger(getClass());

    final Thread[] writers;
    final CountDownLatch stopLatch;
    final CopyOnWriteArrayList<Throwable> failures;
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong idGenerator = new AtomicLong();
    final AtomicLong indexCounter = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean hasBudget = new AtomicBoolean(false); // when set to true, writers will acquire writes from a semaphore
    final Semaphore availableBudget = new Semaphore(0);

    volatile int minFieldSize = 10;
    volatile int maxFieldSize = 140;

    /**
     * Start indexing in the background using a random number of threads.
     *
     * @param index  index name to index into
     * @param type   document type
     * @param client client to use
     */
    public BackgroundIndexer(String index, String type, Client client) {
        this(index, type, client, -1);
    }

    /**
     * Start indexing in the background using a random number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index     index name to index into
     * @param type      document type
     * @param client    client to use
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public BackgroundIndexer(String index, String type, Client client, int numOfDocs) {
        this(index, type, client, numOfDocs, RandomizedTest.scaledRandomIntBetween(2, 5));
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index       index name to index into
     * @param type        document type
     * @param client      client to use
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     */
    public BackgroundIndexer(String index, String type, Client client, int numOfDocs, final int writerCount) {
        this(index, type, client, numOfDocs, writerCount, true, null);
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index       index name to index into
     * @param type        document type
     * @param client      client to use
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     * @param autoStart   set to true to start indexing as soon as all threads have been created.
     * @param random      random instance to use
     */
    public BackgroundIndexer(final String index, final String type, final Client client, final int numOfDocs, final int writerCount,
                             boolean autoStart, Random random) {

        if (random == null) {
            random = RandomizedTest.getRandom();
        }
        failures = new CopyOnWriteArrayList<>();
        writers = new Thread[writerCount];
        stopLatch = new CountDownLatch(writers.length);
        logger.info("--> creating {} indexing threads (auto start: [{}], numOfDocs: [{}])", writerCount, autoStart, numOfDocs);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final boolean batch = random.nextBoolean();
            final Random threadRandom = new Random(random.nextLong());
            writers[i] = new Thread() {
                @Override
                public void run() {
                    long id = -1;
                    try {
                        startLatch.await();
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            if (batch) {
                                int batchSize = threadRandom.nextInt(20) + 1;
                                if (hasBudget.get()) {
                                    batchSize = Math.max(Math.min(batchSize, availableBudget.availablePermits()), 1);// always try to get at least one
                                    if (!availableBudget.tryAcquire(batchSize, 250, TimeUnit.MILLISECONDS)) {
                                        // time out -> check if we have to stop.
                                        continue;
                                    }

                                }
                                BulkRequestBuilder bulkRequest = client.prepareBulk();
                                for (int i = 0; i < batchSize; i++) {
                                    id = idGenerator.incrementAndGet();
                                    bulkRequest.add(client.prepareIndex(index, type, Long.toString(id)).setSource(generateSource(id, threadRandom)));
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

                                if (hasBudget.get() && !availableBudget.tryAcquire(250, TimeUnit.MILLISECONDS)) {
                                    // time out -> check if we have to stop.
                                    continue;
                                }
                                id = idGenerator.incrementAndGet();
                                client.prepareIndex(index, type, Long.toString(id)).setSource(generateSource(id, threadRandom)).get();
                                indexCounter.incrementAndGet();
                            }
                        }
                        logger.info("**** done indexing thread {}  stop: {} numDocsIndexed: {}", indexerId, stop.get(), indexCounter.get());
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
            start(numOfDocs);
        }
    }

    private XContentBuilder generateSource(long id, Random random) throws IOException {
        int contentLength = RandomInts.randomIntBetween(random, minFieldSize, maxFieldSize);
        StringBuilder text = new StringBuilder(contentLength);
        while (text.length() < contentLength) {
            int tokenLength = RandomInts.randomIntBetween(random, 1, Math.min(contentLength - text.length(), 10));
            text.append(" ").append(RandomStrings.randomRealisticUnicodeOfCodepointLength(random, tokenLength));
        }
        XContentBuilder builder = XContentFactory.smileBuilder();
        builder.startObject().field("test", "value" + id)
                .field("text", text.toString())
                .endObject();
        return builder;

    }

    private void setBudget(int numOfDocs) {
        logger.debug("updating budget to [{}]", numOfDocs);
        if (numOfDocs >= 0) {
            hasBudget.set(true);
            availableBudget.release(numOfDocs);
        } else {
            hasBudget.set(false);
        }

    }

    /** Start indexing with no limit to the number of documents */
    public void start() {
        start(-1);
    }

    /**
     * Start indexing
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void start(int numOfDocs) {
        assert !stop.get() : "background indexer can not be started after it has stopped";
        setBudget(numOfDocs);
        startLatch.countDown();
    }

    /** Pausing indexing by setting current document limit to 0 */
    public void pauseIndexing() {
        availableBudget.drainPermits();
        setBudget(0);
    }

    /** Continue indexing after it has paused. No new document limit will be set */
    public void continueIndexing() {
        continueIndexing(-1);
    }

    /**
     * Continue indexing after it has paused.
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void continueIndexing(int numOfDocs) {
        setBudget(numOfDocs);
    }

    /** Stop all background threads * */
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

    /** the minimum size in code points of a payload field in the indexed documents */
    public void setMinFieldSize(int fieldSize) {
        minFieldSize = fieldSize;
    }

    /** the minimum size in code points of a payload field in the indexed documents */
    public void setMaxFieldSize(int fieldSize) {
        maxFieldSize = fieldSize;
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
