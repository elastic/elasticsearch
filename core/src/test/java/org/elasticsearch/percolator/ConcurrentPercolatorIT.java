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
package org.elasticsearch.percolator;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.percolator.PercolatorIT.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;


/**
 *
 */
public class ConcurrentPercolatorIT extends ESIntegTestCase {

    @Test
    public void testSimpleConcurrentPercolator() throws Exception {
        // We need to index a document / define mapping, otherwise field1 doesn't get reconized as number field.
        // If we don't do this, then 'test2' percolate query gets parsed as a TermQuery and not a RangeQuery.
        // The percolate api doesn't parse the doc if no queries have registered, so it can't lazily create a mapping
        assertAcked(prepareCreate("index").addMapping("type", "field1", "type=long", "field2", "type=string")); // random # shards better has a mapping!
        ensureGreen();

        final BytesReference onlyField1 = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .endObject().endObject().bytes();
        final BytesReference onlyField2 = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field2", "value")
                .endObject().endObject().bytes();
        final BytesReference bothFields = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject().bytes();

        client().prepareIndex("index", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .field("field1", 1)
                .field("field2", "value")
                .endObject()).execute().actionGet();

        client().prepareIndex("index", PercolatorService.TYPE_NAME, "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();
        client().prepareIndex("index", PercolatorService.TYPE_NAME, "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();
        refresh(); // make sure it's refreshed

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger counts = new AtomicInteger(0);
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        Thread[] threads = new Thread[scaledRandomIntBetween(2, 5)];
        final int numberOfPercolations = scaledRandomIntBetween(1000, 10000);

        for (int i = 0; i < threads.length; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        while (!stop.get()) {
                            int count = counts.incrementAndGet();
                            if ((count > numberOfPercolations)) {
                                stop.set(true);
                            }
                            PercolateResponse percolate;
                            if (count % 3 == 0) {
                                percolate = client().preparePercolate().setIndices("index").setDocumentType("type")
                                        .setSource(bothFields)
                                        .execute().actionGet();
                                assertThat(percolate.getMatches(), arrayWithSize(2));
                                assertThat(convertFromTextArray(percolate.getMatches(), "index"), arrayContainingInAnyOrder("test1", "test2"));
                            } else if (count % 3 == 1) {
                                percolate = client().preparePercolate().setIndices("index").setDocumentType("type")
                                        .setSource(onlyField2)
                                        .execute().actionGet();
                                assertThat(percolate.getMatches(), arrayWithSize(1));
                                assertThat(convertFromTextArray(percolate.getMatches(), "index"), arrayContaining("test1"));
                            } else {
                                percolate = client().preparePercolate().setIndices("index").setDocumentType("type")
                                        .setSource(onlyField1)
                                        .execute().actionGet();
                                assertThat(percolate.getMatches(), arrayWithSize(1));
                                assertThat(convertFromTextArray(percolate.getMatches(), "index"), arrayContaining("test2"));
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        exceptionHolder.set(e);
                        Thread.currentThread().interrupt();
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        Throwable assertionError = exceptionHolder.get();
        if (assertionError != null) {
            assertionError.printStackTrace();
        }
        assertThat(assertionError + " should be null", assertionError, nullValue());
    }

    @Test
    public void testConcurrentAddingAndPercolating() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "field1", "type=string", "field2", "type=string"));
        ensureGreen();
        final int numIndexThreads = scaledRandomIntBetween(1, 3);
        final int numPercolateThreads = scaledRandomIntBetween(2, 6);
        final int numPercolatorOperationsPerThread = scaledRandomIntBetween(100, 1000);

        final Set<Throwable> exceptionsHolder = ConcurrentCollections.newConcurrentSet();
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger runningPercolateThreads = new AtomicInteger(numPercolateThreads);
        final AtomicInteger type1 = new AtomicInteger();
        final AtomicInteger type2 = new AtomicInteger();
        final AtomicInteger type3 = new AtomicInteger();

        final AtomicInteger idGen = new AtomicInteger();

        Thread[] indexThreads = new Thread[numIndexThreads];
        for (int i = 0; i < numIndexThreads; i++) {
            final Random rand = new Random(getRandom().nextLong());
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        XContentBuilder onlyField1 = XContentFactory.jsonBuilder().startObject()
                                .field("query", termQuery("field1", "value")).endObject();
                        XContentBuilder onlyField2 = XContentFactory.jsonBuilder().startObject()
                                .field("query", termQuery("field2", "value")).endObject();
                        XContentBuilder field1And2 = XContentFactory.jsonBuilder().startObject()
                                .field("query", boolQuery().must(termQuery("field1", "value")).must(termQuery("field2", "value"))).endObject();

                        start.await();
                        while (runningPercolateThreads.get() > 0) {
                            Thread.sleep(100);
                            int x = rand.nextInt(3);
                            String id = Integer.toString(idGen.incrementAndGet());
                            IndexResponse response;
                            switch (x) {
                                case 0:
                                    response = client().prepareIndex("index", PercolatorService.TYPE_NAME, id)
                                            .setSource(onlyField1)
                                            .execute().actionGet();
                                    type1.incrementAndGet();
                                    break;
                                case 1:
                                    response = client().prepareIndex("index", PercolatorService.TYPE_NAME, id)
                                            .setSource(onlyField2)
                                            .execute().actionGet();
                                    type2.incrementAndGet();
                                    break;
                                case 2:
                                    response = client().prepareIndex("index", PercolatorService.TYPE_NAME, id)
                                            .setSource(field1And2)
                                            .execute().actionGet();
                                    type3.incrementAndGet();
                                    break;
                                default:
                                    throw new IllegalStateException("Illegal x=" + x);
                            }
                            assertThat(response.getId(), equalTo(id));
                            assertThat(response.getVersion(), equalTo(1l));
                        }
                    } catch (Throwable t) {
                        exceptionsHolder.add(t);
                        logger.error("Error in indexing thread...", t);
                    }
                }
            };
            indexThreads[i] = new Thread(r);
            indexThreads[i].start();
        }

        Thread[] percolateThreads = new Thread[numPercolateThreads];
        for (int i = 0; i < numPercolateThreads; i++) {
            final Random rand = new Random(getRandom().nextLong());
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        XContentBuilder onlyField1Doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                                .field("field1", "value")
                                .endObject().endObject();
                        XContentBuilder onlyField2Doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                                .field("field2", "value")
                                .endObject().endObject();
                        XContentBuilder field1AndField2Doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                                .field("field1", "value")
                                .field("field2", "value")
                                .endObject().endObject();
                        start.await();
                        for (int counter = 0; counter < numPercolatorOperationsPerThread; counter++) {
                            int x = rand.nextInt(3);
                            int atLeastExpected;
                            PercolateResponse response;
                            switch (x) {
                                case 0:
                                    atLeastExpected = type1.get();
                                    response = client().preparePercolate().setIndices("index").setDocumentType("type")
                                            .setSource(onlyField1Doc).execute().actionGet();
                                    assertNoFailures(response);
                                    assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                                    assertThat(response.getMatches().length, greaterThanOrEqualTo(atLeastExpected));
                                    break;
                                case 1:
                                    atLeastExpected = type2.get();
                                    response = client().preparePercolate().setIndices("index").setDocumentType("type")
                                            .setSource(onlyField2Doc).execute().actionGet();
                                    assertNoFailures(response);
                                    assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                                    assertThat(response.getMatches().length, greaterThanOrEqualTo(atLeastExpected));
                                    break;
                                case 2:
                                    atLeastExpected = type3.get();
                                    response = client().preparePercolate().setIndices("index").setDocumentType("type")
                                            .setSource(field1AndField2Doc).execute().actionGet();
                                    assertNoFailures(response);
                                    assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                                    assertThat(response.getMatches().length, greaterThanOrEqualTo(atLeastExpected));
                                    break;
                            }
                        }
                    } catch (Throwable t) {
                        exceptionsHolder.add(t);
                        logger.error("Error in percolate thread...", t);
                    } finally {
                        runningPercolateThreads.decrementAndGet();
                    }
                }
            };
            percolateThreads[i] = new Thread(r);
            percolateThreads[i].start();
        }

        start.countDown();
        for (Thread thread : indexThreads) {
            thread.join();
        }
        for (Thread thread : percolateThreads) {
            thread.join();
        }

        for (Throwable t : exceptionsHolder) {
            logger.error("Unexpected exception {}", t.getMessage(), t);
        }
        assertThat(exceptionsHolder.isEmpty(), equalTo(true));
    }

    @Test
    public void testConcurrentAddingAndRemovingWhilePercolating() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "field1", "type=string"));
        ensureGreen();
        final int numIndexThreads = scaledRandomIntBetween(1, 3);
        final int numberPercolateOperation = scaledRandomIntBetween(10, 100);

        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
        final AtomicInteger idGen = new AtomicInteger(0);
        final Set<String> liveIds = ConcurrentCollections.newConcurrentSet();
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread[] indexThreads = new Thread[numIndexThreads];
        final Semaphore semaphore = new Semaphore(numIndexThreads, true);
        for (int i = 0; i < indexThreads.length; i++) {
            final Random rand = new Random(getRandom().nextLong());
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                                .field("query", termQuery("field1", "value")).endObject();
                        outer:
                        while (run.get()) {
                            semaphore.acquire();
                            try {
                                if (!liveIds.isEmpty() && rand.nextInt(100) < 19) {
                                    String id;
                                    do {
                                        if (liveIds.isEmpty()) {
                                            continue outer;
                                        }
                                        id = Integer.toString(randomInt(idGen.get()));
                                    } while (!liveIds.remove(id));

                                    DeleteResponse response = client().prepareDelete("index", PercolatorService.TYPE_NAME, id)
                                            .execute().actionGet();
                                    assertThat(response.getId(), equalTo(id));
                                    assertThat("doc[" + id + "] should have been deleted, but isn't", response.isFound(), equalTo(true));
                                } else {
                                    String id = Integer.toString(idGen.getAndIncrement());
                                    IndexResponse response = client().prepareIndex("index", PercolatorService.TYPE_NAME, id)
                                            .setSource(doc)
                                            .execute().actionGet();
                                    liveIds.add(id);
                                    assertThat(response.isCreated(), equalTo(true)); // We only add new docs
                                    assertThat(response.getId(), equalTo(id));
                                }
                            } finally {
                                semaphore.release();
                            }
                        }
                    } catch (InterruptedException iex) {
                        logger.error("indexing thread was interrupted...", iex);
                        run.set(false);
                    } catch (Throwable t) {
                        run.set(false);
                        exceptionHolder.set(t);
                        logger.error("Error in indexing thread...", t);
                    }
                }
            };
            indexThreads[i] = new Thread(r);
            indexThreads[i].start();
        }

        XContentBuilder percolateDoc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", "value")
                .endObject().endObject();
        for (int counter = 0; counter < numberPercolateOperation; counter++) {
            Thread.sleep(5);
            semaphore.acquire(numIndexThreads);
            try {
                if (!run.get()) {
                    break;
                }
                int atLeastExpected = liveIds.size();
                PercolateResponse response = client().preparePercolate().setIndices("index").setDocumentType("type")
                        .setSource(percolateDoc).execute().actionGet();
                assertThat(response.getShardFailures(), emptyArray());
                assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                assertThat(response.getMatches().length, equalTo(atLeastExpected));
            } finally {
                semaphore.release(numIndexThreads);
            }
        }
        run.set(false);
        for (Thread thread : indexThreads) {
            thread.join();
        }
        assertThat("exceptionHolder should have been empty, but holds: " + exceptionHolder.toString(), exceptionHolder.get(), nullValue());
    }

}
