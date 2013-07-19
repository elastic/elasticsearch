/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.recovery;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class RecoveryWhileUnderLoadTests extends AbstractSharedClusterTest {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadTests.class);

    @Test
    @Slow
    public void recoverWhileUnderLoadAllocateBackupsTest() throws Exception {
        logger.info("--> creating test index ...");
        prepareCreate("test", 1);

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final Client client = client();
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            if (id % 1000 == 0) {
                                client.admin().indices().prepareFlush().execute().actionGet();
                            }
                            client.prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 2000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 4000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");

        logger.info("--> allow 2 nodes for index [test] ...");
        // now start another node, while we index
        allowNodes("test", 2);

        logger.info("--> waiting for GREEN health status ...");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> waiting for 10000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 10000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 10000 docs indexed");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(indexCounter.get()));
        }
    }

    @Test
    @Slow
    public void recoverWhileUnderLoadAllocateBackupsRelocatePrimariesTest() throws Exception {
        logger.info("--> creating test index ...");
        prepareCreate("test", 1);

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        logger.info("--> starting {} indexing threads", writers.length);
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final Client client = client();
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client.prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 2000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 4000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");
        logger.info("--> allow 4 nodes for index [test] ...");
        allowNodes("test", 4);

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=4").execute().actionGet().isTimedOut(), equalTo(false));


        logger.info("--> waiting for 15000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 15000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 15000 docs indexed");

        stop.set(true);
        stopLatch.await();

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(indexCounter.get()));
        }
    }

    @Test
    @Slow
    public void recoverWhileUnderLoadWithNodeShutdown() throws Exception {
        logger.info("--> creating test index ...");
        prepareCreate("test", 2);

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final Client client = client();
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client.prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 2000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 4000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");

        // now start more nodes, while we index
        logger.info("--> allow 4 nodes for index [test] ...");
        allowNodes("test", 4);

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=4").execute().actionGet().isTimedOut(), equalTo(false));


        logger.info("--> waiting for 10000 docs to be indexed ...");
        while (client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() < 10000) {
            Thread.sleep(100);
            client().admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 10000 docs indexed");

        // now, shutdown nodes
        logger.info("--> allow 3 nodes for index [test] ...");
        allowNodes("test", 3);
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=3").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> allow 2 nodes for index [test] ...");
        allowNodes("test", 2);
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=2").execute().actionGet().isTimedOut(), equalTo(false));
        
        logger.info("--> allow 1 nodes for index [test] ...");
        allowNodes("test", 1);
        logger.info("--> waiting for YELLOW health status ...");
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForNodes(">=1").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForNodes(">=1").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> refreshing the index");
        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(indexCounter.get()));
        }
    }
}
