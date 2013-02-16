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

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class RecoveryWhileUnderLoadTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadTests.class);

    @AfterMethod
    public void shutdownNodes() {
        closeAllNodes();
    }

    @Test
    public void recoverWhileUnderLoadAllocateBackupsTest() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            if (id % 1000 == 0) {
                                client("node1").admin().indices().prepareFlush().execute().actionGet();
                            }
                            client("node1").prepareIndex("test", "type1", Long.toString(id))
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
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 2000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 4000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");

        logger.info("--> starting [node2] ...");
        // now start another node, while we index
        startNode("node2");

        logger.info("--> waiting for GREEN health status ...");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> waiting for 10000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 10000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 10000 docs indexed");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(indexCounter.get()));
        }
    }

    @Test
    public void recoverWhileUnderLoadAllocateBackupsRelocatePrimariesTest() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        logger.info("--> starting {} indexing threads", writers.length);
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node1").prepareIndex("test", "type1", Long.toString(id))
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
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 2000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 4000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");

        logger.info("--> starting [node2] ...");
        startNode("node2");
        logger.info("--> starting [node3] ...");
        startNode("node3");
        logger.info("--> starting [node4] ...");
        startNode("node4");

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().setWaitForNodes("4").execute().actionGet().isTimedOut(), equalTo(false));


        logger.info("--> waiting for 15000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 15000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 15000 docs indexed");

        stop.set(true);
        stopLatch.await();

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(indexCounter.get()));
        }
    }

    @Test
    public void recoverWhileUnderLoadWithNodeShutdown() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");
        logger.info("--> starting [node2] ...");
        startNode("node2");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node2").prepareIndex("test", "type1", Long.toString(id))
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
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 2000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 4000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 4000 docs indexed");

        // now start more nodes, while we index
        logger.info("--> starting [node3] ...");
        startNode("node3");
        logger.info("--> starting [node4] ...");
        startNode("node4");

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().setWaitForNodes("4").execute().actionGet().isTimedOut(), equalTo(false));


        logger.info("--> waiting for 10000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 10000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 10000 docs indexed");

        // now, shutdown nodes
        logger.info("--> shutting down [node1] ...");
        closeNode("node1");
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().setWaitForNodes("3").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> shutting down [node3] ...");
        closeNode("node3");
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> shutting down [node4] ...");
        closeNode("node4");
        logger.info("--> waiting for YELLOW health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().setWaitForNodes("1").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().setWaitForNodes("1").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> refreshing the index");
        client("node2").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        client("node2").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node2").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(indexCounter.get()));
        }
    }
}
