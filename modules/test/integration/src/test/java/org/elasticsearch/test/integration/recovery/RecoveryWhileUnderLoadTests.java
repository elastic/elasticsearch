/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class RecoveryWhileUnderLoadTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadTests.class);

    @AfterMethod public void shutdownNodes() {
        closeAllNodes();
    }

    @Test public void recoverWhileUnderLoadAllocateBackupsTest() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node1").prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 20000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 20000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 40000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 40000 docs indexed");

        logger.info("--> starting [node2] ...");
        // now start another node, while we index
        startNode("node2");

        logger.info("--> waiting for GREEN health status ...");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> waiting for 100000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 100000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 100000 docs indexed");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }

    @Test public void recoverWhileUnderLoadAllocateBackupsRelocatePrimariesTest() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        logger.info("--> starting {} indexing threads", writers.length);
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node1").prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 20000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 20000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 40000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 40000 docs indexed");

        logger.info("--> starting [node2] ...");
        startNode("node2");
        logger.info("--> starting [node3] ...");
        startNode("node3");
        logger.info("--> starting [node4] ...");
        startNode("node4");

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));


        logger.info("--> waiting for 150000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 150000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 150000 docs indexed");

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
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }

    @Test public void recoverWhileUnderLoadWithNodeShutdown() throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");
        logger.info("--> starting [node2] ...");
        startNode("node2");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node2").prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 20000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 20000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 40000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 40000 docs indexed");

        // now start more nodes, while we index
        logger.info("--> starting [node3] ...");
        startNode("node3");
        logger.info("--> starting [node4] ...");
        startNode("node4");

        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));


        logger.info("--> waiting for 100000 docs to be indexed ...");
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 100000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }
        logger.info("--> 100000 docs indexed");

        // now, shutdown nodes
        logger.info("--> shutting down [node1] ...");
        closeNode("node1");
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down [node3] ...");
        closeNode("node3");
        logger.info("--> waiting for GREEN health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down [node4] ...");
        closeNode("node4");
        logger.info("--> waiting for YELLOW health status ...");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("--> refreshing the index");
        client("node2").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verifying indexed content");
        client("node2").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node2").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }
}
