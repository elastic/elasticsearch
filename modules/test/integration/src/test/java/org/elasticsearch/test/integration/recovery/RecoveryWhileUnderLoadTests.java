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

    @AfterMethod public void shutdownNodes() {
        closeAllNodes();
    }

    @Test public void recoverWhileUnderLoadAllocateBackupsTest() throws Exception {
        startNode("node1");

        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            writers[i] = new Thread() {
                @Override public void run() {
                    while (!stop.get()) {
                        long id = idGenerator.incrementAndGet();
                        client("node1").prepareIndex("test", "type1", Long.toString(id))
                                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                    }
                    stopLatch.countDown();
                }
            };
            writers[i].start();
        }

        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now start another node, while we index
        startNode("server2");

        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));


        // wait till we index 10,0000
        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 100000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        stop.set(true);
        stopLatch.await();

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }

    @Test public void recoverWhileUnderLoadAllocateBackupsRelocatePrimariesTest() throws Exception {
        startNode("node1");

        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            writers[i] = new Thread() {
                @Override public void run() {
                    while (!stop.get()) {
                        long id = idGenerator.incrementAndGet();
                        client("node1").prepareIndex("test", "type1", Long.toString(id))
                                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                    }
                    stopLatch.countDown();
                }
            };
            writers[i].start();
        }

        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now start another node, while we index
        startNode("node2");
        startNode("node3");
        startNode("node4");

        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));


        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 150000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        stop.set(true);
        stopLatch.await();

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }

    @Test public void recoverWhileUnderLoadWithNodeShutdown() throws Exception {
        startNode("node1");
        startNode("node2");

        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            writers[i] = new Thread() {
                @Override public void run() {
                    try {
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client("node2").prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                        }
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 20000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now flush, just to make sure we have some data in the index, not just translog
        client("node1").admin().indices().prepareFlush().execute().actionGet();


        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 40000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now start nore nodes, while we index
        startNode("node3");
        startNode("node4");

        assertThat(client("node1").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));


        while (client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count() < 80000) {
            Thread.sleep(100);
            client("node1").admin().indices().prepareRefresh().execute().actionGet();
        }

        // now, shutdown nodes
        closeNode("node1");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));
        closeNode("node3");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForGreenStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.GREEN));
        closeNode("node4");
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.YELLOW));

        stop.set(true);
        stopLatch.await();
        assertThat(client("node2").admin().cluster().prepareHealth().setTimeout("1m").setWaitForYellowStatus().execute().actionGet().status(), equalTo(ClusterHealthStatus.YELLOW));

        client("node2").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node2").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(idGenerator.get()));
        }
    }
}
