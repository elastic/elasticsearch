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

import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class RelocationTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(RelocationTests.class);

    @AfterMethod
    public void shutdownNodes() {
        closeAllNodes();
    }

    @Test
    public void testSimpleRelocationNoIndexing() {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .execute().actionGet();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client("node1").prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have an actual index");
        client("node1").admin().indices().prepareFlush().execute().actionGet();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client("node1").prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        logger.info("--> verifying count");
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client("node1").prepareCount("test").execute().actionGet().count(), equalTo(20l));

        logger.info("--> start another node");
        startNode("node2");
        ClusterHealthResponse clusterHealthResponse = client("node2").admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.timedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        client("node1").admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(new ShardId("test", 0), "node1", "node2"))
                .execute().actionGet();

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(clusterHealthResponse.timedOut(), equalTo(false));
        clusterHealthResponse = client("node2").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(clusterHealthResponse.timedOut(), equalTo(false));

        logger.info("--> verifying count again...");
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client("node1").prepareCount("test").execute().actionGet().count(), equalTo(20l));
    }

    @Test
    public void testPrimaryRelocationWhileIndexingWith1RelocationAnd1Writer() throws Exception {
        testPrimaryRelocationWhileIndexing(1, 1, false);
    }

    @Test
    public void testPrimaryRelocationWhileIndexingWith5RelocationAnd1Writer() throws Exception {
        testPrimaryRelocationWhileIndexing(5, 1, false);
    }

    @Test
    public void testPrimaryRelocationWhileIndexingWith10RelocationAnd5Writers() throws Exception {
        testPrimaryRelocationWhileIndexing(10, 5, false);
    }

    @Test
    public void testPrimaryRelocationWhileBulkIndexingWith1RelocationAnd1Writer() throws Exception {
        testPrimaryRelocationWhileIndexing(1, 1, true);
    }

    @Test
    public void testPrimaryRelocationWhileBulkIndexingWith10RelocationAnd1Writer() throws Exception {
        testPrimaryRelocationWhileIndexing(10, 1, true);
    }

    @Test
    public void testPrimaryRelocationWhileBulkIndexingWith10RelocationAnd5Writers() throws Exception {
        testPrimaryRelocationWhileIndexing(10, 5, true);
    }

    private void testPrimaryRelocationWhileIndexing(final int numberOfRelocations, final int numberOfWriters, final boolean batch) throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                ).execute().actionGet();

        logger.info("--> starting [node2] ...");
        startNode("node2");

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[numberOfWriters];
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
                            if (batch) {
                                BulkRequestBuilder bulkRequest = client("node1").prepareBulk();
                                for (int i = 0; i < 100; i++) {
                                    long id = idGenerator.incrementAndGet();
                                    if (id % 1000 == 0) {
                                        client("node1").admin().indices().prepareFlush().execute().actionGet();
                                    }
                                    bulkRequest.add(client("node1").prepareIndex("test", "type1", Long.toString(id))
                                            .setSource("test", "value" + id));
                                }
                                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                    if (!bulkItemResponse.failed()) {
                                        indexCounter.incrementAndGet();
                                    } else {
                                        logger.warn("**** failed bulk indexing thread {}, {}/{}", indexerId, bulkItemResponse.failure().getId(), bulkItemResponse.failure().getMessage());
                                    }
                                }
                            } else {
                                long id = idGenerator.incrementAndGet();
                                if (id % 1000 == 0) {
                                    client("node1").admin().indices().prepareFlush().execute().actionGet();
                                }
                                client("node1").prepareIndex("test", "type1", Long.toString(id))
                                        .setSource("test", "value" + id).execute().actionGet();
                                indexCounter.incrementAndGet();
                            }
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

        logger.info("--> starting relocations...");
        for (int i = 0; i < numberOfRelocations; i++) {
            String fromNode = "node" + (1 + (i % 2));
            String toNode = "node1".equals(fromNode) ? "node2" : "node1";
            logger.info("--> START relocate the shard from {} to {}", fromNode, toNode);
            client("node1").admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), fromNode, toNode))
                    .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(clusterHealthResponse.timedOut(), equalTo(false));
            clusterHealthResponse = client("node2").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(clusterHealthResponse.timedOut(), equalTo(false));
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
        }
        logger.info("--> done relocations");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client("node1").admin().indices().prepareRefresh("test").execute().actionGet();
        logger.info("--> searching the index");
        boolean ranOnce = false;
        for (int i = 0; i < 10; i++) {
            try {
                logger.info("--> START search test round {}", i + 1);
                SearchHits hits = client("node1").prepareSearch("test").setQuery(matchAllQuery()).setSize((int) indexCounter.get()).setNoFields().execute().actionGet().hits();
                ranOnce = true;
                if (hits.totalHits() != indexCounter.get()) {
                    int[] hitIds = new int[(int) indexCounter.get()];
                    for (int hit = 0; hit < indexCounter.get(); hit++) {
                        hitIds[hit] = hit + 1;
                    }
                    TIntSet set = new TIntHashSet(hitIds);
                    for (SearchHit hit : hits.hits()) {
                        int id = Integer.parseInt(hit.id());
                        if (!set.remove(id)) {
                            logger.error("Extra id [{}]", id);
                        }
                    }
                    set.forEach(new TIntProcedure() {
                        @Override
                        public boolean execute(int value) {
                            logger.error("Missing id [{}]", value);
                            return true;
                        }
                    });
                }
                assertThat(hits.totalHits(), equalTo(indexCounter.get()));
                logger.info("--> DONE search test round {}", i + 1);
            } catch (SearchPhaseExecutionException ex) {
                // TODO: the first run fails with this failure, waiting for relocating nodes set to 0 is not enough?
                logger.warn("Got exception while searching.", ex);
            }
        }
        if (!ranOnce) {
            assert false;
        }
    }

    @Test
    public void testReplicaRelocationWhileIndexingWith1RelocationAnd1Writer() throws Exception {
        testReplicaRelocationWhileIndexing(1, 1, false);
    }

    @Test
    public void testReplicaRelocationWhileIndexingWith5RelocationAnd1Writer() throws Exception {
        testReplicaRelocationWhileIndexing(5, 1, false);
    }

    @Test
    public void testReplicaRelocationWhileIndexingWith10RelocationAnd5Writers() throws Exception {
        testReplicaRelocationWhileIndexing(10, 5, false);
    }

    @Test
    public void testReplicaRelocationWhileBulkIndexingWith1RelocationAnd1Writer() throws Exception {
        testReplicaRelocationWhileIndexing(1, 1, true);
    }

    @Test
    public void testReplicaRelocationWhileBulkIndexingWith10RelocationAnd1Writer() throws Exception {
        testReplicaRelocationWhileIndexing(10, 1, true);
    }

    @Test
    public void testReplicaRelocationWhileBulkIndexingWith10RelocationAnd5Writers() throws Exception {
        testReplicaRelocationWhileIndexing(10, 5, true);
    }

    private void testReplicaRelocationWhileIndexing(final int numberOfRelocations, final int numberOfWriters, final boolean batch) throws Exception {
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                ).execute().actionGet();

        logger.info("--> starting [node2] ...");
        startNode("node2");

        ClusterHealthResponse healthResponse = client("node2").admin().cluster().prepareHealth().setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.timedOut(), equalTo(false));

        logger.info("--> starting [node3] ...");
        startNode("node3");

        healthResponse = client("node3").admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.timedOut(), equalTo(false));

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[numberOfWriters];
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
                            if (batch) {
                                BulkRequestBuilder bulkRequest = client("node1").prepareBulk();
                                for (int i = 0; i < 100; i++) {
                                    long id = idGenerator.incrementAndGet();
                                    if (id % 1000 == 0) {
                                        client("node1").admin().indices().prepareFlush().execute().actionGet();
                                    }
                                    bulkRequest.add(client("node1").prepareIndex("test", "type1", Long.toString(id))
                                            .setSource("test", "value" + id));
                                }
                                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                    if (!bulkItemResponse.failed()) {
                                        indexCounter.incrementAndGet();
                                    } else {
                                        logger.warn("**** failed bulk indexing thread {}, {}/{}", indexerId, bulkItemResponse.failure().getId(), bulkItemResponse.failure().getMessage());
                                    }
                                }
                            } else {
                                long id = idGenerator.incrementAndGet();
                                if (id % 1000 == 0) {
                                    client("node1").admin().indices().prepareFlush().execute().actionGet();
                                }
                                client("node1").prepareIndex("test", "type1", Long.toString(id))
                                        .setSource("test", "value" + id).execute().actionGet();
                                indexCounter.incrementAndGet();
                            }
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

        logger.info("--> starting relocations...");
        for (int i = 0; i < numberOfRelocations; i++) {
            String fromNode = "node" + (2 + (i % 2));
            String toNode = "node2".equals(fromNode) ? "node3" : "node2";
            logger.info("--> START relocate the shard from {} to {}", fromNode, toNode);
            client("node1").admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), fromNode, toNode))
                    .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(clusterHealthResponse.timedOut(), equalTo(false));
            clusterHealthResponse = client("node2").admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(clusterHealthResponse.timedOut(), equalTo(false));
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
        }
        logger.info("--> done relocations");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        client("node1").admin().indices().prepareRefresh("test").execute().actionGet();
        logger.info("--> searching the index");
        boolean ranOnce = false;
        for (int i = 0; i < 10; i++) {
            try {
                logger.info("--> START search test round {}", i + 1);
                SearchHits hits = client("node1").prepareSearch("test").setQuery(matchAllQuery()).setSize((int) indexCounter.get()).setNoFields().execute().actionGet().hits();
                ranOnce = true;
                if (hits.totalHits() != indexCounter.get()) {
                    int[] hitIds = new int[(int) indexCounter.get()];
                    for (int hit = 0; hit < indexCounter.get(); hit++) {
                        hitIds[hit] = hit + 1;
                    }
                    TIntSet set = new TIntHashSet(hitIds);
                    for (SearchHit hit : hits.hits()) {
                        int id = Integer.parseInt(hit.id());
                        if (!set.remove(id)) {
                            logger.error("Extra id [{}]", id);
                        }
                    }
                    set.forEach(new TIntProcedure() {
                        @Override
                        public boolean execute(int value) {
                            logger.error("Missing id [{}]", value);
                            return true;
                        }
                    });
                }
                assertThat(hits.totalHits(), equalTo(indexCounter.get()));
                logger.info("--> DONE search test round {}", i + 1);
            } catch (SearchPhaseExecutionException ex) {
                // TODO: the first run fails with this failure, waiting for relocating nodes set to 0 is not enough?
                logger.warn("Got exception while searching.", ex);
            }
        }
        if (!ranOnce) {
            assert false;
        }
    }
}
