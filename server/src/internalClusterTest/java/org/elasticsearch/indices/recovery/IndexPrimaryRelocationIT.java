/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexPrimaryRelocationIT extends ESIntegTestCase {

    private static final int RELOCATION_COUNT = 15;

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(2, 3));
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping("field", "type=text").get();
        ensureGreen("test");
        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false && numAutoGenDocs.get() < 10_000) {
                    DocWriteResponse indexResponse = prepareIndex("test").setId("id").setSource("field", "value").get();
                    assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    DeleteResponse deleteResponse = client().prepareDelete("test", "id").get();
                    assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                    prepareIndex("test").setSource("auto", true).get();
                    numAutoGenDocs.incrementAndGet();
                }
            }
        };
        indexingThread.start();

        ClusterState initialState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        DiscoveryNode[] dataNodes = initialState.getNodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        DiscoveryNode relocationSource = initialState.getNodes()
            .getDataNodes()
            .get(initialState.getRoutingTable().shardRoutingTable("test", 0).primaryShard().currentNodeId());
        for (int i = 0; i < RELOCATION_COUNT; i++) {
            DiscoveryNode relocationTarget = randomFrom(dataNodes);
            while (relocationTarget.equals(relocationSource)) {
                relocationTarget = randomFrom(dataNodes);
            }
            logger.info("--> [iteration {}] relocating from {} to {} ", i, relocationSource.getName(), relocationTarget.getName());
            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("test", 0, relocationSource.getId(), relocationTarget.getId()));
            ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                .setTimeout(TimeValue.timeValueSeconds(60))
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .get();
            if (clusterHealthResponse.isTimedOut()) {
                HotThreads.logLocalHotThreads(
                    logger,
                    Level.INFO,
                    "timed out waiting for relocation iteration [" + i + "]",
                    ReferenceDocs.LOGGING
                );
                final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
                logger.info("timed out for waiting for relocation iteration [{}] \ncluster state {}", i, clusterState);
                finished.set(true);
                indexingThread.join();
                throw new AssertionError("timed out waiting for relocation iteration [" + i + "] ");
            }
            logger.info("--> [iteration {}] relocation complete", i);
            relocationSource = relocationTarget;
            // indexing process aborted early, no need for more relocations as test has already failed
            if (indexingThread.isAlive() == false) {
                break;
            }
            if (i > 0 && i % 5 == 0) {
                logger.info("--> [iteration {}] flushing index", i);
                indicesAdmin().prepareFlush("test").get();
            }
        }
        finished.set(true);
        indexingThread.join();
        refresh("test");
        ElasticsearchAssertions.assertHitCount(
            numAutoGenDocs.get(),
            prepareSearch("test").setTrackTotalHits(true),
            prepareSearch("test").setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
        );
    }

}
