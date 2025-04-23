/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class DataNodeRequestSenderIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testSearchWhileRelocating() throws InterruptedException {
        internalCluster().ensureAtLeastNumDataNodes(3);
        var primaries = randomIntBetween(1, 10);
        var replicas = randomIntBetween(0, 1);
        var docs = randomIntBetween(10, 100);

        indicesAdmin().prepareCreate("index-1").setSettings(indexSettings(primaries, replicas)).get();
        indexAndRefresh("index-1", docs);

        // start background searches
        var stopped = new AtomicBoolean(false);
        var queries = new LongAdder();
        var threads = new Thread[randomIntBetween(1, 5)];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false) {
                    try (EsqlQueryResponse resp = run("FROM index-1")) {
                        assertThat(getValuesList(resp), hasSize(docs));
                    }
                    queries.increment();
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }

        // start shard movements
        var rounds = randomIntBetween(1, 10);
        var names = internalCluster().getNodeNames();
        for (int i = 0; i < rounds; i++) {
            for (String name : names) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", name))
                    .get();
                ensureGreen("index-1");
                Thread.yield();
            }
        }

        stopped.set(true);
        for (Thread thread : threads) {
            thread.join(10_000);
        }

        client().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name"))
            .get();
        assertThat(queries.sum(), greaterThan((long) threads.length));
    }

    public void testRetryOnShardMovement() {
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(client().admin().indices().prepareCreate("index-1").setSettings(indexSettings(1, 0)));
        assertAcked(client().admin().indices().prepareCreate("index-2").setSettings(indexSettings(1, 0)));

        indexAndRefresh("index-1", 1);
        indexAndRefresh("index-2", 1);

        var shouldMove = new AtomicBoolean(true);

        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            as(transportService, MockTransportService.class).addRequestHandlingBehavior(
                ExchangeService.OPEN_EXCHANGE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    // move index shard
                    if (shouldMove.compareAndSet(true, false)) {
                        assertAcked(
                            client().admin()
                                .indices()
                                .prepareUpdateSettings("index-1")
                                .setSettings(
                                    Settings.builder().put("index.routing.allocation.exclude._name", getPrimaryShardNodeName("index-1", 0))
                                )
                        );
                        ensureGreen("index-1");
                    }
                    // execute data node request
                    handler.messageReceived(request, channel, task);
                }
            );
        }

        try (EsqlQueryResponse resp = run("FROM " + randomFrom("index-1,index-2", "index-*"))) {
            assertThat(getValuesList(resp), hasSize(2));
        }
    }

    public void testDiscardUnnecessaryRequests() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        var index = randomIdentifier();

        assertAcked(client().admin().indices().prepareCreate(index).setSettings(indexSettings(2, 0)));
        indexAndRefresh(index, 10);

        var slowNode = getPrimaryShardNodeName(index, 1);

        var queryCompleted = new CountDownLatch(1);
        var blockIsReleased = new AtomicReference<Boolean>(null);

        as(internalCluster().getInstance(TransportService.class, slowNode), MockTransportService.class).addRequestHandlingBehavior(
            ExchangeService.OPEN_EXCHANGE_ACTION_NAME,
            (handler, request, channel, task) -> {
                blockIsReleased.set(queryCompleted.await(10, TimeUnit.SECONDS));
                handler.messageReceived(request, channel, task);
            }
        );

        try (EsqlQueryResponse resp = run("FROM " + index + " | LIMIT 1")) {
            assertThat(getValuesList(resp), hasSize(1));
        }

        // This unblocks slow node _after_ the query supposedly complete by results from the other node
        try {
            queryCompleted.countDown();
            assertBusy(() -> assertThat(blockIsReleased.get(), equalTo(Boolean.TRUE)));
        } finally {
            as(internalCluster().getInstance(TransportService.class, slowNode), MockTransportService.class).clearAllRules();
        }
    }

    private static void indexAndRefresh(String index, int docs) {
        var bulk = client().prepareBulk(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(new IndexRequest().source("key", "value-" + i));
        }
        bulk.get();
    }

    private String getPrimaryShardNodeName(String index, int shardId) {
        var clusterState = clusterService().state();
        var nodeId = clusterState.getRoutingTable().shardRoutingTable(index, shardId).primaryShard().currentNodeId();
        return clusterState.nodes().get(nodeId).getName();
    }
}
