/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.DefaultNonPreferredShardIteratorFactory.LazilyExpandingShardIterator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DefaultNonPreferredShardIteratorFactoryTests extends ESTestCase {

    public void testLazilyExpandingIterator() {
        final List<List<String>> allValues = new ArrayList<>();
        final List<String> flatValues = new ArrayList<>();
        IntStream.range(0, randomIntBetween(0, 30)).forEach(i -> {
            int listSize = randomIntBetween(0, 10);
            final var innerList = IntStream.range(0, listSize).mapToObj(j -> (i + "/" + j)).toList();
            allValues.add(innerList);
            flatValues.addAll(innerList);
        });

        Iterator<String> iterator = new LazilyExpandingShardIterator<>(allValues);

        int nextIndex = 0;
        while (true) {
            if (randomBoolean()) {
                assertEquals(iterator.hasNext(), nextIndex < flatValues.size());
            } else {
                if (nextIndex < flatValues.size()) {
                    assertEquals(iterator.next(), flatValues.get(nextIndex++));
                } else {
                    assertThrows(NoSuchElementException.class, iterator::next);
                }
            }
            if (randomBoolean() && nextIndex == flatValues.size()) {
                break;
            }
        }
    }

    public void testShardIterationOrder() {
        final var iteratorFactory = new DefaultNonPreferredShardIteratorFactory(
            new WriteLoadConstraintSettings(
                ClusterSettings.createBuiltInClusterSettings(
                    Settings.builder()
                        .put(
                            WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                            WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                        )
                        .build()
                )
            )
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(randomIntBetween(2, 20));
        final Iterable<ShardRouting> shards = iteratorFactory.createNonPreferredShardIterator(routingAllocation);

        long lastNodeQueueLatency = -1;
        int totalCount = 0;
        for (ShardRouting shardRouting : shards) {
            totalCount++;
            final String nodeId = shardRouting.currentNodeId();
            NodeUsageStatsForThreadPools nodeUsageStatsForThreadPools = routingAllocation.clusterInfo()
                .getNodeUsageStatsForThreadPools()
                .get(nodeId);
            assertNotNull(nodeUsageStatsForThreadPools);
            long thisNodeQueueLatency = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE)
                .maxThreadPoolQueueLatencyMillis();
            // should receive shards from nodes in descending queue latency order
            if (lastNodeQueueLatency != -1) {
                assertThat(thisNodeQueueLatency, lessThanOrEqualTo(lastNodeQueueLatency));
            }
            lastNodeQueueLatency = thisNodeQueueLatency;
            final Double shardWriteLoad = routingAllocation.clusterInfo().getShardWriteLoads().get(shardRouting.shardId());
            // Should not receive shards with no write-load
            assertNotNull(shardWriteLoad);
            // TODO: assert on shard order, when we know what we want that to be
        }

        if (totalCount > 0) {
            assertThat(lastNodeQueueLatency, greaterThanOrEqualTo(0L));
        }
    }

    private RoutingAllocation createRoutingAllocation(int numberOfNodes) {
        int writeThreadPoolSize = randomIntBetween(4, 32);
        final Map<String, NodeUsageStatsForThreadPools> nodeUsageStats = new HashMap<>();
        final List<String> allNodeIds = new ArrayList<>();
        IntStream.range(0, numberOfNodes).mapToObj(i -> "node_" + i).forEach(nodeId -> {
            // Some have no utilization
            if (usually()) {
                nodeUsageStats.put(
                    nodeId,
                    new NodeUsageStatsForThreadPools(
                        nodeId,
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                                writeThreadPoolSize,
                                randomFloatBetween(0.0f, 1.0f, true),
                                randomLongBetween(0, 5_000)
                            )
                        )
                    )
                );
            }
            allNodeIds.add(nodeId);
        });

        ClusterInfo.Builder clusterInfo = ClusterInfo.builder().nodeUsageStatsForThreadPools(nodeUsageStats);

        final int numberOfPrimaries = randomIntBetween(1, numberOfNodes * 3);
        final ClusterState state = ClusterStateCreationUtils.state(
            numberOfNodes,
            new String[] { randomIdentifier(), randomIdentifier(), randomIdentifier() },
            numberOfPrimaries
        );

        final Map<ShardId, Double> shardWriteLoads = new HashMap<>();
        for (RoutingNode node : state.getRoutingNodes()) {
            for (ShardRouting shardRouting : node) {
                // Some have no write-load
                if (usually()) {
                    shardWriteLoads.put(shardRouting.shardId(), randomDoubleBetween(0.0, writeThreadPoolSize, true));
                }
            }
        }
        clusterInfo.shardWriteLoads(shardWriteLoads);

        return new RoutingAllocation(null, state, clusterInfo.build(), null, System.nanoTime());
    }
}
