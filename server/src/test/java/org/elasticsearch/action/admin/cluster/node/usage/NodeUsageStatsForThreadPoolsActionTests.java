/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction.NodeResponse;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Wire (de)serialization tests for {@link NodeResponse}, including backwards-compatibility coverage for
 * the per-shard write load map that was only added starting at {@link NodeResponse#ADD_SHARD_WRITE_LOADS}.
 */
public class NodeUsageStatsForThreadPoolsActionTests extends AbstractWireSerializingTestCase<NodeResponse> {

    @Override
    protected NodeResponse createTestInstance() {
        return randomNodeResponse();
    }

    @Override
    protected Writeable.Reader<NodeResponse> instanceReader() {
        return NodeResponse::new;
    }

    @Override
    protected NodeResponse mutateInstance(NodeResponse instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new NodeResponse(
                DiscoveryNodeUtils.create(instance.getNode().getId() + "-mutated"),
                instance.getNodeUsageStatsForThreadPools(),
                instance.getShardWriteLoads()
            );
            case 1 -> new NodeResponse(
                instance.getNode(),
                randomValueOtherThan(
                    instance.getNodeUsageStatsForThreadPools(),
                    () -> randomNodeUsageStatsForThreadPools(instance.getNode().getId())
                ),
                instance.getShardWriteLoads()
            );
            case 2 -> new NodeResponse(
                instance.getNode(),
                instance.getNodeUsageStatsForThreadPools(),
                randomValueOtherThan(instance.getShardWriteLoads(), NodeUsageStatsForThreadPoolsActionTests::randomShardWriteLoads)
            );
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testShardWriteLoadsRoundTripOnVersionsSupportingThem() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionSupporting(NodeResponse.ADD_SHARD_WRITE_LOADS);
        final NodeResponse original = randomNodeResponse();
        final NodeResponse deserialized = copyInstance(original, version);

        assertThat(deserialized.getNodeUsageStatsForThreadPools(), equalTo(original.getNodeUsageStatsForThreadPools()));
        assertThat(deserialized.getShardWriteLoads(), equalTo(original.getShardWriteLoads()));
    }

    public void testShardWriteLoadsAreEmptyOnVersionsNotSupportingThem() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(NodeResponse.ADD_SHARD_WRITE_LOADS);
        final NodeResponse original = randomNodeResponse();
        final NodeResponse deserialized = copyInstance(original, version);

        // The node's non-shard stats are unaffected by the older transport version.
        assertThat(deserialized.getNodeUsageStatsForThreadPools(), equalTo(original.getNodeUsageStatsForThreadPools()));
        // The shard write loads map, which did not exist in earlier versions, comes back empty after a round-trip of serialization.
        assertThat(deserialized.getShardWriteLoads(), equalTo(Map.of()));
    }

    public void testEmptyShardWriteLoadsRoundTripOnVersionsSupportingThem() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionSupporting(NodeResponse.ADD_SHARD_WRITE_LOADS);
        final DiscoveryNode node = DiscoveryNodeUtils.create("node-0");
        final NodeResponse original = new NodeResponse(node, randomNodeUsageStatsForThreadPools(node.getId()), Map.of());

        final NodeResponse deserialized = copyInstance(original, version);

        assertThat(deserialized.getNodeUsageStatsForThreadPools(), equalTo(original.getNodeUsageStatsForThreadPools()));
        assertThat(deserialized.getShardWriteLoads(), equalTo(Map.of()));
    }

    private static NodeResponse randomNodeResponse() {
        final DiscoveryNode node = DiscoveryNodeUtils.create("node-0");
        return new NodeResponse(node, randomNodeUsageStatsForThreadPools(node.getId()), randomShardWriteLoads());
    }

    private static NodeUsageStatsForThreadPools randomNodeUsageStatsForThreadPools(String nodeId) {
        final Map<String, ThreadPoolUsageStats> threadPoolUsageStatsMap = new HashMap<>();
        threadPoolUsageStatsMap.put(
            ThreadPool.Names.WRITE,
            new ThreadPoolUsageStats(
                randomIntBetween(1, 32) /* number of threads */,
                randomFloat() /* utilization */,
                randomLongBetween(0, 10_000) /* max queue latency */
            )
        );
        return new NodeUsageStatsForThreadPools(nodeId, threadPoolUsageStatsMap);
    }

    private static Map<ShardId, Double> randomShardWriteLoads() {
        final Map<ShardId, Double> shardWriteLoads = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            shardWriteLoads.put(new ShardId(randomIdentifier(), randomUUID(), i), randomDoubleBetween(0.0, 10.0, true));
        }
        return shardWriteLoads;
    }
}
