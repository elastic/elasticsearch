/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionStatsTests extends AbstractWireSerializingTestCase<VersionStats> {

    @Override
    protected Writeable.Reader<VersionStats> instanceReader() {
        return VersionStats::new;
    }

    @Override
    protected VersionStats createTestInstance() {
        return randomInstance();
    }

    @Override
    protected VersionStats mutateInstance(VersionStats instance) {
        return new VersionStats(instance.versionStats().stream().map(svs -> {
            return switch (randomIntBetween(1, 4)) {
                case 1 -> new VersionStats.SingleVersionStats(
                    IndexVersion.V_7_3_0,
                    svs.indexCount,
                    svs.primaryShardCount,
                    svs.totalPrimaryByteCount
                );
                case 2 -> new VersionStats.SingleVersionStats(
                    svs.version,
                    svs.indexCount + 1,
                    svs.primaryShardCount,
                    svs.totalPrimaryByteCount
                );
                case 3 -> new VersionStats.SingleVersionStats(
                    svs.version,
                    svs.indexCount,
                    svs.primaryShardCount + 1,
                    svs.totalPrimaryByteCount
                );
                case 4 -> new VersionStats.SingleVersionStats(
                    svs.version,
                    svs.indexCount,
                    svs.primaryShardCount,
                    svs.totalPrimaryByteCount + 1
                );
                default -> throw new IllegalArgumentException("unexpected branch");
            };
        }).toList());
    }

    public void testCreation() {
        Metadata metadata = Metadata.builder().build();
        VersionStats stats = VersionStats.of(metadata, Collections.emptyList());
        assertThat(stats.versionStats(), equalTo(Collections.emptySet()));

        metadata = new Metadata.Builder().put(indexMeta("foo", Version.CURRENT, 4), true)
            .put(indexMeta("bar", Version.CURRENT, 3), true)
            .put(indexMeta("baz", Version.V_7_0_0, 2), true)
            .build();
        stats = VersionStats.of(metadata, Collections.emptyList());
        assertThat(stats.versionStats().size(), equalTo(2));
        VersionStats.SingleVersionStats s1 = new VersionStats.SingleVersionStats(IndexVersion.current(), 2, 7, 0);
        VersionStats.SingleVersionStats s2 = new VersionStats.SingleVersionStats(IndexVersion.V_7_0_0, 1, 2, 0);
        assertThat(stats.versionStats(), containsInAnyOrder(s1, s2));

        ShardId shardId = new ShardId("bar", "uuid", 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
            ShardRouting.Role.DEFAULT
        );
        Path path = createTempDir().resolve("indices")
            .resolve(shardRouting.shardId().getIndex().getUUID())
            .resolve(String.valueOf(shardRouting.shardId().id()));
        IndexShard indexShard = mock(IndexShard.class);
        StoreStats storeStats = new StoreStats(100, 150, 200);
        when(indexShard.storeStats()).thenReturn(storeStats);
        ShardStats shardStats = new ShardStats(
            shardRouting,
            new ShardPath(false, path, path, shardRouting.shardId()),
            CommonStats.getShardLevelStats(null, indexShard, new CommonStatsFlags(CommonStatsFlags.Flag.Store)),
            null,
            null,
            null,
            false,
            0
        );
        ClusterStatsNodeResponse nodeResponse = new ClusterStatsNodeResponse(
            DiscoveryNodeUtils.create("id"),
            ClusterHealthStatus.GREEN,
            null,
            null,
            new ShardStats[] { shardStats },
            null
        );

        stats = VersionStats.of(metadata, Collections.singletonList(nodeResponse));
        assertThat(stats.versionStats().size(), equalTo(2));
        s1 = new VersionStats.SingleVersionStats(IndexVersion.current(), 2, 7, 100);
        s2 = new VersionStats.SingleVersionStats(IndexVersion.V_7_0_0, 1, 2, 0);
        assertThat(stats.versionStats(), containsInAnyOrder(s1, s2));
    }

    private static IndexMetadata indexMeta(String name, Version version, int primaryShards) {
        return new IndexMetadata.Builder(name).settings(indexSettings(version, primaryShards, randomIntBetween(0, 3))).build();
    }

    public static VersionStats randomInstance() {
        List<IndexVersion> versions = List.of(IndexVersion.current(), IndexVersion.V_7_0_0, IndexVersion.V_7_1_0, IndexVersion.V_7_2_0);
        List<VersionStats.SingleVersionStats> stats = new ArrayList<>();
        for (IndexVersion v : versions) {
            VersionStats.SingleVersionStats s = new VersionStats.SingleVersionStats(
                v,
                randomIntBetween(10, 20),
                randomIntBetween(20, 30),
                randomNonNegativeLong()
            );
            stats.add(s);
        }
        return new VersionStats(stats);
    }
}
