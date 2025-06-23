/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.util.FrozenUtilsTests;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FrozenStorageDeciderServiceTests extends AutoscalingTestCase {

    public void testEstimateSize() {
        final int shards = between(1, 10);
        final int replicas = between(0, 9);
        final IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(shards)
            .numberOfReplicas(replicas)
            .build();
        final Tuple<Long, ClusterInfo> sizeAndClusterInfo = sizeAndClusterInfo(indexMetadata);
        final long expected = sizeAndClusterInfo.v1();
        final ClusterInfo info = sizeAndClusterInfo.v2();
        assertThat(FrozenStorageDeciderService.estimateSize(indexMetadata, info), equalTo(expected));
        assertThat(FrozenStorageDeciderService.estimateSize(indexMetadata, ClusterInfo.EMPTY), equalTo(0L));
    }

    public void testScale() {
        FrozenStorageDeciderService service = new FrozenStorageDeciderService();

        int shards = between(1, 3);
        int replicas = between(0, 2);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(FrozenUtilsTests.indexSettings(DataTier.DATA_FROZEN))
                    .numberOfShards(shards)
                    .numberOfReplicas(replicas)
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        AutoscalingDeciderContext context = mock(AutoscalingDeciderContext.class);
        when(context.state()).thenReturn(state);
        final Tuple<Long, ClusterInfo> sizeAndClusterInfo = sizeAndClusterInfo(metadata.getProject().index("index"));
        final long dataSetSize = sizeAndClusterInfo.v1();
        final ClusterInfo info = sizeAndClusterInfo.v2();
        when(context.info()).thenReturn(info);

        AutoscalingDeciderResult defaultSettingsResult = service.scale(Settings.EMPTY, context);
        assertThat(
            defaultSettingsResult.requiredCapacity().total().storage(),
            equalTo(ByteSizeValue.ofBytes((long) (FrozenStorageDeciderService.DEFAULT_PERCENTAGE * dataSetSize) / 100))
        );
        assertThat(defaultSettingsResult.requiredCapacity().total().memory(), nullValue());
        assertThat(defaultSettingsResult.reason().summary(), equalTo("total data set size [" + dataSetSize + "]"));

        // the percentage is not the cache size, rather the node size. So someone could want 101% just as much as 100% so we do not have
        // a real upper bound. Therefore testing to 200%.
        double percentage = randomDoubleBetween(0.0d, 200.0d, true);
        AutoscalingDeciderResult overrideSettingsResult = service.scale(
            Settings.builder().put(FrozenStorageDeciderService.PERCENTAGE.getKey(), percentage).build(),
            context
        );
        assertThat(
            overrideSettingsResult.requiredCapacity().total().storage(),
            equalTo(ByteSizeValue.ofBytes((long) (percentage * dataSetSize) / 100))
        );
    }

    public Tuple<Long, ClusterInfo> sizeAndClusterInfo(IndexMetadata indexMetadata) {
        long totalSize = 0;
        Map<ShardId, Long> sizes = new HashMap<>();
        Index index = indexMetadata.getIndex();
        Index otherIndex = randomValueOtherThan(index, () -> new Index(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        int shards = indexMetadata.getNumberOfShards();
        int replicas = indexMetadata.getNumberOfReplicas();
        for (int i = 0; i < shards; ++i) {
            long size = randomLongBetween(0, Integer.MAX_VALUE);
            totalSize += size * (replicas + 1);
            sizes.put(new ShardId(index, i), size);
            // add other index shards.
            sizes.put(new ShardId(otherIndex, i), randomLongBetween(0, Integer.MAX_VALUE));
        }
        for (int i = shards; i < shards + between(0, 3); ++i) {
            // add irrelevant shards noise for completeness (should not happen IRL).
            sizes.put(new ShardId(index, i), randomLongBetween(0, Integer.MAX_VALUE));
        }
        ClusterInfo info = new ClusterInfo(Map.of(), Map.of(), Map.of(), sizes, Map.of(), Map.of(), Map.of());
        return Tuple.tuple(totalSize, info);
    }
}
