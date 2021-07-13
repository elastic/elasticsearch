/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.util.FrozenUtilsTests;
import org.elasticsearch.xpack.core.DataTier;

import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FrozenShardsDeciderServiceTests extends AutoscalingTestCase {

    public void testCountFrozenShards() {
        final Metadata.Builder builder = Metadata.builder();
        int count = 0;
        for (int i = 0; i < randomInt(20); ++i) {
            int shards = between(1, 3);
            int replicas = between(0, 2);
            String tierPreference = randomBoolean() ? DataTier.DATA_FROZEN : randomNonFrozenTierPreference();
            if (Objects.equals(tierPreference, DataTier.DATA_FROZEN)) {
                count += shards * (replicas + 1);
            }
            builder.put(
                IndexMetadata.builder("index" + i).settings(indexSettings(tierPreference)).numberOfShards(shards).numberOfReplicas(replicas)
            );
        }

        assertThat(FrozenShardsDeciderService.countFrozenShards(builder.build()), equalTo(count));
    }

    public void testScale() {
        FrozenShardsDeciderService service = new FrozenShardsDeciderService();
        int shards = between(1, 3);
        int replicas = between(0, 2);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(indexSettings(DataTier.DATA_FROZEN))
                    .numberOfShards(shards)
                    .numberOfReplicas(replicas)
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        AutoscalingDeciderContext context = mock(AutoscalingDeciderContext.class);
        when(context.state()).thenReturn(state);
        AutoscalingDeciderResult defaultSettingsResult = service.scale(Settings.EMPTY, context);
        assertThat(
            defaultSettingsResult.requiredCapacity().total().memory(),
            equalTo(ByteSizeValue.ofBytes(FrozenShardsDeciderService.DEFAULT_MEMORY_PER_SHARD.getBytes() * shards * (replicas + 1)))
        );
        assertThat(defaultSettingsResult.reason().summary(), equalTo("shard count [" + (shards * (replicas + 1) + "]")));

        ByteSizeValue memoryPerShard = new ByteSizeValue(
            randomLongBetween(0, 1000),
            randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB)
        );
        AutoscalingDeciderResult overrideSettingsResult = service.scale(
            Settings.builder().put(FrozenShardsDeciderService.MEMORY_PER_SHARD.getKey(), memoryPerShard).build(),
            context
        );
        assertThat(
            overrideSettingsResult.requiredCapacity().total().memory(),
            equalTo(ByteSizeValue.ofBytes(memoryPerShard.getBytes() * shards * (replicas + 1)))
        );
    }

    private String randomNonFrozenTierPreference() {
        return FrozenUtilsTests.randomNonFrozenTierPreference();
    }

    private static Settings indexSettings(String tierPreference) {
        return FrozenUtilsTests.indexSettings(tierPreference);
    }
}
