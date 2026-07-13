/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;

import java.util.List;

public class HealthMetadataSerializationTests extends SimpleDiffableWireSerializationTestCase<ClusterState.Custom> {

    @Override
    protected ClusterState.Custom makeTestChanges(ClusterState.Custom testInstance) {
        if (randomBoolean()) {
            return testInstance;
        }
        return mutate((HealthMetadata) testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<ClusterState.Custom>> diffReader() {
        return HealthMetadata::readDiffFrom;
    }

    @Override
    protected Writeable.Reader<ClusterState.Custom> instanceReader() {
        return HealthMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, HealthMetadata.TYPE, HealthMetadata::new))
        );
    }

    @Override
    protected ClusterState.Custom createTestInstance() {
        return randomHealthMetadata();
    }

    @Override
    protected ClusterState.Custom mutateInstance(ClusterState.Custom instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    private static HealthMetadata randomHealthMetadata() {
        return new HealthMetadata(randomDiskMetadata(), randomShardLimitsMetadata());
    }

    private static HealthMetadata.ShardLimits randomShardLimitsMetadata() {
        return randomBoolean() ? randomShardLimitsMetadataNonNull() : null;
    }

    private static HealthMetadata.ShardLimits randomShardLimitsMetadataNonNull() {
        return new HealthMetadata.ShardLimits(
            randomIntBetween(1, 10000),
            randomIntBetween(1, 10000),
            randomIntBetween(1, 10000),
            randomIntBetween(1, 10000)
        );
    }

    private static HealthMetadata.Disk randomDiskMetadata() {
        return new HealthMetadata.Disk(
            randomRelativeByteSizeValue(),
            ByteSizeValue.ofGb(randomIntBetween(10, 999)),
            randomRelativeByteSizeValue(),
            ByteSizeValue.ofGb(randomIntBetween(10, 999)),
            randomRelativeByteSizeValue(),
            ByteSizeValue.ofGb(randomIntBetween(10, 999))
        );
    }

    private static RelativeByteSizeValue randomRelativeByteSizeValue() {
        if (randomBoolean()) {
            return new RelativeByteSizeValue(ByteSizeValue.ofGb(randomIntBetween(10, 999)));
        } else {
            return new RelativeByteSizeValue(new RatioValue(randomDouble()));
        }
    }

    static HealthMetadata.Disk mutate(HealthMetadata.Disk base) {
        RelativeByteSizeValue highWatermark = base.highWatermark();
        ByteSizeValue highWatermarkMaxHeadRoom = base.highMaxHeadroom();
        RelativeByteSizeValue floodStageWatermark = base.floodStageWatermark();
        ByteSizeValue floodStageWatermarkMaxHeadRoom = base.floodStageMaxHeadroom();
        RelativeByteSizeValue floodStageWatermarkFrozen = base.frozenFloodStageWatermark();
        ByteSizeValue floodStageWatermarkFrozenMaxHeadRoom = base.frozenFloodStageMaxHeadroom();
        switch (randomInt(5)) {
            case 0 -> highWatermark = randomValueOtherThan(highWatermark, HealthMetadataSerializationTests::randomRelativeByteSizeValue);
            case 1 -> highWatermarkMaxHeadRoom = randomValueOtherThan(
                highWatermarkMaxHeadRoom,
                () -> ByteSizeValue.ofGb(randomIntBetween(10, 999))
            );
            case 2 -> floodStageWatermark = randomValueOtherThan(
                floodStageWatermark,
                HealthMetadataSerializationTests::randomRelativeByteSizeValue
            );
            case 3 -> floodStageWatermarkMaxHeadRoom = randomValueOtherThan(
                floodStageWatermarkMaxHeadRoom,
                () -> ByteSizeValue.ofGb(randomIntBetween(10, 999))
            );
            case 4 -> floodStageWatermarkFrozen = randomValueOtherThan(
                floodStageWatermarkFrozen,
                HealthMetadataSerializationTests::randomRelativeByteSizeValue
            );
            case 5 -> floodStageWatermarkFrozenMaxHeadRoom = randomValueOtherThan(
                floodStageWatermarkFrozenMaxHeadRoom,
                () -> ByteSizeValue.ofGb(randomIntBetween(10, 999))
            );
        }
        return new HealthMetadata.Disk(
            highWatermark,
            highWatermarkMaxHeadRoom,
            floodStageWatermark,
            floodStageWatermarkMaxHeadRoom,
            floodStageWatermarkFrozen,
            floodStageWatermarkFrozenMaxHeadRoom
        );
    }

    static HealthMetadata.ShardLimits mutate(HealthMetadata.ShardLimits base) {
        if (base == null) {
            return randomShardLimitsMetadataNonNull();
        }

        int maxShardsPerNode = base.maxShardsPerNode();
        int maxShardsPerNodeFrozen = base.maxShardsPerNodeFrozen();
        int shardCapacityUnhealthyThresholdYellow = base.shardCapacityUnhealthyThresholdYellow();
        int shardCapacityUnhealthyThresholdRed = base.shardCapacityUnhealthyThresholdRed();
        switch (randomInt(3)) {
            case 0 -> maxShardsPerNode = randomValueOtherThan(maxShardsPerNode, () -> randomIntBetween(1, 10000));
            case 1 -> maxShardsPerNodeFrozen = randomValueOtherThan(maxShardsPerNodeFrozen, () -> randomIntBetween(1, 10000));
            case 2 -> shardCapacityUnhealthyThresholdYellow = randomValueOtherThan(
                shardCapacityUnhealthyThresholdYellow,
                () -> randomIntBetween(1, 10000)
            );
            case 3 -> shardCapacityUnhealthyThresholdRed = randomValueOtherThan(
                shardCapacityUnhealthyThresholdRed,
                () -> randomIntBetween(1, 10000)
            );
        }
        return new HealthMetadata.ShardLimits(
            maxShardsPerNode,
            maxShardsPerNodeFrozen,
            shardCapacityUnhealthyThresholdYellow,
            shardCapacityUnhealthyThresholdRed
        );
    }

    private HealthMetadata mutate(HealthMetadata base) {
        return new HealthMetadata(mutate(base.getDiskMetadata()), mutate(base.getShardLimitsMetadata()));
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), ignored -> 1);
    }
}
