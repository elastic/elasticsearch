/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class HealthMetadataSerializationTests extends SimpleDiffableSerializationTestCase<Metadata.Custom> {

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        if (randomBoolean()) {
            return testInstance;
        }
        return mutate((HealthMetadata) testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return HealthMetadata::readDiffFrom;
    }

    @Override
    protected Metadata.Custom doParseInstance(XContentParser parser) throws IOException {
        return HealthMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return HealthMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(Metadata.Custom.class, HealthMetadata.TYPE, HealthMetadata::new))
        );
    }

    @Override
    protected Metadata.Custom createTestInstance() {
        return randomHealthMetadata();
    }

    private static HealthMetadata randomHealthMetadata() {
        return new HealthMetadata(randomDiskHealthThresholds());
    }

    private static HealthMetadata.DiskHealthThresholds randomDiskHealthThresholds() {
        return new HealthMetadata.DiskHealthThresholds(
            randomDiskThresholdValue(),
            randomDiskThresholdValue(),
            randomDiskThresholdValue(),
            randomDiskThresholdValue(),
            ByteSizeValue.ofGb(randomIntBetween(10, 999)),
            randomDiskThresholdValue(),
            randomDiskThresholdValue()
        );
    }

    private static HealthMetadata.DiskHealthThresholds.Threshold randomDiskThresholdValue() {
        return new HealthMetadata.DiskHealthThresholds.Threshold(randomDouble(), ByteSizeValue.ofGb(randomIntBetween(10, 999)));
    }

    static HealthMetadata.DiskHealthThresholds mutateDiskThresholds(HealthMetadata.DiskHealthThresholds base) {
        HealthMetadata.DiskHealthThresholds.Threshold lowWatermark = base.lowWatermark();
        HealthMetadata.DiskHealthThresholds.Threshold highWatermark = base.highWatermark();
        HealthMetadata.DiskHealthThresholds.Threshold floodStageWatermark = base.floodStageWatermark();
        HealthMetadata.DiskHealthThresholds.Threshold floodStageWatermarkFrozen = base.floodStageWatermarkFrozen();
        ByteSizeValue floodStageWatermarkFrozenMaxHeadRoom = base.floodStageWatermarkFrozenMaxHeadroom();
        HealthMetadata.DiskHealthThresholds.Threshold yellowThreshold = base.yellowThreshold();
        HealthMetadata.DiskHealthThresholds.Threshold redThreshold = base.redThreshold();
        switch (randomInt(6)) {
            case 0 -> lowWatermark = randomDiskThresholdValue();
            case 1 -> highWatermark = randomDiskThresholdValue();
            case 2 -> floodStageWatermark = randomDiskThresholdValue();
            case 3 -> floodStageWatermarkFrozen = randomDiskThresholdValue();
            case 4 -> ByteSizeValue.ofGb(randomIntBetween(10, 999));
            case 5 -> yellowThreshold = randomDiskThresholdValue();
            case 6 -> redThreshold = randomDiskThresholdValue();
        }
        return new HealthMetadata.DiskHealthThresholds(
            lowWatermark,
            highWatermark,
            floodStageWatermark,
            floodStageWatermarkFrozen,
            floodStageWatermarkFrozenMaxHeadRoom,
            yellowThreshold,
            redThreshold
        );
    }

    private HealthMetadata mutate(HealthMetadata base) {
        return new HealthMetadata(mutateDiskThresholds(base.getDiskThresholds()));
    }
}
