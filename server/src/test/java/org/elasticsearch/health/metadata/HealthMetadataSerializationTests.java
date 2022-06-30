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
        return new HealthMetadata(randomDiskMetadata());
    }

    private static HealthMetadata.Disk randomDiskMetadata() {
        return new HealthMetadata.Disk(
            randomDiskThreshold(),
            randomDiskThreshold(),
            randomDiskThreshold(),
            randomDiskThreshold(),
            ByteSizeValue.ofGb(randomIntBetween(10, 999))
        );
    }

    private static HealthMetadata.Disk.Threshold randomDiskThreshold() {
        if (randomBoolean()) {
            return new HealthMetadata.Disk.Threshold(ByteSizeValue.ofGb(randomIntBetween(10, 999)));
        } else {
            return new HealthMetadata.Disk.Threshold(randomDouble());
        }
    }

    static HealthMetadata.Disk mutateDiskMetadata(HealthMetadata.Disk base) {
        HealthMetadata.Disk.Threshold lowWatermark = base.lowWatermark();
        HealthMetadata.Disk.Threshold highWatermark = base.highWatermark();
        HealthMetadata.Disk.Threshold floodStageWatermark = base.floodStageWatermark();
        HealthMetadata.Disk.Threshold floodStageWatermarkFrozen = base.frozenFloodStageWatermark();
        ByteSizeValue floodStageWatermarkFrozenMaxHeadRoom = base.frozenFloodStageMaxHeadroom();
        switch (randomInt(4)) {
            case 0 -> lowWatermark = randomDiskThreshold();
            case 1 -> highWatermark = randomDiskThreshold();
            case 2 -> floodStageWatermark = randomDiskThreshold();
            case 3 -> floodStageWatermarkFrozen = randomDiskThreshold();
            case 4 -> ByteSizeValue.ofGb(randomIntBetween(10, 999));
        }
        return new HealthMetadata.Disk(
            lowWatermark,
            highWatermark,
            floodStageWatermark,
            floodStageWatermarkFrozen,
            floodStageWatermarkFrozenMaxHeadRoom
        );
    }

    private HealthMetadata mutate(HealthMetadata base) {
        return new HealthMetadata(mutateDiskMetadata(base.getDiskMetadata()));
    }
}
