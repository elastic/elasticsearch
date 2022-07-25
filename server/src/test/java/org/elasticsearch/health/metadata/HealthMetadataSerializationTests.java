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
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
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
            randomRelativeByteSizeValue(),
            randomRelativeByteSizeValue(),
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

    static HealthMetadata.Disk mutateDiskMetadata(HealthMetadata.Disk base) {
        RelativeByteSizeValue highWatermark = base.highWatermark();
        RelativeByteSizeValue floodStageWatermark = base.floodStageWatermark();
        RelativeByteSizeValue floodStageWatermarkFrozen = base.frozenFloodStageWatermark();
        ByteSizeValue floodStageWatermarkFrozenMaxHeadRoom = base.frozenFloodStageMaxHeadroom();
        switch (randomInt(3)) {
            case 0 -> highWatermark = randomRelativeByteSizeValue();
            case 1 -> floodStageWatermark = randomRelativeByteSizeValue();
            case 2 -> floodStageWatermarkFrozen = randomRelativeByteSizeValue();
            case 3 -> ByteSizeValue.ofGb(randomIntBetween(10, 999));
        }
        return new HealthMetadata.Disk(highWatermark, floodStageWatermark, floodStageWatermarkFrozen, floodStageWatermarkFrozenMaxHeadRoom);
    }

    private HealthMetadata mutate(HealthMetadata base) {
        return new HealthMetadata(mutateDiskMetadata(base.getDiskMetadata()));
    }
}
