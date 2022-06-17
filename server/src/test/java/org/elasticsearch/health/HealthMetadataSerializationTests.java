/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class HealthMetadataSerializationTests extends SimpleDiffableSerializationTestCase<Metadata.Custom> {

    private static final String[] SAMPLE_THRESHOLDS = { "0.1", "0.2", "0.5", "0.7", "0.8", "0.95", "100GB" };

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
            List.of(
                new NamedWriteableRegistry.Entry(Metadata.Custom.class, HealthMetadata.TYPE, HealthMetadata::new),
                new NamedWriteableRegistry.Entry(
                    HealthMetadata.DiskThresholds.class,
                    HealthMetadata.DiskThresholds.NAME,
                    HealthMetadata.DiskThresholds::readFrom
                )
            )
        );
    }

    @Override
    protected Metadata.Custom createTestInstance() {
        return randomHealthMetadata();
    }

    private static HealthMetadata randomHealthMetadata() {
        return new HealthMetadata(
             new HealthMetadata.DiskThresholds(
             randomFrom(SAMPLE_THRESHOLDS),
             randomFrom(SAMPLE_THRESHOLDS),
             randomFrom(SAMPLE_THRESHOLDS),
             randomFrom(SAMPLE_THRESHOLDS),
             randomFrom(SAMPLE_THRESHOLDS),
             randomFrom(SAMPLE_THRESHOLDS)
             )
        );
    }

    private HealthMetadata mutate(HealthMetadata base) {
        return new HealthMetadata(mutateDiskThresholds(base.getDiskThresholds()));
    }

    private HealthMetadata.DiskThresholds mutateDiskThresholds(HealthMetadata.DiskThresholds diskThresholds) {
        String lowWatermark = diskThresholds.lowWatermark();
        String highWatermark = diskThresholds.highWatermark();
        String floodStageWatermark = diskThresholds.floodStageWatermark();
        String floodStageWatermarkFrozen = diskThresholds.floodStageWatermarkFrozen();
        String yellowThreshold = diskThresholds.yellowThreshold();
        String redThreshold = diskThresholds.redThreshold();
        switch (randomInt(5)) {
            case 0 -> lowWatermark = randomFrom(SAMPLE_THRESHOLDS);
            case 1 -> highWatermark = randomFrom(SAMPLE_THRESHOLDS);
            case 2 -> floodStageWatermark = randomFrom(SAMPLE_THRESHOLDS);
            case 3 -> floodStageWatermarkFrozen = randomFrom(SAMPLE_THRESHOLDS);
            case 4 -> yellowThreshold = randomFrom(SAMPLE_THRESHOLDS);
            case 5 -> redThreshold = randomFrom(SAMPLE_THRESHOLDS);
        }
        return new HealthMetadata.DiskThresholds(
            lowWatermark,
            highWatermark,
            floodStageWatermark,
            floodStageWatermarkFrozen,
            yellowThreshold,
            redThreshold
        );
    }
}
