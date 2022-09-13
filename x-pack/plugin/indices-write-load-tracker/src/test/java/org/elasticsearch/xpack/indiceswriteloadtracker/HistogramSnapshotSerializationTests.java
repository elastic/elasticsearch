/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class HistogramSnapshotSerializationTests extends AbstractSerializingTestCase<HistogramSnapshot> {

    @Override
    protected HistogramSnapshot doParseInstance(XContentParser parser) throws IOException {
        return HistogramSnapshot.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<HistogramSnapshot> instanceReader() {
        return HistogramSnapshot::new;
    }

    @Override
    protected HistogramSnapshot createTestInstance() {
        return randomHistogramSnapshot();
    }

    @Override
    protected HistogramSnapshot mutateInstance(HistogramSnapshot instance) throws IOException {
        return mutateHistogramSnapshot(instance);
    }

    public static HistogramSnapshot mutateHistogramSnapshot(HistogramSnapshot histogramSnapshot) {
        final var mutationBranch = randomInt(6);
        return switch (mutationBranch) {
            case 0 -> new HistogramSnapshot(
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 1 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 2 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 3 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 4 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 5 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.p99(),
                histogramSnapshot.max()
            );
            case 6 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                randomDoubleBetween(0.0, 128.0, true),
                histogramSnapshot.max()
            );
            case 7 -> new HistogramSnapshot(
                histogramSnapshot.average(),
                histogramSnapshot.p50(),
                histogramSnapshot.p63(),
                histogramSnapshot.p75(),
                histogramSnapshot.p90(),
                histogramSnapshot.p95(),
                histogramSnapshot.p99(),
                randomDoubleBetween(0.0, 128.0, true)
            );
            default -> throw new IllegalStateException("Unexpected value: " + mutationBranch);
        };
    }

    public static HistogramSnapshot randomHistogramSnapshot() {
        return new HistogramSnapshot(
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true)
        );
    }
}
