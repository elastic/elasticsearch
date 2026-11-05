/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.IndexFieldStats;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexPropertiesTests extends ESTestCase {

    public void testShardCountAndModeArePreserved() {
        IndexMode mode = randomFrom(IndexMode.availableModes());
        int count = between(0, 1000);
        IndexProperties props = new IndexProperties(mode, count);
        assertEquals(count, props.numberOfShards());
        assertEquals(mode, props.indexMode());
    }

    public void testSerializationRoundTripCurrentVersion() throws IOException {
        IndexMode mode = randomFrom(IndexMode.availableModes());
        int count = between(0, 1000);
        IndexProperties original = new IndexProperties(mode, count, randomLongBetween(-1, Long.MAX_VALUE), randomLongBetween(-1, 1000));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        original.writeTo(out);

        try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
            in.setTransportVersion(TransportVersion.current());
            IndexProperties roundTripped = new IndexProperties(in);
            assertEquals(original, roundTripped);
        }
    }

    /**
     * Verifies that deserializing from a pre-SHARD_COUNTS node (which writes only the IndexMode)
     * produces a shard count of 0 ("unknown").
     */
    public void testDeserializationFromPreShardCountsVersionDefaultsToZero() throws IOException {
        // Use the version just before SHARD_COUNTS so the wire stream omits shard count. Skip
        // IndexModes that cannot be serialized at that version (e.g. vectordb_columnar).
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(IndexProperties.SHARD_COUNTS);
        IndexMode mode = randomValueOtherThanMany(
            m -> m.supportsVersion(oldVersion) == false,
            () -> randomFrom(IndexMode.availableModes())
        );
        int count = between(1, 1000);
        IndexProperties original = new IndexProperties(mode, count);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        original.writeTo(out);

        try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
            in.setTransportVersion(oldVersion);
            IndexProperties roundTripped = new IndexProperties(in);
            assertEquals(mode, roundTripped.indexMode());
            assertEquals(0, roundTripped.numberOfShards());
        }
    }

    public void testDeserializationFromPreDocStatsVersionIsUnknown() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(IndexProperties.DOC_STATS);
        IndexMode mode = randomValueOtherThanMany(
            m -> m.supportsVersion(oldVersion) == false,
            () -> randomFrom(IndexMode.availableModes())
        );
        IndexProperties original = new IndexProperties(mode, between(1, 1000), between(0, 1000), between(0, 1000));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        original.writeTo(out);

        try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
            in.setTransportVersion(oldVersion);
            IndexProperties roundTripped = new IndexProperties(in);
            assertEquals(original.numberOfShards(), roundTripped.numberOfShards());
            assertEquals(-1, roundTripped.docCount());
            assertEquals(-1, roundTripped.sizeInBytes());
        }
    }

    public void testFromShardSampleExtrapolatesToIndex() {
        IndexProperties props = IndexProperties.fromShardSample(IndexMode.STANDARD, 5, 100, 2000, Map.of());
        assertEquals(500, props.docCount());
        assertEquals(10000, props.sizeInBytes());
    }

    public void testFromShardSampleUnknownWithoutShardStatsOrShardCount() {
        IndexProperties noStats = IndexProperties.fromShardSample(IndexMode.STANDARD, 5, -1, -1, Map.of());
        assertEquals(-1, noStats.docCount());
        assertEquals(-1, noStats.sizeInBytes());
        IndexProperties noShards = IndexProperties.fromShardSample(IndexMode.STANDARD, 0, 100, 2000, Map.of());
        assertEquals(-1, noShards.docCount());
        assertEquals(-1, noShards.sizeInBytes());
    }

    public void testFromShardSampleExtrapolatesFieldStats() {
        IndexProperties props = IndexProperties.fromShardSample(
            IndexMode.STANDARD,
            3,
            100,
            2000,
            Map.of("k", new IndexFieldStats(90, 100, 1000, null, null), "n", new IndexFieldStats(100, 100, -1, -5L, 7L))
        );
        assertEquals(new IndexFieldStats(270, 300, 3000, null, null), props.fieldStats().get("k"));
        assertEquals(new IndexFieldStats(300, 300, -1, -5L, 7L), props.fieldStats().get("n"));
    }

    public void testFieldStatsAreNotSerialized() throws IOException {
        IndexProperties original = new IndexProperties(
            IndexMode.STANDARD,
            1,
            10,
            100,
            Map.of("k", new IndexFieldStats(10, 10, 100, null, null))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        original.writeTo(out);

        try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
            in.setTransportVersion(TransportVersion.current());
            IndexProperties roundTripped = new IndexProperties(in);
            assertEquals(Map.of(), roundTripped.fieldStats());
            assertEquals(original, roundTripped);
        }
    }
}
