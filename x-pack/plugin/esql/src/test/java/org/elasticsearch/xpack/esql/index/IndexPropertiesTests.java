/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

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
        IndexProperties original = new IndexProperties(mode, count);

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
        // Use the version just before SHARD_COUNTS: modern enough to support all IndexModes, but
        // the wire stream won't include a shard count.
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(IndexProperties.SHARD_COUNTS);
        IndexMode mode = randomFrom(IndexMode.availableModes());
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
}
