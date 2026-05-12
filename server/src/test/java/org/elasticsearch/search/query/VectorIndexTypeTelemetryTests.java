/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;
import org.elasticsearch.test.ESTestCase;

public class VectorIndexTypeTelemetryTests extends ESTestCase {

    /**
     * Ordinals are encoded into bits 1-4 of the {@code QuerySearchResult} telemetry byte.
     * Reordering or removing values would silently change the on-the-wire meaning of older
     * payloads. New values may only be appended.
     */
    public void testOrdinalsAreStable() {
        assertEquals(0, VectorIndexTypeTelemetry.NONE.ordinal());
        assertEquals(1, VectorIndexTypeTelemetry.UNRECOGNIZED.ordinal());
        assertEquals(2, VectorIndexTypeTelemetry.HNSW.ordinal());
        assertEquals(3, VectorIndexTypeTelemetry.FLAT.ordinal());
        assertEquals(4, VectorIndexTypeTelemetry.BBQ.ordinal());
        assertEquals(5, VectorIndexTypeTelemetry.MIXED.ordinal());
        // 4 bits hold 16 ordinals; if we exceed this the wire format must grow.
        assertTrue("VectorIndexTypeTelemetry must fit in 4 bits", VectorIndexTypeTelemetry.values().length <= 16);
    }

    public void testFromOrdinalIsStable() {
        // Since this is serialized, the order must be stable.
        assertEquals(VectorIndexTypeTelemetry.NONE, VectorIndexTypeTelemetry.fromOrdinal(0));
        assertEquals(VectorIndexTypeTelemetry.UNRECOGNIZED, VectorIndexTypeTelemetry.fromOrdinal(1));
        assertEquals(VectorIndexTypeTelemetry.HNSW, VectorIndexTypeTelemetry.fromOrdinal(2));
        assertEquals(VectorIndexTypeTelemetry.FLAT, VectorIndexTypeTelemetry.fromOrdinal(3));
        assertEquals(VectorIndexTypeTelemetry.BBQ, VectorIndexTypeTelemetry.fromOrdinal(4));
        assertEquals(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.fromOrdinal(5));
        assertEquals(VectorIndexTypeTelemetry.UNRECOGNIZED, VectorIndexTypeTelemetry.fromOrdinal(6));
    }

    public void testLabels() {
        assertNull(VectorIndexTypeTelemetry.NONE.label());
        assertEquals("unrecognized", VectorIndexTypeTelemetry.UNRECOGNIZED.label());
        assertEquals("hnsw", VectorIndexTypeTelemetry.HNSW.label());
        assertEquals("flat", VectorIndexTypeTelemetry.FLAT.label());
        assertEquals("bbq", VectorIndexTypeTelemetry.BBQ.label());
        assertEquals("mixed", VectorIndexTypeTelemetry.MIXED.label());
    }

    public void testFromOrdinalRoundTrips() {
        for (VectorIndexTypeTelemetry value : VectorIndexTypeTelemetry.values()) {
            assertSame(value, VectorIndexTypeTelemetry.fromOrdinal(value.ordinal()));
        }
    }

    public void testFromOrdinalOutOfRangeReturnsUnrecognized() {
        assertSame(VectorIndexTypeTelemetry.UNRECOGNIZED, VectorIndexTypeTelemetry.fromOrdinal(-1));
        assertSame(VectorIndexTypeTelemetry.UNRECOGNIZED, VectorIndexTypeTelemetry.fromOrdinal(15));
        assertSame(VectorIndexTypeTelemetry.UNRECOGNIZED, VectorIndexTypeTelemetry.fromOrdinal(Integer.MAX_VALUE));
    }

    public void testEveryVectorIndexTypeBucketsToConcreteValue() {
        for (VectorIndexType type : VectorIndexType.values()) {
            VectorIndexTypeTelemetry bucket = VectorIndexTypeTelemetry.of(type);
            // of() resolves a known local enum value, so it must never produce sentinel buckets.
            assertNotSame("VectorIndexType " + type + " must bucket to a concrete telemetry value", VectorIndexTypeTelemetry.NONE, bucket);
            assertNotSame(
                "VectorIndexType " + type + " must bucket to a concrete telemetry value",
                VectorIndexTypeTelemetry.UNRECOGNIZED,
                bucket
            );
            assertNotSame("VectorIndexType " + type + " must bucket to a concrete telemetry value", VectorIndexTypeTelemetry.MIXED, bucket);
        }
    }

    public void testBucketAssignment() {
        assertSame(VectorIndexTypeTelemetry.HNSW, VectorIndexTypeTelemetry.of(VectorIndexType.HNSW));
        assertSame(VectorIndexTypeTelemetry.HNSW, VectorIndexTypeTelemetry.of(VectorIndexType.INT8_HNSW));
        assertSame(VectorIndexTypeTelemetry.HNSW, VectorIndexTypeTelemetry.of(VectorIndexType.INT4_HNSW));
        assertSame(VectorIndexTypeTelemetry.FLAT, VectorIndexTypeTelemetry.of(VectorIndexType.FLAT));
        assertSame(VectorIndexTypeTelemetry.FLAT, VectorIndexTypeTelemetry.of(VectorIndexType.INT8_FLAT));
        assertSame(VectorIndexTypeTelemetry.FLAT, VectorIndexTypeTelemetry.of(VectorIndexType.INT4_FLAT));
        assertSame(VectorIndexTypeTelemetry.BBQ, VectorIndexTypeTelemetry.of(VectorIndexType.BBQ_HNSW));
        assertSame(VectorIndexTypeTelemetry.BBQ, VectorIndexTypeTelemetry.of(VectorIndexType.BBQ_FLAT));
        assertSame(VectorIndexTypeTelemetry.BBQ, VectorIndexTypeTelemetry.of(VectorIndexType.BBQ_DISK));
    }

    public void testMergeNoneIsIdentity() {
        for (VectorIndexTypeTelemetry value : VectorIndexTypeTelemetry.values()) {
            assertSame(value, VectorIndexTypeTelemetry.NONE.merge(value));
            assertSame(value, value.merge(VectorIndexTypeTelemetry.NONE));
        }
    }

    public void testMergeSameBucket() {
        assertSame(VectorIndexTypeTelemetry.HNSW, VectorIndexTypeTelemetry.HNSW.merge(VectorIndexTypeTelemetry.HNSW));
        assertSame(VectorIndexTypeTelemetry.BBQ, VectorIndexTypeTelemetry.BBQ.merge(VectorIndexTypeTelemetry.BBQ));
    }

    public void testMergeDifferentBucketsCollapseToMixed() {
        assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.HNSW.merge(VectorIndexTypeTelemetry.FLAT));
        assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.HNSW.merge(VectorIndexTypeTelemetry.BBQ));
        assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.FLAT.merge(VectorIndexTypeTelemetry.BBQ));
        // UNRECOGNIZED combined with any other concrete bucket is genuinely mixed: we don't know what
        // the unrecognized side actually was, so we cannot claim agreement.
        assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.UNRECOGNIZED.merge(VectorIndexTypeTelemetry.HNSW));
        assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.HNSW.merge(VectorIndexTypeTelemetry.UNRECOGNIZED));
    }

    public void testMergeUnrecognizedWithUnrecognized() {
        assertSame(
            VectorIndexTypeTelemetry.UNRECOGNIZED,
            VectorIndexTypeTelemetry.UNRECOGNIZED.merge(VectorIndexTypeTelemetry.UNRECOGNIZED)
        );
    }

    public void testMergeMixedIsAbsorbing() {
        for (VectorIndexTypeTelemetry value : VectorIndexTypeTelemetry.values()) {
            if (value == VectorIndexTypeTelemetry.NONE) {
                continue;
            }
            assertSame(VectorIndexTypeTelemetry.MIXED, VectorIndexTypeTelemetry.MIXED.merge(value));
            assertSame(VectorIndexTypeTelemetry.MIXED, value.merge(VectorIndexTypeTelemetry.MIXED));
        }
    }
}
