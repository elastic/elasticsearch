/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.nio.ByteOrder;

/**
 * Focused byte-level tests for {@link CompoundOutputEvaluator.RowOutput#appendGeoPoint(double, double, int)}.
 * {@code appendGeoPoint} hand-rolls the WKB encoding on the per-row hot path instead of calling
 * {@link WellKnownBinary#toWKB(org.elasticsearch.geometry.Geometry, ByteOrder)}, so these tests pin its output
 * byte-for-byte to the canonical encoding (both {@code WellKnownBinary.toWKB} and
 * {@link SpatialCoordinateTypes#asWkb(org.elasticsearch.geometry.Geometry)}, which must agree with each other).
 */
public class CompoundOutputEvaluatorGeoPointTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testMatchesWellKnownBinaryForKnownPoints() {
        assertEncodingMatches(0.0, 0.0);
        assertEncodingMatches(51.5074, -0.1278); // London
        assertEncodingMatches(35.6895, 139.6917); // Tokyo
        assertEncodingMatches(-33.8688, 151.2093); // Sydney (southern hemisphere)
        assertEncodingMatches(40.7128, -74.0060); // New York (western hemisphere)
    }

    public void testMatchesWellKnownBinaryForBoundaryPoints() {
        for (double lat : new double[] { -90.0, 0.0, 90.0 }) {
            for (double lon : new double[] { -180.0, 0.0, 180.0 }) {
                assertEncodingMatches(lat, lon);
            }
        }
    }

    public void testMatchesWellKnownBinaryForEdgeValues() {
        // appendGeoPoint writes raw IEEE-754 little-endian doubles, so it must match WellKnownBinary even for
        // non-geographic edge values. new Point(x, y) is never "empty" and has no Z, so toWKB always produces a 21-byte
        // 2D point and never rejects these.
        assertEncodingMatches(-0.0, 0.0);
        assertEncodingMatches(0.0, -0.0);
        assertEncodingMatches(Double.NaN, Double.NaN);
        assertEncodingMatches(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        assertEncodingMatches(Double.MIN_VALUE, Double.MAX_VALUE);
        assertEncodingMatches(-Double.MAX_VALUE, -Double.MIN_VALUE);
    }

    public void testByteLayout() {
        double lat = randomDoubleBetween(-90, 90, true);
        double lon = randomDoubleBetween(-180, 180, true);
        BytesRef wkb = encode(lat, lon);
        assertEquals("WKB for a 2D point must be 21 bytes", 21, wkb.length);
        byte[] b = wkb.bytes;
        int o = wkb.offset;
        assertEquals("byte order must be little-endian (1)", (byte) 1, b[o]);
        assertEquals("geometry type must be Point (1) as int32 LE", 1, ByteUtils.readIntLE(b, o + 1));
        // x holds longitude, y holds latitude; compare raw bits to prove no precision loss.
        assertEquals(
            "x must hold longitude exactly",
            Double.doubleToLongBits(lon),
            Double.doubleToLongBits(ByteUtils.readDoubleLE(b, o + 5))
        );
        assertEquals(
            "y must hold latitude exactly",
            Double.doubleToLongBits(lat),
            Double.doubleToLongBits(ByteUtils.readDoubleLE(b, o + 13))
        );
    }

    public void testRandomizedParityWithWellKnownBinary() {
        for (int i = 0; i < 1000; i++) {
            assertEncodingMatches(randomDoubleBetween(-90, 90, true), randomDoubleBetween(-180, 180, true));
        }
    }

    public void testScratchBufferReuseAcrossRows() {
        // appendGeoPoint reuses a shared scratch buffer across rows; ensure successive encodings are independent and correct.
        CompoundOutputEvaluator.RowOutput rowOutput = new CompoundOutputEvaluator.RowOutput(1);
        double lat1 = 12.34, lon1 = -56.78;
        double lat2 = -87.65, lon2 = 43.21;
        BytesRef first = encodeWith(rowOutput, lat1, lon1);
        BytesRef second = encodeWith(rowOutput, lat2, lon2);
        assertEquals(canonical(lat1, lon1), first);
        assertEquals(canonical(lat2, lon2), second);
    }

    // ---- Helpers ----

    private void assertEncodingMatches(double lat, double lon) {
        assertEquals("WKB mismatch for lat=" + lat + " lon=" + lon, encode(lat, lon), canonical(lat, lon));
    }

    /**
     * Canonical WKB produced by the general-purpose utilities. The two oracles must agree with each other.
     */
    private static BytesRef canonical(double lat, double lon) {
        Point point = new Point(lon, lat);
        BytesRef viaWkb = new BytesRef(WellKnownBinary.toWKB(point, ByteOrder.LITTLE_ENDIAN));
        BytesRef viaSpatial = SpatialCoordinateTypes.GEO.asWkb(point);
        assertEquals("WellKnownBinary and SpatialCoordinateTypes must agree", viaWkb, viaSpatial);
        return viaWkb;
    }

    private BytesRef encode(double lat, double lon) {
        return encodeWith(new CompoundOutputEvaluator.RowOutput(1), lat, lon);
    }

    private BytesRef encodeWith(CompoundOutputEvaluator.RowOutput rowOutput, double lat, double lon) {
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1);
        rowOutput.startRow(new Block.Builder[] { builder });
        rowOutput.appendGeoPoint(lat, lon, 0);
        BytesRefBlock block = null;
        try {
            block = builder.build();
            // Copy out of the block so the result is safe to use after the block is released.
            return BytesRef.deepCopyOf(block.getBytesRef(0, new BytesRef()));
        } finally {
            Releasables.closeExpectNoException(builder, block);
        }
    }
}
