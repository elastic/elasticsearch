/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Line;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class GeoLineDecomposerTests extends ESTestCase {

    public void testEmptyLineIsNoOp() {
        List<Line> collector = new ArrayList<>();
        GeoLineDecomposer.decomposeLine(Line.EMPTY, collector);
        assertEquals(0, collector.size());
    }

    public void testSimpleLineIsKeptAsSingleSegment() {
        Line line = new Line(new double[] { 10, 20, 30 }, new double[] { 1, 2, 3 });
        List<Line> collector = new ArrayList<>();
        GeoLineDecomposer.decomposeLine(line, collector);
        assertEquals(1, collector.size());
        assertArrayEquals(new double[] { 10, 20, 30 }, collector.get(0).getX(), 0.0);
        assertArrayEquals(new double[] { 1, 2, 3 }, collector.get(0).getY(), 0.0);
    }

    public void testCrossingDatelineIsSplit() {
        Line line = new Line(new double[] { 160, 200 }, new double[] { 10, 20 });
        List<Line> collector = new ArrayList<>();
        GeoLineDecomposer.decomposeLine(line, collector);
        assertEquals(2, collector.size());
        assertArrayEquals(new double[] { 160, 180 }, collector.get(0).getX(), 0.0);
        assertArrayEquals(new double[] { -180, -160 }, collector.get(1).getX(), 0.0);
    }

    /**
     * Longitudes in (180, 360] and multi-revolution encodings up to ±1e6 are legitimate
     * cross-dateline inputs (e.g. lon=200 == -160 wrapped, lon=900 == -180+180*3).
     * The validator must not reject them.
     */
    public void testAllowsLongitudeUpTo360() {
        List<Line> collector = new ArrayList<>();
        GeoLineDecomposer.decomposeLine(new Line(new double[] { 10, 200 }, new double[] { 1, 2 }), collector);
        assertFalse(collector.isEmpty());
    }

    public void testAllowsMultiRevolutionLongitude() {
        // lon=900 = 2.5 revolutions, still a legitimate cross-dateline encoding
        List<Line> collector = new ArrayList<>();
        GeoLineDecomposer.decomposeLine(new Line(new double[] { 10, 900 }, new double[] { 1, 2 }), collector);
        assertFalse(collector.isEmpty());
    }

    /**
     * Reproduces the OOM seen in the field: a LineString whose second longitude is a corrupted
     * value (~-4.14e19) drives the dateline-decomposing loop to add hundreds of millions of
     * 2-point Lines before exiting (~350 million objects, 24 GB retained, node OOMed in ~3s).
     * The fix must reject the document fast with an IllegalArgumentException.
     */
    public void testRejectsExtremeLongitude() {
        Line line = new Line(
            new double[] { 116.5037459137329, -4.142431633325595e19 },
            new double[] { 39.793895444619, 71.0 }
        );
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> GeoLineDecomposer.decomposeLine(line, new ArrayList<>())
        );
        assertTrue(ex.getMessage(), ex.getMessage().startsWith("invalid LineString coordinate"));
    }

    public void testRejectsNonFiniteCoordinates() {
        for (double bad : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
            expectThrows(IllegalArgumentException.class, () ->
                GeoLineDecomposer.decomposeLine(new Line(new double[] { bad, 10 }, new double[] { 1, 2 }), new ArrayList<>()));
            expectThrows(IllegalArgumentException.class, () ->
                GeoLineDecomposer.decomposeLine(new Line(new double[] { 10, 20 }, new double[] { bad, 2 }), new ArrayList<>()));
        }
    }

    public void testRejectsLongitudeBeyondOneMillion() {
        expectThrows(IllegalArgumentException.class, () ->
            GeoLineDecomposer.decomposeLine(new Line(new double[] { 10, 1e6 + 1 }, new double[] { 1, 2 }), new ArrayList<>()));
    }
}
