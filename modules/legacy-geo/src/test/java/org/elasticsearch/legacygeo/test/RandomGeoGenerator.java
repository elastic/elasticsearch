/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.test;

import org.elasticsearch.common.geo.GeoPoint;

import java.util.Random;

/**
 * Random geo generation utilities for randomized {@code geo_point} type testing
 * does not depend on jts or spatial4j. Use {@link org.elasticsearch.legacygeo.test.RandomShapeGenerator}
 * to create random OGC compliant shapes.
 */
public class RandomGeoGenerator {

    public static void randomPoint(Random r, double[] pt) {
        final double[] min = { -180, -90 };
        final double[] max = { 180, 90 };
        randomPointIn(r, min[0], min[1], max[0], max[1], pt);
    }

    public static void randomPointIn(
        Random r,
        final double minLon,
        final double minLat,
        final double maxLon,
        final double maxLat,
        double[] pt
    ) {
        assert pt != null && pt.length == 2;

        // normalize min and max
        double[] min = { normalizeLongitude(minLon), normalizeLatitude(minLat) };
        double[] max = { normalizeLongitude(maxLon), normalizeLatitude(maxLat) };
        final double[] tMin = new double[2];
        final double[] tMax = new double[2];
        tMin[0] = Math.min(min[0], max[0]);
        tMax[0] = Math.max(min[0], max[0]);
        tMin[1] = Math.min(min[1], max[1]);
        tMax[1] = Math.max(min[1], max[1]);

        pt[0] = tMin[0] + r.nextDouble() * (tMax[0] - tMin[0]);
        pt[1] = tMin[1] + r.nextDouble() * (tMax[1] - tMin[1]);
    }

    public static GeoPoint randomPoint(Random r) {
        return randomPointIn(r, -180, -90, 180, 90);
    }

    public static GeoPoint randomPointIn(Random r, final double minLon, final double minLat, final double maxLon, final double maxLat) {
        double[] pt = new double[2];
        randomPointIn(r, minLon, minLat, maxLon, maxLat, pt);
        return new GeoPoint(pt[1], pt[0]);
    }

    /** Puts latitude in range of -90 to 90. */
    private static double normalizeLatitude(double latitude) {
        if (latitude >= -90 && latitude <= 90) {
            return latitude; // common case, and avoids slight double precision shifting
        }
        double off = Math.abs((latitude + 90) % 360);
        return (off <= 180 ? off : 360 - off) - 90;
    }

    /** Puts longitude in range of -180 to +180. */
    private static double normalizeLongitude(double longitude) {
        if (longitude >= -180 && longitude <= 180) {
            return longitude; // common case, and avoids slight double precision shifting
        }
        double off = (longitude + 180) % 360;
        if (off < 0) {
            return 180 + off;
        } else if (off == 0 && longitude > 0) {
            return 180;
        } else {
            return -180 + off;
        }
    }
}
