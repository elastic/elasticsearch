/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.geo.GeoUtils.normalizePoint;

/**
 * Splits lines by datelines.
 */
public class GeoLineDecomposer {

    private static final double DATELINE = 180;

    private GeoLineDecomposer() {
        // no instances
    }

    public static void decomposeMultiLine(MultiLine multiLine, List<Line> collector) {
        for (Line line : multiLine) {
            decomposeLine(line, collector);
        }
    }

    /**
     * Splits the specified line by datelines and adds them to the supplied lines array
     */
    public static void decomposeLine(Line line, List<Line> collector) {
        if (line.isEmpty()) {
            return;
        }
        double[] lons = new double[line.length()];
        double[] lats = new double[lons.length];

        for (int i = 0; i < lons.length; i++) {
            double[] lonLat = new double[] {line.getX(i), line.getY(i)};
            normalizePoint(lonLat,false, true);
            lons[i] = lonLat[0];
            lats[i] = lonLat[1];
        }
        decompose(lons, lats, collector);
    }

    /**
     * Decompose a linestring given as array of coordinates by anti-meridian.
     */
    private static void decompose(double[] lons, double[] lats, List<Line> collector) {
        int offset = 0;

        double shift = 0;
        int i = 1;
        while (i < lons.length) {
            // Check where the line is going east (+1), west (-1) or directly north/south (0)
            int direction = Double.compare(lons[i], lons[i - 1]);
            double newShift = calculateShift(lons[i - 1], direction < 0);
            // first point lon + shift is always between -180.0 and +180.0
            if (i - offset > 1 && newShift != shift) {
                // Jumping over anti-meridian - we need to start a new segment
                double[] partLons = Arrays.copyOfRange(lons, offset, i);
                double[] partLats = Arrays.copyOfRange(lats, offset, i);
                performShift(shift, partLons);
                shift = newShift;
                offset = i - 1;
                collector.add(new Line(partLons, partLats));
            } else {
                // Check if new point intersects with anti-meridian
                shift = newShift;
                double t = intersection(lons[i - 1] + shift, lons[i] + shift);
                if (Double.isNaN(t) == false) {
                    // Found intersection, all previous segments are now part of the linestring
                    double[] partLons = Arrays.copyOfRange(lons, offset, i + 1);
                    double[] partLats = Arrays.copyOfRange(lats, offset, i + 1);
                    lons[i - 1] = partLons[partLons.length - 1] = (direction > 0 ? DATELINE : -DATELINE) - shift;
                    lats[i - 1] = partLats[partLats.length - 1] = lats[i - 1] + (lats[i] - lats[i - 1]) * t;
                    performShift(shift, partLons);
                    offset = i - 1;
                    collector.add(new Line(partLons, partLats));
                } else {
                    // Didn't find intersection - just continue checking
                    i++;
                }
            }
        }

        if (offset == 0) {
            performShift(shift, lons);
            collector.add(new Line(lons, lats));
        } else if (offset < lons.length - 1) {
            double[] partLons = Arrays.copyOfRange(lons, offset, lons.length);
            double[] partLats = Arrays.copyOfRange(lats, offset, lats.length);
            performShift(shift, partLons);
            collector.add(new Line(partLons, partLats));
        }
    }

    /**
     * shifts all coordinates by shift
     */
    private static void performShift(double shift, double[] lons) {
        if (shift != 0) {
            for (int j = 0; j < lons.length; j++) {
                lons[j] = lons[j] + shift;
            }
        }
    }

    /**
     * Calculates how many degres the given longitude needs to be moved east in order to be in -180 - +180. +180 is inclusive only
     * if include180 is true.
     */
    private static double calculateShift(double lon, boolean include180) {
        double normalized = GeoUtils.centeredModulus(lon, 360);
        double shift = Math.round(normalized - lon);
        if (include180 == false && normalized == 180.0) {
            shift = shift - 360;
        }
        return shift;
    }

    /**
     * Checks it the segment from p1x to p2x intersects with anti-meridian
     * p1x must be with in -180 +180 range
     */
    private static double intersection(double p1x, double p2x) {
        if (p1x == p2x) {
            return Double.NaN;
        }
        final double t = ((p1x < p2x ? DATELINE : -DATELINE) - p1x) / (p2x - p1x);
        if (t >= 1 || t <= 0) {
            return Double.NaN;
        } else {
            return t;
        }
    }
}
