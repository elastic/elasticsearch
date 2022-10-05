/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.lucene.spatial3d.geom;

public class Spatial3DTestUtil {
    /**
     * Test whether a collection of points has any two contiguous points that have a very similar latitude
     * while also having a different longitude
     */
    public static boolean containsHorizontalLine(GeoPoint[] points) {
        boolean ans = false;
        for (int i = 0; i < points.length; i++) {
            int prev = (i == 0) ? points.length - 1 : i - 1;
            double latDiff = Math.abs(points[i].latitude - points[prev].latitude);
            double lonDiff = Math.abs(points[i].longitude - points[prev].longitude);
            ans |= latDiff < 0.001 && lonDiff > 0.001;
        }
        return ans;
    }
}
