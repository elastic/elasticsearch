/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import org.elasticsearch.geometry.Rectangle;

/**
 * Utility functions to transforms WGS84 coordinates into spherical mercator.
 */
class SphericalMercatorUtils {

    private static double MERCATOR_FACTOR = 20037508.34 / 180.0;

    /**
     * Transforms WGS84 longitude to a Spherical mercator longitude
     */
    public static double lonToSphericalMercator(double lon) {
        return lon * MERCATOR_FACTOR;
    }

    /**
     * Transforms WGS84 latitude to a Spherical mercator latitude
     */
    public static double latToSphericalMercator(double lat) {
        double y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180);
        return y * MERCATOR_FACTOR;
    }

    /**
     * Transforms WGS84 rectangle to a Spherical mercator rectangle
     */
    public static Rectangle recToSphericalMercator(Rectangle r) {
        return new Rectangle(
            lonToSphericalMercator(r.getMinLon()),
            lonToSphericalMercator(r.getMaxLon()),
            latToSphericalMercator(r.getMaxLat()),
            latToSphericalMercator(r.getMinLat())
        );

    }

    private SphericalMercatorUtils() {
        // no instances
    }
}
