/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.core.ESSloppyMath;
import org.elasticsearch.geometry.Rectangle;

/**
 * Utility functions to transforms WGS84 coordinates into spherical mercator.
 */
public class SphericalMercatorUtils {

    public static final double MERCATOR_BOUNDS = 20037508.34;
    private static final double MERCATOR_FACTOR = MERCATOR_BOUNDS / 180.0;
    // tangent lower limit. Below this limit the transformation result is -Infinity
    private static final double TAN_LOWER_LIMIT = Math.tan(Math.nextUp(0.0));

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
        final double cos = SloppyMath.cos((90 + lat) * Math.PI / 360);
        final double tan = Math.max(TAN_LOWER_LIMIT, Math.sqrt(1 - cos * cos) / cos);
        final double y = ESSloppyMath.log(tan) / (Math.PI / 180);
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
