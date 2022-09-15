/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.lucene.spatial3d.geom;

/**
 * Class which constructs a GeoPolygon representing a regular simple convex polygon, for example an S2 google pixel, or an H3 uber tile.
 */
public class GeoRegularConvexPolygonFactory {

    private GeoRegularConvexPolygonFactory() {}

    /**
     * Creates a convex polygon with N planes by providing N points in CCW. This is a very fast shape
     * and there are no checks that the points currently define a convex shape. The last point should not be a copy of the first point.
     *
     * @param planetModel The planet model
     * @param points an array of at least three points in CCW orientation.
     * @return the generated shape.
     */
    public static GeoRegularConvexPolygon makeGeoPolygon(final PlanetModel planetModel, final GeoPoint... points) {
        return new GeoRegularConvexPolygon(planetModel, points);
    }
}
