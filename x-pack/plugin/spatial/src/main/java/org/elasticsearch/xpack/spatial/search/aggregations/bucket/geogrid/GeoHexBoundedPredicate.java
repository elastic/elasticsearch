/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.xpack.spatial.index.query.H3LatLonGeometry;

/**
 * Filters out geohex tiles using the provided bounds at the provided precision.
 */
public class GeoHexBoundedPredicate {

    private final boolean crossesDateline;
    private final GeoBoundingBox bbox;
    private final int precision;

    public GeoHexBoundedPredicate(int precision, GeoBoundingBox bbox) {
        this.crossesDateline = bbox.right() < bbox.left();
        this.bbox = bbox;
        this.precision = precision;
    }

    /** Check if the provided geohash intersects with the provided bounds. */
    public boolean validAddress(String address) {
        final H3LatLonGeom geometry = new H3LatLonGeom(address);
        Component2D component = geometry.toComponent2D();
        if (crossesDateline) {
            PointValues.Relation a = component.relate(-180, bbox.left(), bbox.bottom(), bbox.top());
            PointValues.Relation b = component.relate(bbox.right(), 180, bbox.bottom(), bbox.top());
            return a != PointValues.Relation.CELL_OUTSIDE_QUERY || b != PointValues.Relation.CELL_OUTSIDE_QUERY;
        } else {
            PointValues.Relation a = component.relate(bbox.left(), bbox.right(), bbox.bottom(), bbox.top());
            return a != PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
    }

    public long getMaxCells() {
        // TODO: Calculate correctly based on bounds
        return UnboundedGeoHexGridTiler.calcMaxAddresses(precision);
    }

    /** Extend class to get access to internal functions */
    static class H3LatLonGeom extends H3LatLonGeometry {

        H3LatLonGeom(String h3Address) {
            super(h3Address);
        }

        @Override
        protected Component2D toComponent2D() {
            return super.toComponent2D();
        }

        static class Scaled extends H3LatLonGeometry.Scaled {
            Scaled(String h3Address, double scaleFactor) {
                super(h3Address, scaleFactor);
            }

            @Override
            protected Component2D toComponent2D() {
                return super.toComponent2D();
            }
        }
    }
}
