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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;

/**
 * Bounded geohex aggregation. It accepts H3 addresses that intersect the provided bounds.
 * The additional support for testing intersection with inflated bounds is used when testing
 * parent cells, since child cells can exceed the bounds of their parent. We inflate the bounds
 * by half of the width and half of the height.
 */
public class BoundedGeoHexGridTiler extends AbstractGeoHexGridTiler {
    private final GeoBoundingBox inflatedBbox;
    private final GeoBoundingBox bbox;
    private final int precision;
    private static final double FACTOR = 0.5;

    public BoundedGeoHexGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.bbox = bbox;
        this.precision = precision;
        final double height = bbox.top() - bbox.bottom();
        final double minY = Math.max(bbox.bottom() - FACTOR * height, -90d);
        final double maxY = Math.min(bbox.top() + FACTOR * height, 90d);
        final double width = Math.abs(bbox.right() - bbox.left());
        final double minX = bbox.left() - FACTOR * width;
        final double maxX = bbox.right() + FACTOR * width;
        // TODO: I think we can do much better when the inflated bounding box goes across the dateline
        if (precision <= 2) {
            inflatedBbox = new GeoBoundingBox(new GeoPoint(90d, -180d), new GeoPoint(-90d, 180d));
        } else if (bbox.right() < bbox.left() || (minX < -180d || maxX > 180d)) {
            inflatedBbox = new GeoBoundingBox(new GeoPoint(maxY, -180d), new GeoPoint(minY, 180d));
        } else {
            inflatedBbox = new GeoBoundingBox(new GeoPoint(maxY, minX), new GeoPoint(minY, maxX));
        }
    }

    @Override
    protected long getMaxCells() {
        // TODO: Calculate correctly based on bounds
        return UnboundedGeoHexGridTiler.calcMaxAddresses(precision);
    }

    @Override
    protected boolean validH3(long h3) {
        final Component2D component2D = H3CartesianUtil.getComponent(h3);
        final int resolution = H3.getResolution(h3);
        if (resolution != precision) {
            return cellIntersectsBounds(component2D, inflatedBbox);
        }
        return cellIntersectsBounds(component2D, bbox);
    }

    @Override
    protected GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException {
        final Component2D component2D = H3CartesianUtil.getComponent(h3);
        final int resolution = H3.getResolution(h3);
        if (resolution != precision) {
            if (cellIntersectsBounds(component2D, inflatedBbox)) {
                // close to the poles, the properties of the H3 grid are lost because of the equirectangular projection,
                // therefore we cannot ensure that the relationship at this level make any sense in the next level.
                // Therefore, we just return CROSSES which just mean keep recursing.
                if (component2D.getMaxY() > H3CartesianUtil.getNorthPolarBound(resolution)
                    || component2D.getMinY() < H3CartesianUtil.getSouthPolarBound(resolution)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                return geoValue.relate(component2D);
            } else {
                return GeoRelation.QUERY_DISJOINT;
            }
        }
        return cellIntersectsBounds(component2D, bbox) ? geoValue.relate(component2D) : GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected boolean valueInsideBounds(GeoShapeValues.GeoShapeValue geoValue) {
        if (bbox.bottom() <= geoValue.boundingBox().minY() && bbox.top() >= geoValue.boundingBox().maxY()) {
            if (bbox.right() < bbox.left()) {
                return bbox.left() <= geoValue.boundingBox().minX() || bbox.right() >= geoValue.boundingBox().maxX();
            } else {
                return bbox.left() <= geoValue.boundingBox().minX() && bbox.right() >= geoValue.boundingBox().maxX();
            }
        }
        return false;
    }

    private static boolean cellIntersectsBounds(Component2D component, GeoBoundingBox bbox) {
        if (bbox.right() < bbox.left()) {
            PointValues.Relation a = component.relate(bbox.left(), 180, bbox.bottom(), bbox.top());
            PointValues.Relation b = component.relate(-180, bbox.right(), bbox.bottom(), bbox.top());
            return a != PointValues.Relation.CELL_OUTSIDE_QUERY || b != PointValues.Relation.CELL_OUTSIDE_QUERY;
        } else {
            PointValues.Relation a = component.relate(bbox.left(), bbox.right(), bbox.bottom(), bbox.top());
            return a != PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
    }
}
