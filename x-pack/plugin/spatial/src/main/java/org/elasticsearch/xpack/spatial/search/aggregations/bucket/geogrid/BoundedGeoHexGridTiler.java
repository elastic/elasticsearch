/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Rectangle;
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
    private final GeoHexVisitor visitor;
    private final int precision;
    private static final double FACTOR = 0.06;

    public BoundedGeoHexGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.bbox = bbox;
        this.visitor = new GeoHexVisitor();
        this.precision = precision;
        inflatedBbox = inflateBbox(precision, bbox);
    }

    private static GeoBoundingBox inflateBbox(int precision, GeoBoundingBox bbox) {
        /*
        * Here is the tricky part of this approach. We need to be able to filter cells at higher precisions
        * because they are not in bounds, but we need to make sure we don't filter too much. We use h3 bins at the given
        * resolution to check the height ands width at that level, and we factor it depending on the precision.
        *
        * The values have been tune using test GeoHexTilerTests#testLargeShapeWithBounds
        */
        final double factor = FACTOR * (1 << precision);
        final Rectangle minMin = H3CartesianUtil.toBoundingBox(H3.geoToH3(bbox.bottom(), bbox.left(), precision));
        final Rectangle maxMax = H3CartesianUtil.toBoundingBox(H3.geoToH3(bbox.top(), bbox.right(), precision));
        // compute height and width at the given precision
        final double height = Math.max(height(minMin), height(maxMax));
        final double width = Math.max(width(minMin), width(maxMax));
        // inflate the coordinates using the factor
        final double minY = Math.max(bbox.bottom() - factor * height, -90d);
        final double maxY = Math.min(bbox.top() + factor * height, 90d);
        final double left = GeoUtils.normalizeLon(bbox.left() - factor * width);
        final double right = GeoUtils.normalizeLon(bbox.right() + factor * width);
        if (2 * factor * width + width(bbox) >= 360d) {
            // if the total width bigger than the world, then it covers all longitude range.
            return new GeoBoundingBox(new GeoPoint(maxY, -180d), new GeoPoint(minY, 180d));
        } else {
            return new GeoBoundingBox(new GeoPoint(maxY, left), new GeoPoint(minY, right));
        }
    }

    private static double height(Rectangle rectangle) {
        return rectangle.getMaxY() - rectangle.getMinY();
    }

    private static double width(Rectangle rectangle) {
        if (rectangle.getMinX() > rectangle.getMaxX()) {
            return 360d + rectangle.getMaxX() - rectangle.getMinX();
        } else {
            return rectangle.getMaxX() - rectangle.getMinX();
        }
    }

    private static double width(GeoBoundingBox bbox) {
        if (bbox.left() > bbox.right()) {
            return 360d + bbox.right() - bbox.left();
        } else {
            return bbox.right() - bbox.left();
        }
    }

    @Override
    protected long getMaxCells() {
        // TODO: Calculate correctly based on bounds
        return UnboundedGeoHexGridTiler.calcMaxAddresses(precision);
    }

    @Override
    protected boolean h3IntersectsBounds(long h3) {
        visitor.reset(h3);
        final int resolution = H3.getResolution(h3);
        if (resolution != precision) {
            return cellIntersectsBounds(visitor, inflatedBbox);
        }
        return cellIntersectsBounds(visitor, bbox);
    }

    @Override
    protected GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException {
        visitor.reset(h3);
        final int resolution = H3.getResolution(h3);
        if (resolution != precision) {
            if (cellIntersectsBounds(visitor, inflatedBbox)) {
                // close to the poles, the properties of the H3 grid are lost because of the equirectangular projection,
                // therefore we cannot ensure that the relationship at this level make any sense in the next level.
                // Therefore, we just return CROSSES which just mean keep recursing.
                if (visitor.getMaxY() > H3CartesianUtil.getNorthPolarBound(resolution)
                    || visitor.getMinY() < H3CartesianUtil.getSouthPolarBound(resolution)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                geoValue.visit(visitor);
                return visitor.relation();
            } else {
                return GeoRelation.QUERY_DISJOINT;
            }
        }
        if (cellIntersectsBounds(visitor, bbox)) {
            geoValue.visit(visitor);
            return visitor.relation();
        }
        return GeoRelation.QUERY_DISJOINT;
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

    private static boolean cellIntersectsBounds(GeoHexVisitor visitor, GeoBoundingBox bbox) {
        return visitor.intersectsBbox(bbox.left(), bbox.right(), bbox.bottom(), bbox.top());
    }
}
