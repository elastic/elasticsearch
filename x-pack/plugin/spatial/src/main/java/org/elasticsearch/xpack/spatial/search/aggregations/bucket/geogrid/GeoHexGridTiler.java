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
 * Implements the logic for the GeoHex aggregation over a geoshape doc value.
 */
public abstract class GeoHexGridTiler extends GeoGridTiler {

    private static final long[] RES0CELLS = H3.getLongRes0Cells();

    private GeoHexGridTiler(int precision) {
        super(precision);
    }

    /** Factory method to create GeoHexGridTiler objects */
    public static GeoHexGridTiler makeGridTiler(int precision, GeoBoundingBox geoBoundingBox) {
        return geoBoundingBox == null || geoBoundingBox.isUnbounded()
            ? new GeoHexGridTiler.UnboundedGeoHexGridTiler(precision)
            : new GeoHexGridTiler.BoundedGeoHexGridTiler(precision, geoBoundingBox);
    }

    /** check if the provided H3 bin is in the solution space of this tiler */
    protected abstract boolean h3IntersectsBounds(long h3);

    /** Return the relation between the H3 bin and the geoValue. If the h3 is out of the tiler solution (e.g.
     * {@link #h3IntersectsBounds(long)} is false), it should return {@link GeoRelation#QUERY_DISJOINT}
    */
    protected abstract GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException;

    /** Return true if the provided {@link GeoShapeValues.GeoShapeValue} is fully contained in our solution space.
     */
    protected abstract boolean valueInsideBounds(GeoShapeValues.GeoShapeValue geoValue) throws IOException;

    @Override
    public long encode(double x, double y) {
        // TODO: maybe we should remove this method from the API
        throw new IllegalArgumentException("no supported");
    }

    @Override
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        final GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();
        // first check if we are touching just fetch cells
        if (bounds.maxX() - bounds.minX() < 180d) {
            final long singleCell = boundsInSameCell(bounds, precision);
            if (singleCell > 0) {
                return setValuesFromPointResolution(singleCell, values, geoValue);
            }
            // TODO: specialize when they are neighbour cells.
        }
        // recurse tree
        return setValuesByRecursion(values, geoValue, bounds);
    }

    /**
     * It calls {@link #maybeAdd(long, GeoRelation, GeoShapeCellValues, int)} for {@code h3} and the neighbour cells if necessary.
     */
    private int setValuesFromPointResolution(long h3, GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        int valueIndex = 0;
        {
            final GeoRelation relation = relateTile(geoValue, h3);
            valueIndex = maybeAdd(h3, relation, values, valueIndex);
            if (relation == GeoRelation.QUERY_CONTAINS) {
                return valueIndex;
            }
        }
        // Point resolution is done using H3 library which uses spherical geometry. It might happen that in cartesian, the
        // actual point value is in a neighbour cell as well.
        {
            final int ringSize = H3.hexRingSize(h3);
            for (int i = 0; i < ringSize; i++) {
                final long n = H3.hexRingPosToH3(h3, i);
                final GeoRelation relation = relateTile(geoValue, n);
                valueIndex = maybeAdd(n, relation, values, valueIndex);
                if (relation == GeoRelation.QUERY_CONTAINS) {
                    return valueIndex;
                }
            }
        }
        return valueIndex;
    }

    private long boundsInSameCell(GeoShapeValues.BoundingBox bounds, int res) {
        final long minH3 = H3.geoToH3(bounds.minY(), bounds.minX(), res);
        final long maxH3 = H3.geoToH3(bounds.maxY(), bounds.maxX(), res);
        if (minH3 != maxH3) {
            // Normally sufficient to check only bottom-left against top-right
            return -1;
        }
        if (H3CartesianUtil.isPolar(minH3)) {
            // But with polar cells we must check the other two corners too
            final long minMax = H3.geoToH3(bounds.minY(), bounds.maxX(), res);
            final long maxMin = H3.geoToH3(bounds.maxY(), bounds.minX(), res);
            if (minMax != minH3 || maxMin != minH3) {
                return -1;
            }
        }
        // If all checks passed, we can use this cell in an optimization
        return minH3;
    }

    /**
     * Adds {@code h3} to {@link GeoShapeCellValues} if {@link #relateTile(GeoShapeValues.GeoShapeValue, long)} returns
     * a relation different to {@link GeoRelation#QUERY_DISJOINT}.
     */
    private int maybeAdd(long h3, GeoRelation relation, GeoShapeCellValues values, int valueIndex) {
        if (relation != GeoRelation.QUERY_DISJOINT) {
            values.resizeCell(valueIndex + 1);
            values.add(valueIndex++, h3);
        }
        return valueIndex;
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    // package private for testing
    int setValuesByRecursion(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue, GeoShapeValues.BoundingBox bounds)
        throws IOException {
        // NOTE: When we recurse, we cannot shortcut for CONTAINS relationship because it might fail when visiting noChilds.
        int valueIndex = 0;
        if (bounds.maxX() - bounds.minX() < 180d) {
            final long singleCell = boundsInSameCell(bounds, 0);
            if (singleCell > 0) {
                // When the level 0 bounds are within a single cell, we can search that cell and its immediate neighbours
                valueIndex = setValuesByRecursion(values, geoValue, singleCell, 0, valueIndex);
                final int ringSize = H3.hexRingSize(singleCell);
                for (int i = 0; i < ringSize; i++) {
                    valueIndex = setValuesByRecursion(values, geoValue, H3.hexRingPosToH3(singleCell, i), 0, valueIndex);
                }
                return valueIndex;
            }
            // TODO: specialize when they are neighbour cells.
        }
        for (long h3 : RES0CELLS) {
            valueIndex = setValuesByRecursion(values, geoValue, h3, 0, valueIndex);
        }
        return valueIndex;
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private int setValuesByRecursion(
        GeoShapeCellValues values,
        GeoShapeValues.GeoShapeValue geoValue,
        long h3,
        int precision,
        int valueIndex
    ) throws IOException {
        assert H3.getResolution(h3) == precision;
        final GeoRelation relation = relateTile(geoValue, h3);
        if (precision == this.precision) {
            // When we're at the desired level
            return maybeAdd(h3, relation, values, valueIndex);
        } else {
            assert precision < this.precision;
            // When we're at higher tree levels, check if we want to keep iterating.
            if (relation != GeoRelation.QUERY_DISJOINT) {
                int i = 0;
                if (relation == GeoRelation.QUERY_INSIDE) {
                    // H3 cells do not fully contain the children. The only one we know we fully contain
                    // is the center child which is always at position 0.
                    final long centerChild = H3.childPosToH3(h3, i++);
                    valueIndex = setAllValuesByRecursion(values, centerChild, precision + 1, valueIndex, valueInsideBounds(geoValue));
                }
                final int numChildren = H3.h3ToChildrenSize(h3);
                for (; i < numChildren; i++) {
                    final long child = H3.childPosToH3(h3, i);
                    valueIndex = setValuesByRecursion(values, geoValue, child, precision + 1, valueIndex);
                }
                // H3 cells do intersects with other cells that are not part of the children cells. If the parent cell of those
                // cells is disjoint, they will not be visited, therefore visit them here.
                final int numNoChildren = H3.h3ToNotIntersectingChildrenSize(h3);
                for (int j = 0; j < numNoChildren; j++) {
                    final long noChild = H3.noChildIntersectingPosToH3(h3, j);
                    if (relateTile(geoValue, H3.h3ToParent(noChild)) == GeoRelation.QUERY_DISJOINT) {
                        valueIndex = setValuesByRecursion(values, geoValue, noChild, precision + 1, valueIndex);
                    }
                }
            }
        }
        return valueIndex;
    }

    /**
     * Recursively scan the H3 tree, assuming all children are fully contained in the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private int setAllValuesByRecursion(GeoShapeCellValues values, long h3, int precision, int valueIndex, boolean valueInsideBounds) {
        if (valueInsideBounds || h3IntersectsBounds(h3)) {
            if (precision == this.precision) {
                values.resizeCell(valueIndex + 1);
                values.add(valueIndex++, h3);
            } else {
                final int numChildren = H3.h3ToChildrenSize(h3);
                for (int i = 0; i < numChildren; i++) {
                    valueIndex = setAllValuesByRecursion(values, H3.childPosToH3(h3, i), precision + 1, valueIndex, valueInsideBounds);
                }
            }
        }
        return valueIndex;
    }

    static long calcMaxAddresses(int precision) {
        // TODO: Verify this (and perhaps move the calculation into H3 and based on NUM_BASE_CELLS and others)
        final int baseHexagons = 110;
        final int basePentagons = 12;
        return baseHexagons * (long) Math.pow(7, precision) + basePentagons * (long) Math.pow(6, precision);
    }

    /**
     * Bounded geohex aggregation. It accepts H3 addresses that intersect the provided bounds.
     * The additional support for testing intersection with inflated bounds is used when testing
     * parent cells, since child cells can exceed the bounds of their parent. We inflate the bounds
     * by half of the width and half of the height.
     */
    static class BoundedGeoHexGridTiler extends GeoHexGridTiler {
        private final GeoBoundingBox[] inflatedBboxes;
        private final GeoBoundingBox bbox;
        private final GeoHexVisitor visitor;
        private final int resolution;
        private static final double FACTOR = 0.36;

        BoundedGeoHexGridTiler(int resolution, GeoBoundingBox bbox) {
            super(resolution);
            this.bbox = bbox;
            this.visitor = new GeoHexVisitor();
            this.resolution = resolution;
            inflatedBboxes = new GeoBoundingBox[resolution];
            for (int i = 0; i < resolution; i++) {
                inflatedBboxes[i] = inflateBbox(i, bbox, FACTOR);
            }
        }

        /**
         * Since H3 cells do not fully contain their child cells, we need to take care that when
         * filtering cells at a lower precision than the final precision, we must not exclude
         * parents that do not match the filter, but their own children or descendents might match.
         * For this reason the filter needs to be expanded to cover all descendent cells.
         *
         * This is done by taking the H3 cells at two corners, and expanding the filter width
         * by 35% of the max width of those cells, and filter height by 35% of the max height of those cells.
         *
         * The inflation factor of 35% has been verified using test GeoHexTilerTests#testLargeShapeWithBounds
         */
        static GeoBoundingBox inflateBbox(int precision, GeoBoundingBox bbox, double factor) {
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

        static double height(Rectangle rectangle) {
            return rectangle.getMaxY() - rectangle.getMinY();
        }

        static double width(Rectangle rectangle) {
            if (rectangle.getMinX() > rectangle.getMaxX()) {
                return 360d + rectangle.getMaxX() - rectangle.getMinX();
            } else {
                return rectangle.getMaxX() - rectangle.getMinX();
            }
        }

        static double width(GeoBoundingBox bbox) {
            if (bbox.left() > bbox.right()) {
                return 360d + bbox.right() - bbox.left();
            } else {
                return bbox.right() - bbox.left();
            }
        }

        @Override
        protected long getMaxCells() {
            // TODO: Calculate correctly based on bounds
            return UnboundedGeoHexGridTiler.calcMaxAddresses(resolution);
        }

        @Override
        protected boolean h3IntersectsBounds(long h3) {
            visitor.reset(h3);
            final int resolution = H3.getResolution(h3);
            if (resolution != this.resolution) {
                assert resolution < this.resolution;
                return cellIntersectsBounds(visitor, inflatedBboxes[resolution]);
            }
            return cellIntersectsBounds(visitor, bbox);
        }

        @Override
        protected GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException {
            visitor.reset(h3);
            final int resolution = H3.getResolution(h3);
            if (resolution != this.resolution) {
                assert resolution < this.resolution;
                if (cellIntersectsBounds(visitor, inflatedBboxes[resolution])) {
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

    /**
     * Unbounded geohex aggregation. It accepts any hash.
     */
    private static class UnboundedGeoHexGridTiler extends GeoHexGridTiler {

        private final long maxAddresses;

        private final GeoHexVisitor visitor;

        UnboundedGeoHexGridTiler(int precision) {
            super(precision);
            this.visitor = new GeoHexVisitor();
            maxAddresses = calcMaxAddresses(precision);
        }

        @Override
        protected boolean h3IntersectsBounds(long h3) {
            return true;
        }

        @Override
        protected GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException {
            visitor.reset(h3);
            final int resolution = H3.getResolution(h3);
            if (resolution != precision
                && (visitor.getMaxY() > H3CartesianUtil.getNorthPolarBound(resolution)
                    || visitor.getMinY() < H3CartesianUtil.getSouthPolarBound(resolution))) {
                // close to the poles, the properties of the H3 grid are lost because of the equirectangular projection,
                // therefore we cannot ensure that the relationship at this level make any sense in the next level.
                // Therefore, we just return CROSSES which just mean keep recursing.
                return GeoRelation.QUERY_CROSSES;
            }
            geoValue.visit(visitor);
            return visitor.relation();
        }

        @Override
        protected boolean valueInsideBounds(GeoShapeValues.GeoShapeValue geoValue) {
            return true;
        }

        @Override
        protected long getMaxCells() {
            return maxAddresses;
        }
    }

}
