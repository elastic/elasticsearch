/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.H3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Computes the H3 cells intersecting a geo_shape, given as its indexed triangle tree.
 * <p>
 * Adapted from {@code GeoHexGridTiler} in the spatial module, which backs the {@code geohex_grid} aggregation, so that
 * ES|QL produces the same cells with the same performance characteristics. The search starts from the bounds of the
 * shape: a shape that fits in a single cell at the target resolution only needs that cell and its ring checked, and
 * otherwise only the resolution 0 cell containing the bounds and its ring are recursed rather than all 122. During the
 * recursion a cell found to lie inside the shape adds all its descendants without further geometry tests.
 * <p>
 * Differences from the original: cells are collected into a list rather than doc values, they are not sorted, and the
 * collection stops at a caller supplied limit with a callback so the caller can warn about the truncation. The tiler
 * keeps a scratch {@link GeoHexVisitor}, so an instance must not be shared between threads.
 */
public abstract class GeoHexGridTiler {

    private static final long[] RES0CELLS = H3.getLongRes0Cells();

    protected final int precision;
    protected final GeoHexVisitor visitor = new GeoHexVisitor();

    private GeoHexGridTiler(int precision) {
        this.precision = precision;
    }

    /** Factory method to create GeoHexGridTiler objects */
    public static GeoHexGridTiler makeGridTiler(int precision, GeoBoundingBox geoBoundingBox) {
        return geoBoundingBox == null || geoBoundingBox.isUnbounded()
            ? new UnboundedGeoHexGridTiler(precision)
            : new BoundedGeoHexGridTiler(precision, geoBoundingBox);
    }

    /** returns the precision of this tiler */
    public int precision() {
        return precision;
    }

    /** check if the provided H3 bin is in the solution space of this tiler */
    protected abstract boolean h3IntersectsBounds(long h3);

    /**
     * Return the relation between the H3 bin and the shape. If the h3 is out of the tiler solution (e.g.
     * {@link #h3IntersectsBounds(long)} is false), it should return {@link GeoRelation#QUERY_DISJOINT}
     */
    protected abstract GeoRelation relateTile(GeoShapeDocValues shape, long h3) throws IOException;

    /** Return true if the provided shape is fully contained in our solution space. */
    protected abstract boolean valueInsideBounds(GeoShapeDocValues shape);

    /**
     * The cells at this tiler's precision that intersect the shape, in the order they are found, at most
     * {@code maxCells} of them. When the limit is reached {@code onTruncation} is called once with a message and
     * the partial list is returned.
     */
    public List<Long> cells(GeoShapeDocValues shape, int maxCells, Consumer<String> onTruncation) throws IOException {
        Cells cells = new Cells(maxCells, onTruncation);
        assert shape.minLon() <= shape.maxLon();
        // first check if we are touching just fetch cells
        if (shape.maxLon() - shape.minLon() < 180d) {
            final long singleCell = boundsInSameCell(shape, precision);
            if (singleCell > 0) {
                setValuesFromPointResolution(singleCell, cells, shape);
                return cells.list;
            }
            // TODO: specialize when they are neighbour cells.
        }
        // recurse tree
        setValuesByRecursion(cells, shape);
        return cells.list;
    }

    /**
     * It calls {@link #maybeAdd(long, GeoRelation, Cells)} for {@code h3} and the neighbour cells if necessary.
     */
    private void setValuesFromPointResolution(long h3, Cells cells, GeoShapeDocValues shape) throws IOException {
        {
            final GeoRelation relation = relateTile(shape, h3);
            maybeAdd(h3, relation, cells);
            if (relation == GeoRelation.QUERY_CONTAINS || cells.full()) {
                return;
            }
        }
        // Point resolution is done using H3 library which uses spherical geometry. It might happen that in cartesian, the
        // actual point value is in a neighbour cell as well.
        {
            final int ringSize = H3.hexRingSize(h3);
            for (int i = 0; i < ringSize; i++) {
                final long n = H3.hexRingPosToH3(h3, i);
                final GeoRelation relation = relateTile(shape, n);
                maybeAdd(n, relation, cells);
                if (relation == GeoRelation.QUERY_CONTAINS || cells.full()) {
                    return;
                }
            }
        }
    }

    private static long boundsInSameCell(GeoShapeDocValues shape, int res) {
        final long minH3 = H3.geoToH3(shape.minLat(), shape.minLon(), res);
        final long maxH3 = H3.geoToH3(shape.maxLat(), shape.maxLon(), res);
        if (minH3 != maxH3) {
            // Normally sufficient to check only bottom-left against top-right
            return -1;
        }
        if (H3CartesianUtil.isPolar(minH3)) {
            // But with polar cells we must check the other two corners too
            final long minMax = H3.geoToH3(shape.minLat(), shape.maxLon(), res);
            final long maxMin = H3.geoToH3(shape.maxLat(), shape.minLon(), res);
            if (minMax != minH3 || maxMin != minH3) {
                return -1;
            }
        }
        // If all checks passed, we can use this cell in an optimization
        return minH3;
    }

    /**
     * Adds {@code h3} to the cells if {@link #relateTile(GeoShapeDocValues, long)} returned a relation different to
     * {@link GeoRelation#QUERY_DISJOINT}.
     */
    private static void maybeAdd(long h3, GeoRelation relation, Cells cells) {
        if (relation != GeoRelation.QUERY_DISJOINT) {
            cells.add(h3);
        }
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    // package private for testing
    void setValuesByRecursion(Cells cells, GeoShapeDocValues shape) throws IOException {
        // NOTE: When we recurse, we cannot shortcut for CONTAINS relationship because it might fail when visiting noChilds.
        if (shape.maxLon() - shape.minLon() < 180d) {
            final long singleCell = boundsInSameCell(shape, 0);
            if (singleCell > 0) {
                // When the level 0 bounds are within a single cell, we can search that cell and its immediate neighbours
                setValuesByRecursion(cells, shape, singleCell, 0);
                final int ringSize = H3.hexRingSize(singleCell);
                for (int i = 0; i < ringSize && cells.full() == false; i++) {
                    setValuesByRecursion(cells, shape, H3.hexRingPosToH3(singleCell, i), 0);
                }
                return;
            }
            // TODO: specialize when they are neighbour cells.
        }
        for (long h3 : RES0CELLS) {
            if (cells.full()) {
                return;
            }
            setValuesByRecursion(cells, shape, h3, 0);
        }
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private void setValuesByRecursion(Cells cells, GeoShapeDocValues shape, long h3, int precision) throws IOException {
        assert H3.getResolution(h3) == precision;
        if (cells.full()) {
            return;
        }
        final GeoRelation relation = relateTile(shape, h3);
        if (precision == this.precision) {
            // When we're at the desired level
            maybeAdd(h3, relation, cells);
        } else {
            assert precision < this.precision;
            // When we're at higher tree levels, check if we want to keep iterating.
            if (relation != GeoRelation.QUERY_DISJOINT) {
                int i = 0;
                if (relation == GeoRelation.QUERY_INSIDE) {
                    // H3 cells do not fully contain the children. The only one we know we fully contain
                    // is the center child which is always at position 0.
                    final long centerChild = H3.childPosToH3(h3, i++);
                    setAllValuesByRecursion(cells, centerChild, precision + 1, valueInsideBounds(shape));
                }
                final int numChildren = H3.h3ToChildrenSize(h3);
                for (; i < numChildren && cells.full() == false; i++) {
                    final long child = H3.childPosToH3(h3, i);
                    setValuesByRecursion(cells, shape, child, precision + 1);
                }
                // H3 cells do intersects with other cells that are not part of the children cells. If the parent cell of those
                // cells is disjoint, they will not be visited, therefore visit them here.
                final int numNoChildren = H3.h3ToNotIntersectingChildrenSize(h3);
                for (int j = 0; j < numNoChildren && cells.full() == false; j++) {
                    final long noChild = H3.noChildIntersectingPosToH3(h3, j);
                    if (relateTile(shape, H3.h3ToParent(noChild)) == GeoRelation.QUERY_DISJOINT) {
                        setValuesByRecursion(cells, shape, noChild, precision + 1);
                    }
                }
            }
        }
    }

    /**
     * Recursively scan the H3 tree, assuming all children are fully contained in the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private void setAllValuesByRecursion(Cells cells, long h3, int precision, boolean valueInsideBounds) {
        if (cells.full()) {
            return;
        }
        if (valueInsideBounds || h3IntersectsBounds(h3)) {
            if (precision == this.precision) {
                cells.add(h3);
            } else {
                final int numChildren = H3.h3ToChildrenSize(h3);
                for (int i = 0; i < numChildren && cells.full() == false; i++) {
                    setAllValuesByRecursion(cells, H3.childPosToH3(h3, i), precision + 1, valueInsideBounds);
                }
            }
        }
    }

    /** Collects cells up to a limit, warning once through the callback when the limit is reached. */
    static final class Cells {
        private final List<Long> list = new ArrayList<>();
        private final int maxCells;
        private final Consumer<String> onTruncation;
        private boolean full;

        Cells(int maxCells, Consumer<String> onTruncation) {
            this.maxCells = maxCells;
            this.onTruncation = onTruncation;
        }

        void add(long h3) {
            if (full) {
                return;
            }
            if (list.size() >= maxCells) {
                full = true;
                onTruncation.accept("ST_GEOHEX generated more than " + maxCells + " grid cells");
                return;
            }
            list.add(h3);
        }

        boolean full() {
            return full;
        }

        List<Long> list() {
            return list;
        }
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
        private final int resolution;
        private static final double FACTOR = 0.37;

        BoundedGeoHexGridTiler(int resolution, GeoBoundingBox bbox) {
            super(resolution);
            this.bbox = bbox;
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
        protected GeoRelation relateTile(GeoShapeDocValues shape, long h3) throws IOException {
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
                    shape.visit(visitor);
                    return visitor.relation();
                } else {
                    return GeoRelation.QUERY_DISJOINT;
                }
            }
            if (cellIntersectsBounds(visitor, bbox)) {
                shape.visit(visitor);
                return visitor.relation();
            }
            return GeoRelation.QUERY_DISJOINT;
        }

        @Override
        protected boolean valueInsideBounds(GeoShapeDocValues shape) {
            if (bbox.bottom() <= shape.minLat() && bbox.top() >= shape.maxLat()) {
                if (bbox.right() < bbox.left()) {
                    return bbox.left() <= shape.minLon() || bbox.right() >= shape.maxLon();
                } else {
                    return bbox.left() <= shape.minLon() && bbox.right() >= shape.maxLon();
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

        UnboundedGeoHexGridTiler(int precision) {
            super(precision);
        }

        @Override
        protected boolean h3IntersectsBounds(long h3) {
            return true;
        }

        @Override
        protected GeoRelation relateTile(GeoShapeDocValues shape, long h3) throws IOException {
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
            shape.visit(visitor);
            return visitor.relation();
        }

        @Override
        protected boolean valueInsideBounds(GeoShapeDocValues shape) {
            return true;
        }
    }
}
