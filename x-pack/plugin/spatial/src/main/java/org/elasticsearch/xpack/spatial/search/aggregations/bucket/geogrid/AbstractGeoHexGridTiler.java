/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;

/**
 * Implements most of the logic for the GeoHex aggregation.
 */
abstract class AbstractGeoHexGridTiler extends GeoGridTiler {

    private static final long[] RES0CELLS = H3.getLongRes0Cells();

    AbstractGeoHexGridTiler(int precision) {
        super(precision);
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
            for (long n : H3.hexRing(h3)) {
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
                for (long n : H3.hexRing(singleCell)) {
                    valueIndex = setValuesByRecursion(values, geoValue, n, 0, valueIndex);
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
}
