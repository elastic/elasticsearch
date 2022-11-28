/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.h3.H3;
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
    protected abstract boolean validH3(long h3);

    /** Return the relation between the H3 bin and the geoValue. If the h3 is out of the tiler solution (e.g.
     * {@link #validH3(long)} is false), it should return {@link GeoRelation#QUERY_DISJOINT}
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
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();
        // first check if we are touching just fetch cells
        if (bounds.maxX() - bounds.minX() < 180d) {
            long minH3 = H3.geoToH3(bounds.minY(), bounds.minX(), precision);
            long maxH3 = H3.geoToH3(bounds.maxY(), bounds.maxX(), precision);
            if (minH3 == maxH3) {
                return setValuesFromPointResolution(minH3, values, geoValue);
            }
            // TODO: specialize when they are neighbour cells.
        }
        // recurse tree
        return setValuesByRecursion(values, geoValue, bounds);
    }

    /**
     * It calls {@link #maybeAdd(long, GeoShapeCellValues, GeoShapeValues.GeoShapeValue, int)}  for{@code h3} and the
     * neighbour cells.
     */
    private int setValuesFromPointResolution(long h3, GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        int valueIndex = maybeAdd(h3, values, geoValue, 0);
        // Point resolution is done using H3 library which uses spherical geometry. It might happen that in cartesian, the
        // actual point value is in a neighbour cell as well.
        // TODO: if the H3 bin fully contains the geoValue and it does not touch any edge then we can stop here.
        // Earlier test shows a very important performance improvements for low resolutions.
        for (long n : H3.hexRing(h3)) {
            valueIndex = maybeAdd(n, values, geoValue, valueIndex);
        }
        return valueIndex;
    }

    /**
     * Adds {@code h3} to {@link GeoShapeCellValues} if {@link #relateTile(GeoShapeValues.GeoShapeValue, long)} returns
     * a relation different to {@link GeoRelation#QUERY_DISJOINT}.
     */
    // package private for testing
    int maybeAdd(long h3, GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue, int valueIndex) throws IOException {
        if (relateTile(geoValue, h3) != GeoRelation.QUERY_DISJOINT) {
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
        int valueIndex = 0;
        if (bounds.maxX() - bounds.minX() < 180d) {
            final long minH3 = H3.geoToH3(bounds.minY(), bounds.minX(), 0);
            final long maxH3 = H3.geoToH3(bounds.maxY(), bounds.maxX(), 0);
            if (minH3 == maxH3) {
                valueIndex = setValuesByRecursion(values, geoValue, minH3, 0, valueIndex);
                // TODO: if the H3 bin fully contains the geoValue and it does not touch any edge then we can stop here.
                for (long n : H3.hexRing(minH3)) {
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
        if (precision == this.precision) {
            // When we're at the desired level, we want to test against the exact H3 cell
            valueIndex = maybeAdd(h3, values, geoValue, valueIndex);
        } else {
            assert precision < this.precision;
            // When we're at higher tree levels, we want to test against slightly larger cells, to be sure to cover all child cells.
            final GeoRelation relation = relateTile(geoValue, h3);
            if (relation != GeoRelation.QUERY_DISJOINT) {
                final long[] children = H3.h3ToChildren(h3);
                int i = 0;
                if (relation == GeoRelation.QUERY_INSIDE) {
                    // H3 cells do not fully contain the children. The only one we know we fully contain
                    // is the center child which is always at position 0.
                    valueIndex = setAllValuesByRecursion(values, children[0], precision + 1, valueIndex, valueInsideBounds(geoValue));
                    i++;
                }
                for (; i < children.length; i++) {
                    valueIndex = setValuesByRecursion(values, geoValue, children[i], precision + 1, valueIndex);
                }
                // H3 cells do intersects with other cells that are not part of the children cells. If the parent cell of those
                // cells is disjoint, they will not be visited, therefore visit them here.
                for (long child : H3.h3ToNoChildrenIntersecting(h3)) {
                    if (relateTile(geoValue, H3.h3ToParent(child)) == GeoRelation.QUERY_DISJOINT) {
                        valueIndex = setValuesByRecursion(values, geoValue, child, precision + 1, valueIndex);
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
        if (valueInsideBounds || validH3(h3)) {
            if (precision == this.precision) {
                values.resizeCell(valueIndex + 1);
                values.add(valueIndex++, h3);
            } else {
                for (long child : H3.h3ToChildren(h3)) {
                    valueIndex = setAllValuesByRecursion(values, child, precision + 1, valueIndex, valueInsideBounds);
                }
            }
        }
        return valueIndex;
    }
}
