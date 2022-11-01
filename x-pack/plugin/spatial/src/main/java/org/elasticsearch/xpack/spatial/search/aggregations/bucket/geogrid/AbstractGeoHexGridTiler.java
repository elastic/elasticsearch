/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.H3LatLonGeometry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements most of the logic for the GeoHex aggregation.
 */
public abstract class AbstractGeoHexGridTiler extends GeoGridTiler {

    /** Scale factor to inflate an H3 tile by to ensure all children are covered */
    public static final double INFLATION_FACTOR = 1.17;

    AbstractGeoHexGridTiler(int precision) {
        super(precision);
    }

    /** check if the provided H3 address is in the solution space of this tiler */
    protected abstract boolean cellIntersectsBounds(String hash);

    /** check if the provided H3 address, after scaling, is in the solution space of this tiler */
    protected abstract boolean cellIntersectsBounds(String hash, double scaleFactor);

    @Override
    public long encode(double x, double y) {
        return H3.geoToH3(y, x, precision);
    }

    @Override
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // When the shape represents a point, we compute the address directly as we do it for GeoPoint
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            return setPointValue(values, H3.geoToH3Address(bounds.minY(), bounds.minX(), precision));
        }
        // TODO: test the H3 cells of the bounds at the specified precision, because we can optimize the search
        // if the bounds leads to neighbouring H3 cells, we won't need to to the recursive tree search. Using H3.areNeiughborCells.
        return setValuesByRecursion(values, geoValue);
    }

    /**
     * Iterate over all cells at the specified precision and collect all those that intersect the geometry.
     */
    protected int setValuesByBruteForce(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        for (String h3 : getAllCellsAt(precision)) {
            GeoRelation relation = relateTile(geoValue, h3);
            if (relation != GeoRelation.QUERY_DISJOINT) {
                values.resizeCell(values.docValueCount() + 1);
                values.add(values.docValueCount() - 1, H3.stringToH3(h3));
            }
        }
        return values.docValueCount();
    }

    private void addCells(ArrayList<String> cells, String h3, int precision, int depth) {
        if (depth == precision) {
            cells.add(h3);
        } else {
            for (String child : H3.h3ToChildren(h3)) {
                addCells(cells, child, precision, depth + 1);
            }
        }
    }

    protected List<String> getAllCellsAt(int precision) {
        ArrayList<String> cells = new ArrayList<>();
        for (String h3 : H3.getStringRes0Cells()) {
            addCells(cells, h3, precision, 0);
        }
        return cells;
    }

    /**
     * Recursively scan the H3 tree, assuming all children are fully contained in the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private void setAllValuesByRecursion(GeoShapeCellValues values, String h3, int precision) {
        if (precision == this.precision) {
            values.resizeCell(values.docValueCount() + 1);
            values.add(values.docValueCount() - 1, H3.stringToH3(h3));
        } else {
            // TODO: we should not need to search all top level cells, but rather use the bounds to find a subset using H3.asNeighborCells
            for (String child : H3.h3ToChildren(h3)) {
                setAllValuesByRecursion(values, child, precision + 1);
            }
        }
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    private void setValuesByRecursion(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue, String h3, int precision)
        throws IOException {
        if (precision <= this.precision) {
            if (precision == this.precision) {
                // When we're at the desired level, we want to test against the exact H3 cell
                GeoRelation relation = relateTile(geoValue, h3);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    values.resizeCell(values.docValueCount() + 1);
                    values.add(values.docValueCount() - 1, H3.stringToH3(h3));
                }
            } else {
                // When we're at higher tree levels, we want to test against slightly larger cells, to be sure to cover all child cells.
                GeoRelation relation = relateTileInflatedToCoverChildren(geoValue, h3);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    for (String child : H3.h3ToChildren(h3)) {
                        // TODO: determine case for optimization, probably only the central child cell, which is always the first child
                        if (relation == GeoRelation.QUERY_INSIDE && false) {
                            // Without this optimization the unbounded test slows down from 120ms to over 28seconds
                            setAllValuesByRecursion(values, child, precision + 1);
                        } else {
                            setValuesByRecursion(values, geoValue, child, precision + 1);
                        }
                    }
                }
            }
        }
    }

    private static String h3ToPolygon(String h3) {
        StringBuilder sb = new StringBuilder("POLYGON((");
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng point = boundary.getLatLon(i);
            if (i > 0) sb.append(", ");
            sb.append(point.getLonDeg()).append(" ").append(point.getLatDeg());
        }
        return sb.append("))").toString();
    }

    /**
     * Recursively search the H3 tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    protected int setValuesByRecursion(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        for (String h3 : H3.getStringRes0Cells()) {
            setValuesByRecursion(values, geoValue, h3, 0);
        }
        return values.docValueCount();
    }

    /**
     * Sets a singular doc-value for the {@link GeoShapeValues.GeoShapeValue}.
     */
    private int setPointValue(GeoShapeCellValues docValues, String addressOfPoint) {
        if (cellIntersectsBounds(addressOfPoint)) {
            docValues.resizeCell(1);
            docValues.add(0, H3.stringToH3(addressOfPoint));
            return 1;
        }
        return 0;
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, String addressOfTile) throws IOException {
        if (cellIntersectsBounds(addressOfTile)) {
            H3LatLonGeometry hexagon = new GeoHexBoundedPredicate.H3LatLonGeom(addressOfTile);
            return geoValue.relate(hexagon);
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    private GeoRelation relateTileInflatedToCoverChildren(GeoShapeValues.GeoShapeValue geoValue, String addressOfTile) throws IOException {
        if (cellIntersectsBounds(addressOfTile, INFLATION_FACTOR)) {
            H3LatLonGeometry hexagon = new GeoHexBoundedPredicate.H3LatLonGeom.Scaled(addressOfTile, INFLATION_FACTOR);
            return geoValue.relate(hexagon);
        }
        return GeoRelation.QUERY_DISJOINT;
    }
}
