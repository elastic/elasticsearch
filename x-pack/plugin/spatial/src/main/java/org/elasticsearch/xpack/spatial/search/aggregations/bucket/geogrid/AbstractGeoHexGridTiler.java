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
import java.util.ArrayList;
import java.util.List;

/**
 * Implements most of the logic for the GeoHex aggregation.
 */
abstract class AbstractGeoHexGridTiler extends GeoGridTiler {

    AbstractGeoHexGridTiler(int precision) {
        super(precision);
    }

    /** check if the provided H3 address is in the solution space of this tiler */
    protected abstract boolean validAddress(String hash);

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
            return setValue(values, geoValue, H3.geoToH3Address(bounds.minX(), bounds.minY(), precision));
        }
        return setValuesByRecursion(values, geoValue);
    }

    /**
     * Iterate over all cells at the specified precision and collect all those that intersect the geometry.
     */
    protected int setValuesByBruteForce(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        for (String h3 : getAllCellsAt(precision)) {
            GeoHexBoundedPredicate.H3LatLonGeom hexagon = new GeoHexBoundedPredicate.H3LatLonGeom(h3);
            GeoRelation relation = geoValue.relate(hexagon);
            if (relation != GeoRelation.QUERY_DISJOINT) {
                values.resizeCell(values.docValueCount() + 1);
                values.add(values.docValueCount() + 1, H3.stringToH3(h3));
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

    private List<String> getAllCellsAt(int precision) {
        ArrayList<String> cells = new ArrayList<>((int) this.getMaxCells());
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
            values.add(values.docValueCount() + 1, H3.stringToH3(h3));
        } else {
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
            GeoHexBoundedPredicate.H3LatLonGeom hexagon = new GeoHexBoundedPredicate.H3LatLonGeom(h3);
            GeoRelation relation = geoValue.relate(hexagon);
            if (relation != GeoRelation.QUERY_DISJOINT) {
                if (precision == this.precision) {
                    values.resizeCell(values.docValueCount() + 1);
                    values.add(values.docValueCount() + 1, H3.stringToH3(h3));
                } else {
                    for (String child : H3.h3ToChildren(h3)) {
                        if (relation == GeoRelation.QUERY_INSIDE) {
                            setAllValuesByRecursion(values, child, precision + 1);
                        } else {
                            setValuesByRecursion(values, geoValue, child, precision + 1);
                        }
                    }
                }
            }
        }
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
    protected int setValue(GeoShapeCellValues docValues, GeoShapeValues.GeoShapeValue geoValue, String address) throws IOException {
        if (relateTile(geoValue, address) != GeoRelation.QUERY_DISJOINT) {
            docValues.resizeCell(1);
            docValues.add(0, H3.stringToH3(address));
            return 1;
        }
        return 0;
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, String address) throws IOException {
        if (validAddress(address)) {
            GeoHexBoundedPredicate.H3LatLonGeom hexagon = new GeoHexBoundedPredicate.H3LatLonGeom(address);
            return geoValue.relate(hexagon);
        }
        return GeoRelation.QUERY_DISJOINT;
    }
}
