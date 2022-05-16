/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * Base class to help convert {@link MultiGeoPointValues} to {@link CellValues}
 */
public abstract class CellIdSource extends ValuesSource.Numeric {

    private final GeoPoint valuesSource;
    private final int precision;
    private final GeoBoundingBox geoBoundingBox;

    protected CellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
    }

    protected final int precision() {
        return precision;
    }

    @Override
    public final boolean isFloatingPoint() {
        return false;
    }

    @Override
    public final SortedNumericDocValues longValues(LeafReaderContext ctx) {
        final MultiGeoPointValues multiGeoPointValues = valuesSource.geoPointValues(ctx);

        if (geoBoundingBox.isUnbounded()) {
            return unboundedCellValues(multiGeoPointValues);
        } else {
            return boundedCellValues(multiGeoPointValues, geoBoundingBox);
        }
    }

    /**
     * Generate an unbounded iterator of grid-cells
     */
    protected abstract CellValues unboundedCellValues(MultiGeoPointValues values);

    /**
     * Generate a bounded iterator of grid-cells
     */
    protected abstract CellValues boundedCellValues(MultiGeoPointValues values, GeoBoundingBox boundingBox);

    @Override
    public final SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

}
