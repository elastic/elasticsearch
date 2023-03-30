/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Base class to help convert {@link MultiGeoPointValues} to {@link CellMultiValues}
 * and {@link GeoPointValues} to {@link CellSingleValue}
 */
public abstract class CellIdSource extends ValuesSource.Numeric {

    private final GeoPoint valuesSource;
    private final int precision;
    private final GeoBoundingBox geoBoundingBox;
    private final boolean crossesDateline;

    protected final LongConsumer circuitBreakerConsumer;

    protected CellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox, LongConsumer circuitBreakerConsumer) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
        this.crossesDateline = geoBoundingBox.left() > geoBoundingBox.right();
        this.circuitBreakerConsumer = circuitBreakerConsumer;
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
        final GeoPointValues values = org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(multiGeoPointValues);
        if (geoBoundingBox.isUnbounded()) {
            return values == null ? unboundedCellMultiValues(multiGeoPointValues) : DocValues.singleton(unboundedCellSingleValue(values));
        } else {
            return values == null
                ? boundedCellMultiValues(multiGeoPointValues, geoBoundingBox)
                : DocValues.singleton(boundedCellSingleValue(values, geoBoundingBox));
        }
    }

    /**
     * Generate an unbounded iterator of grid-cells for singleton case.
     */
    protected abstract NumericDocValues unboundedCellSingleValue(GeoPointValues values);

    /**
     * Generate a bounded iterator of grid-cells for singleton case.
     */
    protected abstract NumericDocValues boundedCellSingleValue(GeoPointValues values, GeoBoundingBox boundingBox);

    /**
     * Generate an unbounded iterator of grid-cells for multi-value case.
     */
    protected abstract SortedNumericDocValues unboundedCellMultiValues(MultiGeoPointValues values);

    /**
     * Generate a bounded iterator of grid-cells for multi-value case.
     */
    protected abstract SortedNumericDocValues boundedCellMultiValues(MultiGeoPointValues values, GeoBoundingBox boundingBox);

    @Override
    public final SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /**
     * checks if the point is inside the bounding box. If the method return true, the point should be added to the final
     * result, otherwise implementors might need to check if the point grid intersects the bounding box.
     *
     * This method maybe faster than having to compute the bounding box for each point grid.
     * */
    protected boolean pointInBounds(double lon, double lat) {
        if (geoBoundingBox.top() > lat && geoBoundingBox.bottom() < lat) {
            if (crossesDateline) {
                return geoBoundingBox.left() < lon || geoBoundingBox.right() > lon;
            } else {
                return geoBoundingBox.left() < lon && geoBoundingBox.right() > lon;
            }
        }
        return false;
    }

    /**
     * Class representing the long-encoded grid-cells belonging to
     * the multi-value geo-doc-values. Class must encode the values and then
     * sort them in order to account for the cells correctly.
     */
    protected abstract static class CellMultiValues extends AbstractSortingNumericDocValues {
        private final MultiGeoPointValues geoValues;
        protected final int precision;

        protected CellMultiValues(MultiGeoPointValues geoValues, int precision, LongConsumer circuitBreakerConsumer) {
            super(circuitBreakerConsumer);
            this.geoValues = geoValues;
            this.precision = precision;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                int docValueCount = geoValues.docValueCount();
                resize(docValueCount);
                int j = 0;
                for (int i = 0; i < docValueCount; i++) {
                    j = advanceValue(geoValues.nextValue(), j);
                }
                resize(j);
                sort();
                return true;
            } else {
                return false;
            }
        }

        /**
         * Sets the appropriate long-encoded value for <code>target</code>
         * in <code>values</code>.
         *
         * @param target    the geo-value to encode
         * @param valuesIdx the index into <code>values</code> to set
         * @return          valuesIdx + 1 if value was set, valuesIdx otherwise.
         */
        protected abstract int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx);
    }

    /**
     * Class representing the long-encoded grid-cells belonging to
     * the singleton geo-doc-values.
     */
    protected abstract static class CellSingleValue extends AbstractNumericDocValues {
        private final GeoPointValues geoValues;
        protected final int precision;
        protected long value;

        protected CellSingleValue(GeoPointValues geoValues, int precision) {
            this.geoValues = geoValues;
            this.precision = precision;

        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            return geoValues.advanceExact(docId) && advance(geoValues.pointValue());
        }

        @Override
        public long longValue() throws IOException {
            return value;
        }

        /**
         * Sets the appropriate long-encoded value for <code>target</code>
         * in <code>value</code>.
         *
         * @param target    the geo-value to encode
         * @return          true if the value needs to be added, otherwise false.
         */
        protected abstract boolean advance(org.elasticsearch.common.geo.GeoPoint target);

        @Override
        public int docID() {
            return -1;
        }
    }
}
