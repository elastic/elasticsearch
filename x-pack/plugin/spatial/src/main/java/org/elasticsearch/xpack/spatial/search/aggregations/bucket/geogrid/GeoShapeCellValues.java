/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;

import java.io.IOException;
import java.util.function.Consumer;

/** Sorted numeric doc values for geo shapes */
class GeoShapeCellValues extends ByteTrackingSortingNumericDocValues {
    private final MultiGeoShapeValues geoShapeValues;
    private final Consumer<Long> circuitBreakerConsumer;
    protected int precision;
    protected GeoGridTiler tiler;

    protected GeoShapeCellValues(MultiGeoShapeValues geoShapeValues, int precision, GeoGridTiler tiler,
                                 Consumer<Long> circuitBreakerConsumer) {
        this.geoShapeValues = geoShapeValues;
        this.precision = precision;
        this.tiler = tiler;
        this.circuitBreakerConsumer = circuitBreakerConsumer;
        circuitBreakerConsumer.accept((long) Long.BYTES);
    }

    @Override
    public boolean advanceExact(int docId) throws IOException {
        if (geoShapeValues.advanceExact(docId)) {
            assert geoShapeValues.docValueCount() == 1;
            int j = advanceValue(geoShapeValues.nextValue());
            resize(j);
            sort();
            return true;
        } else {
            return false;
        }
    }

    // for testing
    protected long[] getValues() {
        return values;
    }

    protected void add(int idx, long value) {
        values[idx] = value;
    }

    void resizeCell(int newSize) {
        int oldValuesLength = values.length;
        resize(newSize);
        int newValuesLength = values.length;
        if (newValuesLength > oldValuesLength) {
            long bytesDiff = (newValuesLength - oldValuesLength) * Long.BYTES;
            circuitBreakerConsumer.accept(bytesDiff);
        }
    }

    /**
     * Sets the appropriate long-encoded value for <code>target</code>
     * in <code>values</code>.
     *
     * @param target    the geo-shape to encode
     * @return          number of buckets for given shape tiling of <code>target</code>.
     */
    int advanceValue(MultiGeoShapeValues.GeoShapeValue target) {
        return tiler.setValues(this, target, precision);
    }
}

