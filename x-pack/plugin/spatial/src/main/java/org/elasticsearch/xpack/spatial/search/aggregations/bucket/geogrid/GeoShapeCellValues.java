/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.function.LongConsumer;

/** Sorted numeric doc values for geo shapes */
final class GeoShapeCellValues extends AbstractSortingNumericDocValues {
    private final GeoShapeValues geoShapeValues;
    private final GeoGridTiler tiler;

    GeoShapeCellValues(GeoShapeValues geoShapeValues, GeoGridTiler tiler, LongConsumer circuitBreakerConsumer) {
        super(circuitBreakerConsumer);
        this.geoShapeValues = geoShapeValues;
        this.tiler = tiler;
    }

    @Override
    public boolean advanceExact(int docId) throws IOException {
        if (geoShapeValues.advanceExact(docId)) {
            final int j = tiler.setValues(this, geoShapeValues.value());
            resize(j);
            sort();
            return true;
        } else {
            return false;
        }
    }

    // for testing
    long[] getValues() {
        return values;
    }

    void resizeCell(int newSize) {
        resize(newSize);
    }

    void add(int idx, long value) {
        values[idx] = value;
    }
}
