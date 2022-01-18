/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;

/**
 * Class representing the long-encoded grid-cells belonging to
 * the geo-doc-values. Class must encode the values and then
 * sort them in order to account for the cells correctly.
 */
public abstract class CellValues extends AbstractSortingNumericDocValues {
    private MultiGeoPointValues geoValues;
    protected int precision;

    protected CellValues(MultiGeoPointValues geoValues, int precision) {
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
