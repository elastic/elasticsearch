/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Class to encapsulate a set of ValuesSource objects labeled by field name
 */
public abstract class ArrayValuesSource<VS extends ValuesSource> {
    protected MultiValueMode multiValueMode;
    protected String[] names;
    protected VS[] values;

    public static class NumericArrayValuesSource extends ArrayValuesSource<ValuesSource.Numeric> {
        public NumericArrayValuesSource(Map<String, ValuesSource.Numeric> valuesSources, MultiValueMode multiValueMode) {
            super(valuesSources, multiValueMode);
            if (valuesSources != null) {
                this.values = valuesSources.values().toArray(new ValuesSource.Numeric[0]);
            } else {
                this.values = new ValuesSource.Numeric[0];
            }
        }

        public NumericDoubleValues getField(final int ordinal, LeafReaderContext ctx) throws IOException {
            if (ordinal > names.length) {
                throw new IndexOutOfBoundsException("ValuesSource array index " + ordinal + " out of bounds");
            }
            return multiValueMode.select(values[ordinal].doubleValues(ctx));
        }
    }

    public static class BytesArrayValuesSource extends ArrayValuesSource<ValuesSource.Bytes> {
        public BytesArrayValuesSource(Map<String, ValuesSource.Bytes> valuesSources, MultiValueMode multiValueMode) {
            super(valuesSources, multiValueMode);
            this.values = valuesSources.values().toArray(new ValuesSource.Bytes[0]);
        }

        public Object getField(final int ordinal, LeafReaderContext ctx) throws IOException {
            return values[ordinal].bytesValues(ctx);
        }
    }

    public static class GeoPointValuesSource extends ArrayValuesSource<ValuesSource.GeoPoint> {
        public GeoPointValuesSource(Map<String, ValuesSource.GeoPoint> valuesSources, MultiValueMode multiValueMode) {
            super(valuesSources, multiValueMode);
            this.values = valuesSources.values().toArray(new ValuesSource.GeoPoint[0]);
        }
    }

    private ArrayValuesSource(Map<String, ?> valuesSources, MultiValueMode multiValueMode) {
        if (valuesSources != null) {
            this.names = valuesSources.keySet().toArray(new String[0]);
        }
        this.multiValueMode = multiValueMode;
    }

    public boolean needsScores() {
        boolean needsScores = false;
        for (ValuesSource value : values) {
            needsScores |= value.needsScores();
        }
        return needsScores;
    }

    public String[] fieldNames() {
        return this.names;
    }
}
