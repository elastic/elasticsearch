/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

/**
 * A {@link SingleDimensionValuesSource} for geotile values.
 *
 * Since geotile values can be represented as long values, this class is almost the same as {@link LongValuesSource}
 * The main differences is {@link GeoTileValuesSource#setAfter(Comparable)} as it needs to accept geotile string values i.e. "zoom/x/y".
 */
class GeoTileValuesSource extends LongValuesSource {
    GeoTileValuesSource(BigArrays bigArrays,
                        MappedFieldType fieldType,
                        CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc,
                        LongUnaryOperator rounding,
                        DocValueFormat format,
                        boolean missingBucket,
                        int size,
                        int reverseMul) {
        super(bigArrays, fieldType, docValuesFunc, rounding, format, missingBucket, size, reverseMul);
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value instanceof Number) {
            afterValue = ((Number) value).longValue();
        } else {
            afterValue = format.parseLong(
                value.toString(),
                false,
                () -> { throw new IllegalArgumentException("now() is not supported in [after] key"); }
            );
        }
    }
}
