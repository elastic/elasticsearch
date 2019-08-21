/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

/**
 * A {@link SingleDimensionValuesSource} for geohash values.
 *
 * Since geohash values can be represented as long values, this class is almost the same as {@link LongValuesSource}
 * The main differences is {@link GeohashValuesSource#setAfter(Comparable)} as it needs to accept geohash string values.
 */
class GeohashValuesSource extends LongValuesSource {
    private final int precision;
    private final CellIdSource.GeoPointLongEncoder encoder;
    GeohashValuesSource(BigArrays bigArrays,
                        MappedFieldType fieldType,
                        CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc,
                        LongUnaryOperator rounding,
                        DocValueFormat format,
                        boolean missingBucket,
                        int size,
                        int reverseMul,
                        int precision,
                        CellIdSource.GeoPointLongEncoder encoder) {
        super(bigArrays, fieldType, docValuesFunc, rounding, format, missingBucket, size, reverseMul);
        this.precision = precision;
        this.encoder = encoder;
    }

    @Override
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value instanceof Number) {
            afterValue = ((Number) value).longValue();
        } else {
            // if it is a string it should be a geohash formatted value.
            // We need to preserve the precision between the decoding the geohash and encoding it into a long
            Point point = Geohash.toPoint(value.toString());
            afterValue = encoder.encode(point.getLon(), point.getLat(), precision);
        }
    }
}
