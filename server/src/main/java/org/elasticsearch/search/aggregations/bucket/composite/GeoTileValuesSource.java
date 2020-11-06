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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

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
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value instanceof Number) {
            afterValue = ((Number) value).longValue();
        } else {
            afterValue = GeoTileUtils.longEncode(value.toString());
        }
    }
}
