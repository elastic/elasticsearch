/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.field.data.doubles;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;

import java.io.IOException;

/**
 *
 */
public class DoubleFieldDataType implements FieldDataType<DoubleFieldData> {

    @Override
    public ExtendedFieldComparatorSource newFieldComparatorSource(final FieldDataCache cache, final String missing) {
        if (missing == null) {
            return new ExtendedFieldComparatorSource() {
                @Override
                public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new DoubleFieldDataComparator(numHits, fieldname, cache);
                }

                @Override
                public int reducedType() {
                    return SortField.DOUBLE;
                }
            };
        }
        if (missing.equals("_last")) {
            return new ExtendedFieldComparatorSource() {
                @Override
                public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new DoubleFieldDataMissingComparator(numHits, fieldname, cache, reversed ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
                }

                @Override
                public int reducedType() {
                    return SortField.DOUBLE;
                }
            };
        }
        if (missing.equals("_first")) {
            return new ExtendedFieldComparatorSource() {
                @Override
                public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new DoubleFieldDataMissingComparator(numHits, fieldname, cache, reversed ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
                }

                @Override
                public int reducedType() {
                    return SortField.DOUBLE;
                }
            };
        }
        return new ExtendedFieldComparatorSource() {
            @Override
            public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new DoubleFieldDataMissingComparator(numHits, fieldname, cache, Double.parseDouble(missing));
            }

            @Override
            public int reducedType() {
                return SortField.DOUBLE;
            }
        };
    }

    @Override
    public DoubleFieldData load(IndexReader reader, String fieldName) throws IOException {
        return DoubleFieldData.load(reader, fieldName);
    }
}
