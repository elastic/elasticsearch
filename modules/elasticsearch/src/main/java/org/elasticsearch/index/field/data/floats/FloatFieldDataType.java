/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.field.data.floats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class FloatFieldDataType implements FieldDataType<FloatFieldData> {

    @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache, final String missing) {
        if (missing == null) {
            return new FieldComparatorSource() {
                @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new FloatFieldDataComparator(numHits, fieldname, cache);
                }
            };
        }
        if (missing.equals("_last")) {
            return new FieldComparatorSource() {
                @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new FloatFieldDataMissingComparator(numHits, fieldname, cache, reversed ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
                }
            };
        }
        if (missing.equals("_first")) {
            return new FieldComparatorSource() {
                @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new FloatFieldDataMissingComparator(numHits, fieldname, cache, reversed ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY);
                }
            };
        }
        return new FieldComparatorSource() {
            @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new FloatFieldDataMissingComparator(numHits, fieldname, cache, Float.parseFloat(missing));
            }
        };
    }

    @Override public FloatFieldData load(IndexReader reader, String fieldName) throws IOException {
        return FloatFieldData.load(reader, fieldName);
    }
}
