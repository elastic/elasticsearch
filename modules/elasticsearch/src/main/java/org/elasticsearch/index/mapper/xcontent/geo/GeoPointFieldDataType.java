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

package org.elasticsearch.index.mapper.xcontent.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringOrdValFieldDataComparator;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoPointFieldDataType implements FieldDataType<GeoPointFieldData> {

    public static final GeoPointFieldDataType TYPE = new GeoPointFieldDataType();

    @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache, final String missing) {
        return new FieldComparatorSource() {
            @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new StringOrdValFieldDataComparator(numHits, fieldname, sortPos, reversed, cache);
            }
        };
    }

    @Override public GeoPointFieldData load(IndexReader reader, String fieldName) throws IOException {
        return GeoPointFieldData.load(reader, fieldName);
    }
}
