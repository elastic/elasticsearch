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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class LatLonPointDVAtomicFieldData extends AbstractAtomicGeoPointFieldData {
    private final LeafReader reader;
    private final String fieldName;

    LatLonPointDVAtomicFieldData(LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public MultiGeoValues getGeoValues() {
        try {
            final SortedNumericDocValues numericValues = DocValues.getSortedNumeric(reader, fieldName);
            return new MultiGeoValues() {

                final GeoPointValue point = new GeoPointValue(new GeoPoint());

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return numericValues.advanceExact(doc);
                }

                @Override
                public int docValueCount() {
                    return numericValues.docValueCount();
                }

                @Override
                public ValuesSourceType valuesSourceType() {
                    return CoreValuesSourceType.GEOPOINT;
                }

                @Override
                public GeoValue nextValue() throws IOException {
                    final long encoded = numericValues.nextValue();
                    point.geoPoint().reset(GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)),
                            GeoEncodingUtils.decodeLongitude((int) encoded));
                    return point;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
