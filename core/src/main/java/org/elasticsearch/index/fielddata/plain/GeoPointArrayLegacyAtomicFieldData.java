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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 */
public abstract class GeoPointArrayLegacyAtomicFieldData extends AbstractAtomicGeoPointFieldData {

    @Override
    public void close() {
    }

    static class WithOrdinals extends GeoPointArrayLegacyAtomicFieldData {

        private final DoubleArray lon, lat;
        private final Ordinals ordinals;
        private final int maxDoc;

        public WithOrdinals(DoubleArray lon, DoubleArray lat, Ordinals ordinals, int maxDoc) {
            super();
            this.lon = lon;
            this.lat = lat;
            this.ordinals = ordinals;
            this.maxDoc = maxDoc;
        }

        @Override
        public long ramBytesUsed() {
            return Integer.BYTES/*size*/ + lon.ramBytesUsed() + lat.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.add(Accountables.namedAccountable("latitude", lat));
            resources.add(Accountables.namedAccountable("longitude", lon));
            return Collections.unmodifiableList(resources);
        }

        @Override
        public MultiGeoPointValues getGeoPointValues() {
            final RandomAccessOrds ords = ordinals.ordinals();
            final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
            if (singleOrds != null) {
                final GeoPoint point = new GeoPoint();
                final GeoPointValues values = new GeoPointValues() {
                    @Override
                    public GeoPoint get(int docID) {
                        final int ord = singleOrds.getOrd(docID);
                        if (ord >= 0) {
                            return point.reset(lat.get(ord), lon.get(ord));
                        }
                        return point.reset(Double.NaN, Double.NaN);
                    }
                };
                return FieldData.singleton(values, DocValues.docsWithValue(singleOrds, maxDoc));
            } else {
                final GeoPoint point = new GeoPoint();
                return new MultiGeoPointValues() {

                    @Override
                    public GeoPoint valueAt(int index) {
                        final long ord = ords.ordAt(index);
                        if (ord >= 0) {
                            return point.reset(lat.get(ord), lon.get(ord));
                        }
                        return point.reset(Double.NaN, Double.NaN);
                    }

                    @Override
                    public void setDocument(int docId) {
                        ords.setDocument(docId);
                    }

                    @Override
                    public int count() {
                        return ords.cardinality();
                    }
                };
            }
        }
    }

    /**
     * Assumes unset values are marked in bitset, and docId is used as the index to the value array.
     */
    public static class Single extends GeoPointArrayLegacyAtomicFieldData {

        private final DoubleArray lon, lat;
        private final BitSet set;

        public Single(DoubleArray lon, DoubleArray lat, BitSet set) {
            this.lon = lon;
            this.lat = lat;
            this.set = set;
        }

        @Override
        public long ramBytesUsed() {
            return Integer.BYTES + lon.ramBytesUsed() + lat.ramBytesUsed() + (set == null ? 0 : set.ramBytesUsed());
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.add(Accountables.namedAccountable("latitude", lat));
            resources.add(Accountables.namedAccountable("longitude", lon));
            if (set != null) {
                resources.add(Accountables.namedAccountable("missing bitset", set));
            }
            return Collections.unmodifiableList(resources);
        }

        @Override
        public MultiGeoPointValues getGeoPointValues() {
            final GeoPoint point = new GeoPoint();
            final GeoPointValues values = new GeoPointValues() {
                @Override
                public GeoPoint get(int docID) {
                    if (set == null || set.get(docID)) {
                        return point.reset(lat.get(docID), lon.get(docID));
                    }
                    return point.reset(Double.NaN, Double.NaN);
                }
            };
            return FieldData.singleton(values, set);
        }
    }

}