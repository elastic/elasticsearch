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
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public abstract class GeoPointArrayAtomicFieldData extends AbstractAtomicGeoPointFieldData {
    @Override
    public void close() {
    }

    static class WithOrdinals extends GeoPointArrayAtomicFieldData {
        private final LongArray indexedPoints;
        private final Ordinals ordinals;
        private final int maxDoc;

        public WithOrdinals(LongArray indexedPoints, Ordinals ordinals, int maxDoc) {
            super();
            this.indexedPoints = indexedPoints;
            this.ordinals = ordinals;
            this.maxDoc = maxDoc;
        }

        @Override
        public long ramBytesUsed() {
            return Integer.BYTES + indexedPoints.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.add(Accountables.namedAccountable("indexedPoints", indexedPoints));
            return Collections.unmodifiableList(resources);
        }

        @Override
        public MultiGeoPointValues getGeoPointValues() {
            final RandomAccessOrds ords = ordinals.ordinals();
            final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
            final GeoPoint point = new GeoPoint(Double.NaN, Double.NaN);
            if (singleOrds != null) {
                final GeoPointValues values = new GeoPointValues() {
                    @Override
                    public GeoPoint get(int docID) {
                        final int ord = singleOrds.getOrd(docID);
                        if (ord >= 0) {
                            return point.resetFromIndexHash(indexedPoints.get(ord));
                        }
                        return point.reset(Double.NaN, Double.NaN);
                    }
                };
                return FieldData.singleton(values, DocValues.docsWithValue(singleOrds, maxDoc));
            }
            return new MultiGeoPointValues() {
                @Override
                public GeoPoint valueAt(int index) {
                    return point.resetFromIndexHash(indexedPoints.get(ords.ordAt(index)));
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

    public static class Single extends GeoPointArrayAtomicFieldData {
        private final LongArray indexedPoint;
        private final BitSet set;

        public Single(LongArray indexedPoint, BitSet set) {
            this.indexedPoint = indexedPoint;
            this.set = set;
        }

        @Override
        public long ramBytesUsed() {
            return Integer.BYTES + indexedPoint.ramBytesUsed()
                    + (set == null ? 0 : set.ramBytesUsed());
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.add(Accountables.namedAccountable("indexedPoints", indexedPoint));
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
                        return point.resetFromIndexHash(indexedPoint.get(docID));
                    }
                    return point.reset(Double.NaN, Double.NaN);
                }
            };
            return FieldData.singleton(values, set);
        }
    }
}
