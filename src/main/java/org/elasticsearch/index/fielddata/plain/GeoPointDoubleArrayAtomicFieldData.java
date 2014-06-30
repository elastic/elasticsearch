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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class GeoPointDoubleArrayAtomicFieldData extends AtomicGeoPointFieldData<ScriptDocValues> {

    protected long size = -1;

    @Override
    public void close() {
    }

    @Override
    public ScriptDocValues getScriptValues() {
        return new ScriptDocValues.GeoPoints(getGeoPointValues());
    }

    static class WithOrdinals extends GeoPointDoubleArrayAtomicFieldData {

        private final DoubleArray lon, lat;
        private final Ordinals ordinals;

        public WithOrdinals(DoubleArray lon, DoubleArray lat, Ordinals ordinals) {
            super();
            this.lon = lon;
            this.lat = lat;
            this.ordinals = ordinals;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + lon.ramBytesUsed() + lat.ramBytesUsed();
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesWithOrdinals(lon, lat, ordinals.ordinals());
        }

        public static class GeoPointValuesWithOrdinals extends GeoPointValues {

            private final DoubleArray lon, lat;
            private final BytesValues.WithOrdinals ordinals;

            private final GeoPoint scratch = new GeoPoint();

            GeoPointValuesWithOrdinals(DoubleArray lon, DoubleArray lat, BytesValues.WithOrdinals ordinals) {
                super(ordinals.isMultiValued());
                this.lon = lon;
                this.lat = lat;
                this.ordinals = ordinals;
            }

            @Override
            public GeoPoint nextValue() {
                final long ord = ordinals.nextOrd();
                return scratch.reset(lat.get(ord), lon.get(ord));
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return ordinals.setDocument(docId);
            }
        }
    }

    /**
     * Assumes unset values are marked in bitset, and docId is used as the index to the value array.
     */
    public static class SingleFixedSet extends GeoPointDoubleArrayAtomicFieldData {

        private final DoubleArray lon, lat;
        private final FixedBitSet set;

        public SingleFixedSet(DoubleArray lon, DoubleArray lat, FixedBitSet set) {
            super();
            this.lon = lon;
            this.lat = lat;
            this.set = set;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + lon.ramBytesUsed() + lat.ramBytesUsed() + RamUsageEstimator.sizeOf(set.getBits());
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingleFixedSet(lon, lat, set);
        }


        static class GeoPointValuesSingleFixedSet extends GeoPointValues {

            private final DoubleArray lon;
            private final DoubleArray lat;
            private final FixedBitSet set;
            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingleFixedSet(DoubleArray lon, DoubleArray lat, FixedBitSet set) {
                super(false);
                this.lon = lon;
                this.lat = lat;
                this.set = set;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return set.get(docId) ? 1 : 0;
            }

            @Override
            public GeoPoint nextValue() {
                return scratch.reset(lat.get(docId), lon.get(docId));
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends GeoPointDoubleArrayAtomicFieldData {

        private final DoubleArray lon, lat;

        public Single(DoubleArray lon, DoubleArray lat) {
            super();
            this.lon = lon;
            this.lat = lat;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + (lon.ramBytesUsed() + lat.ramBytesUsed());
            }
            return size;
        }


        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingle(lon, lat);
        }

        static class GeoPointValuesSingle extends GeoPointValues {

            private final DoubleArray lon;
            private final DoubleArray lat;

            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingle(DoubleArray lon, DoubleArray lat) {
                super(false);
                this.lon = lon;
                this.lat = lat;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return 1;
            }

            @Override
            public GeoPoint nextValue() {
                return scratch.reset(lat.get(docId), lon.get(docId));
            }
        }
    }
}