/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.util.BigDoubleArrayList;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class GeoPointDoubleArrayAtomicFieldData extends AtomicGeoPointFieldData<ScriptDocValues> {

    public static GeoPointDoubleArrayAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    private final int numDocs;

    protected long size = -1;

    public GeoPointDoubleArrayAtomicFieldData(int numDocs) {
        this.numDocs = numDocs;
    }

    @Override
    public void close() {
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public ScriptDocValues getScriptValues() {
        return new ScriptDocValues.GeoPoints(getGeoPointValues());
    }

    static class Empty extends GeoPointDoubleArrayAtomicFieldData {

        Empty(int numDocs) {
            super(numDocs);
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return 0;
        }

        @Override
        public long getMemorySizeInBytes() {
            return 0;
        }

        @Override
        public BytesValues getBytesValues(boolean needsHashes) {
            return BytesValues.EMPTY;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return GeoPointValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    static class WithOrdinals extends GeoPointDoubleArrayAtomicFieldData {

        private final BigDoubleArrayList lon, lat;
        private final Ordinals ordinals;

        public WithOrdinals(BigDoubleArrayList lon, BigDoubleArrayList lat, int numDocs, Ordinals ordinals) {
            super(numDocs);
            this.lon = lon;
            this.lat = lat;
            this.ordinals = ordinals;
        }

        @Override
        public boolean isMultiValued() {
            return ordinals.isMultiValued();
        }

        @Override
        public boolean isValuesOrdered() {
            return true;
        }

        @Override
        public long getNumberUniqueValues() {
            return ordinals.getNumOrds();
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + lon.sizeInBytes() + lat.sizeInBytes();
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesWithOrdinals(lon, lat, ordinals.ordinals());
        }

        public static class GeoPointValuesWithOrdinals extends GeoPointValues {

            private final BigDoubleArrayList lon, lat;
            private final Ordinals.Docs ordinals;

            private final GeoPoint scratch = new GeoPoint();

            GeoPointValuesWithOrdinals(BigDoubleArrayList lon, BigDoubleArrayList lat, Ordinals.Docs ordinals) {
                super(ordinals.isMultiValued());
                this.lon = lon;
                this.lat = lat;
                this.ordinals = ordinals;
            }

            @Override
            public GeoPoint nextValue() {
                final long ord = ordinals.nextOrd();
                assert ord > 0;
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

        private final BigDoubleArrayList lon, lat;
        private final FixedBitSet set;
        private final long numOrds;

        public SingleFixedSet(BigDoubleArrayList lon, BigDoubleArrayList lat, int numDocs, FixedBitSet set, long numOrds) {
            super(numDocs);
            this.lon = lon;
            this.lat = lat;
            this.set = set;
            this.numOrds = numOrds;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + lon.sizeInBytes() + lat.sizeInBytes() + RamUsageEstimator.sizeOf(set.getBits());
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingleFixedSet(lon, lat, set);
        }


        static class GeoPointValuesSingleFixedSet extends GeoPointValues {

            private final BigDoubleArrayList lon;
            private final BigDoubleArrayList lat;
            private final FixedBitSet set;
            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingleFixedSet(BigDoubleArrayList lon, BigDoubleArrayList lat, FixedBitSet set) {
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

        private final BigDoubleArrayList lon, lat;
        private final long numOrds;

        public Single(BigDoubleArrayList lon, BigDoubleArrayList lat, int numDocs, long numOrds) {
            super(numDocs);
            this.lon = lon;
            this.lat = lat;
            this.numOrds = numOrds;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + (lon.sizeInBytes() + lat.sizeInBytes());
            }
            return size;
        }


        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingle(lon, lat);
        }

        static class GeoPointValuesSingle extends GeoPointValues {

            private final BigDoubleArrayList lon;
            private final BigDoubleArrayList lat;

            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingle(BigDoubleArrayList lon, BigDoubleArrayList lat) {
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