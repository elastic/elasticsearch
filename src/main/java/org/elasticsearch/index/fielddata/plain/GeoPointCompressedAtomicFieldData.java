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
import org.apache.lucene.util.packed.PagedMutable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

/**
 * Field data atomic impl for geo points with lossy compression.
 */
public abstract class GeoPointCompressedAtomicFieldData extends AtomicGeoPointFieldData<ScriptDocValues> {

    protected long size = -1;

    @Override
    public void close() {
    }

    @Override
    public ScriptDocValues getScriptValues() {
        return new ScriptDocValues.GeoPoints(getGeoPointValues());
    }

    static class WithOrdinals extends GeoPointCompressedAtomicFieldData {

        private final GeoPointFieldMapper.Encoding encoding;
        private final PagedMutable lon, lat;
        private final Ordinals ordinals;

        public WithOrdinals(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat, Ordinals ordinals) {
            super();
            this.encoding = encoding;
            this.lon = lon;
            this.lat = lat;
            this.ordinals = ordinals;
        }

        @Override
        public boolean isMultiValued() {
            return ordinals.isMultiValued();
        }

        @Override
        public long getNumberUniqueValues() {
            return ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + lon.ramBytesUsed() + lat.ramBytesUsed();
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesWithOrdinals(encoding, lon, lat, ordinals.ordinals());
        }

        public static class GeoPointValuesWithOrdinals extends GeoPointValues {

            private final GeoPointFieldMapper.Encoding encoding;
            private final PagedMutable lon, lat;
            private final Ordinals.Docs ordinals;

            private final GeoPoint scratch = new GeoPoint();

            GeoPointValuesWithOrdinals(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat, Ordinals.Docs ordinals) {
                super(ordinals.isMultiValued());
                this.encoding = encoding;
                this.lon = lon;
                this.lat = lat;
                this.ordinals = ordinals;
            }

            @Override
            public GeoPoint nextValue() {
                final long ord = ordinals.nextOrd();
                return encoding.decode(lat.get(ord), lon.get(ord), scratch);
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
    public static class SingleFixedSet extends GeoPointCompressedAtomicFieldData {

        private final GeoPointFieldMapper.Encoding encoding;
        private final PagedMutable lon, lat;
        private final FixedBitSet set;
        private final long numOrds;

        public SingleFixedSet(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat, FixedBitSet set, long numOrds) {
            super();
            this.encoding = encoding;
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
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + lon.ramBytesUsed() + lat.ramBytesUsed() + RamUsageEstimator.sizeOf(set.getBits());
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingleFixedSet(encoding, lon, lat, set);
        }


        static class GeoPointValuesSingleFixedSet extends GeoPointValues {

            private final GeoPointFieldMapper.Encoding encoding;
            private final PagedMutable lat, lon;
            private final FixedBitSet set;
            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingleFixedSet(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat, FixedBitSet set) {
                super(false);
                this.encoding = encoding;
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
                return encoding.decode(lat.get(docId), lon.get(docId), scratch);
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends GeoPointCompressedAtomicFieldData {

        private final GeoPointFieldMapper.Encoding encoding;
        private final PagedMutable lon, lat;
        private final long numOrds;

        public Single(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat, long numOrds) {
            super();
            this.encoding = encoding;
            this.lon = lon;
            this.lat = lat;
            this.numOrds = numOrds;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + (lon.ramBytesUsed() + lat.ramBytesUsed());
            }
            return size;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValuesSingle(encoding, lon, lat);
        }

        static class GeoPointValuesSingle extends GeoPointValues {

            private final GeoPointFieldMapper.Encoding encoding;
            private final PagedMutable lon, lat;

            private final GeoPoint scratch = new GeoPoint();


            GeoPointValuesSingle(GeoPointFieldMapper.Encoding encoding, PagedMutable lon, PagedMutable lat) {
                super(false);
                this.encoding = encoding;
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
                return encoding.decode(lat.get(docId), lon.get(docId), scratch);
            }
        }
    }
}