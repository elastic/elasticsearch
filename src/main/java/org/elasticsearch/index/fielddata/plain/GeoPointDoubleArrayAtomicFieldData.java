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
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.util.GeoPointArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;
import org.elasticsearch.index.mapper.geo.GeoPoint;
import org.elasticsearch.index.search.geo.GeoHashUtils;

/**
 */
public abstract class GeoPointDoubleArrayAtomicFieldData implements AtomicGeoPointFieldData {

    public static final GeoPointDoubleArrayAtomicFieldData EMPTY = new Empty();

    protected final double[] lon;
    protected final double[] lat;
    private final int numDocs;

    protected long size = -1;

    public GeoPointDoubleArrayAtomicFieldData(double[] lon, double[] lat, int numDocs) {
        this.lon = lon;
        this.lat = lat;
        this.numDocs = numDocs;
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

        Empty() {
            super(null, null, 0);
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
        public long getMemorySizeInBytes() {
            return 0;
        }

        @Override
        public BytesValues getBytesValues() {
            return BytesValues.EMPTY;
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return HashedBytesValues.EMPTY;
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return GeoPointValues.EMPTY;
        }

        @Override
        public StringValues getStringValues() {
            return StringValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    public static class WithOrdinals extends GeoPointDoubleArrayAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(double[] lon, double[] lat, int numDocs, Ordinals ordinals) {
            super(lon, lat, numDocs);
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
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lon.length * RamUsage.NUM_BYTES_DOUBLE)) + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lat.length * RamUsage.NUM_BYTES_DOUBLE)) + ordinals.getMemorySizeInBytes();
            }
            return size;
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(lon, lat, ordinals.ordinals());
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValues(lon, lat, ordinals.ordinals());
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final double[] lon;
            private final double[] lat;
            private final Ordinals.Docs ordinals;

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
            private final ValuesIter valuesIter;

            StringValues(double[] lon, double[] lat, Ordinals.Docs ordinals) {
                this.lon = lon;
                this.lat = lat;
                this.ordinals = ordinals;
                this.valuesIter = new ValuesIter(lon, lat);
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public String getValue(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return null;
                }
                return GeoHashUtils.encode(lat[ord], lon[ord]);
            }

            @Override
            public StringArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return StringArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    int ord = ords.values[i];
                    arrayScratch.values[arrayScratch.end++] = GeoHashUtils.encode(lat[ord], lon[ord]);
                }
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return valuesIter.reset(ordinals.getIter(docId));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                Ordinals.Docs.Iter iter = ordinals.getIter(docId);
                int ord = iter.next();
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                do {
                    proc.onValue(docId, GeoHashUtils.encode(lat[ord], lon[ord]));
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final double[] lon;
                private final double[] lat;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(double[] lon, double[] lat) {
                    this.lon = lon;
                    this.lat = lat;
                }

                public ValuesIter reset(Ordinals.Docs.Iter ordsIter) {
                    this.ordsIter = ordsIter;
                    this.ord = ordsIter.next();
                    return this;
                }

                @Override
                public boolean hasNext() {
                    return ord != 0;
                }

                @Override
                public String next() {
                    String value = GeoHashUtils.encode(lat[ord], lon[ord]);
                    ord = ordsIter.next();
                    return value;
                }
            }
        }

        static class GeoPointValues implements org.elasticsearch.index.fielddata.GeoPointValues {

            private final double[] lon;
            private final double[] lat;
            private final Ordinals.Docs ordinals;

            private final GeoPoint scratch = new GeoPoint();
            private final GeoPointArrayRef arrayScratch = new GeoPointArrayRef(new GeoPoint[1], 1);
            private final ValuesIter valuesIter;
            private final SafeValuesIter safeValuesIter;

            GeoPointValues(double[] lon, double[] lat, Ordinals.Docs ordinals) {
                this.lon = lon;
                this.lat = lat;
                this.ordinals = ordinals;
                this.valuesIter = new ValuesIter(lon, lat);
                this.safeValuesIter = new SafeValuesIter(lon, lat);
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public GeoPoint getValue(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return null;
                }
                return scratch.reset(lat[ord], lon[ord]);
            }

            @Override
            public GeoPoint getValueSafe(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return null;
                }
                return new GeoPoint(lat[ord], lon[ord]);
            }

            @Override
            public GeoPointArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return GeoPointArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    int ord = ords.values[i];
                    arrayScratch.values[arrayScratch.end++].reset(lat[ord], lon[ord]);
                }
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return valuesIter.reset(ordinals.getIter(docId));
            }

            @Override
            public Iter getIterSafe(int docId) {
                return safeValuesIter.reset(ordinals.getIter(docId));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                Ordinals.Docs.Iter iter = ordinals.getIter(docId);
                int ord = iter.next();
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                do {
                    proc.onValue(docId, scratch.reset(lat[ord], lon[ord]));
                } while ((ord = iter.next()) != 0);
            }

            @Override
            public void forEachSafeValueInDoc(int docId, ValueInDocProc proc) {
                Ordinals.Docs.Iter iter = ordinals.getIter(docId);
                int ord = iter.next();
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                do {
                    proc.onValue(docId, new GeoPoint(lat[ord], lon[ord]));
                } while ((ord = iter.next()) != 0);
            }

            @Override
            public void forEachLatLonValueInDoc(int docId, LatLonValueInDocProc proc) {
                Ordinals.Docs.Iter iter = ordinals.getIter(docId);
                int ord = iter.next();
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                do {
                    proc.onValue(docId, lat[ord], lon[ord]);
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final double[] lon;
                private final double[] lat;
                private final GeoPoint scratch = new GeoPoint();

                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(double[] lon, double[] lat) {
                    this.lon = lon;
                    this.lat = lat;
                }

                public ValuesIter reset(Ordinals.Docs.Iter ordsIter) {
                    this.ordsIter = ordsIter;
                    this.ord = ordsIter.next();
                    return this;
                }

                @Override
                public boolean hasNext() {
                    return ord != 0;
                }

                @Override
                public GeoPoint next() {
                    scratch.reset(lat[ord], lon[ord]);
                    ord = ordsIter.next();
                    return scratch;
                }
            }

            static class SafeValuesIter implements Iter {

                private final double[] lon;
                private final double[] lat;

                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                SafeValuesIter(double[] lon, double[] lat) {
                    this.lon = lon;
                    this.lat = lat;
                }

                public SafeValuesIter reset(Ordinals.Docs.Iter ordsIter) {
                    this.ordsIter = ordsIter;
                    this.ord = ordsIter.next();
                    return this;
                }

                @Override
                public boolean hasNext() {
                    return ord != 0;
                }

                @Override
                public GeoPoint next() {
                    GeoPoint value = new GeoPoint(lat[ord], lon[ord]);
                    ord = ordsIter.next();
                    return value;
                }
            }
        }
    }

    /**
     * Assumes unset values are marked in bitset, and docId is used as the index to the value array.
     */
    public static class SingleFixedSet extends GeoPointDoubleArrayAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(double[] lon, double[] lat, int numDocs, FixedBitSet set) {
            super(lon, lat, numDocs);
            this.set = set;
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
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lon.length * RamUsage.NUM_BYTES_DOUBLE)) + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lat.length * RamUsage.NUM_BYTES_DOUBLE)) + (set.getBits().length * RamUsage.NUM_BYTES_LONG);
            }
            return size;
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(lon, lat, set);
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValues(lon, lat, set);
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final double[] lon;
            private final double[] lat;
            private final FixedBitSet set;

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
            private final Iter.Single iter = new Iter.Single();

            StringValues(double[] lon, double[] lat, FixedBitSet set) {
                this.lon = lon;
                this.lat = lat;
                this.set = set;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return set.get(docId);
            }

            @Override
            public String getValue(int docId) {
                if (set.get(docId)) {
                    return GeoHashUtils.encode(lat[docId], lon[docId]);
                } else {
                    return null;
                }
            }

            @Override
            public StringArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0] = GeoHashUtils.encode(lat[docId], lon[docId]);
                    return arrayScratch;
                } else {
                    return StringArrayRef.EMPTY;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset(GeoHashUtils.encode(lat[docId], lon[docId]));
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, GeoHashUtils.encode(lat[docId], lon[docId]));
                } else {
                    proc.onMissing(docId);
                }
            }
        }

        static class GeoPointValues implements org.elasticsearch.index.fielddata.GeoPointValues {

            private final double[] lon;
            private final double[] lat;
            private final FixedBitSet set;

            private final GeoPoint scratch = new GeoPoint();
            private final GeoPointArrayRef arrayScratch = new GeoPointArrayRef(new GeoPoint[1]);
            private final Iter.Single iter = new Iter.Single();

            GeoPointValues(double[] lon, double[] lat, FixedBitSet set) {
                this.lon = lon;
                this.lat = lat;
                this.set = set;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return set.get(docId);
            }

            @Override
            public GeoPoint getValue(int docId) {
                if (set.get(docId)) {
                    return scratch.reset(lat[docId], lon[docId]);
                } else {
                    return null;
                }
            }

            @Override
            public GeoPoint getValueSafe(int docId) {
                if (set.get(docId)) {
                    return new GeoPoint(lat[docId], lon[docId]);
                } else {
                    return null;
                }
            }

            @Override
            public GeoPointArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0].reset(lat[docId], lon[docId]);
                    return arrayScratch;
                } else {
                    return GeoPointArrayRef.EMPTY;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset(scratch.reset(lat[docId], lon[docId]));
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public Iter getIterSafe(int docId) {
                if (set.get(docId)) {
                    return iter.reset(new GeoPoint(lat[docId], lon[docId]));
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, scratch.reset(lat[docId], lon[docId]));
                } else {
                    proc.onMissing(docId);
                }
            }

            @Override
            public void forEachSafeValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, new GeoPoint(lat[docId], lon[docId]));
                } else {
                    proc.onMissing(docId);
                }
            }

            @Override
            public void forEachLatLonValueInDoc(int docId, LatLonValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, lat[docId], lon[docId]);
                } else {
                    proc.onMissing(docId);
                }
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends GeoPointDoubleArrayAtomicFieldData {

        public Single(double[] lon, double[] lat, int numDocs) {
            super(lon, lat, numDocs);
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
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lon.length * RamUsage.NUM_BYTES_DOUBLE)) + (RamUsage.NUM_BYTES_ARRAY_HEADER + (lat.length * RamUsage.NUM_BYTES_DOUBLE));
            }
            return size;
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(lon, lat);
        }

        @Override
        public GeoPointValues getGeoPointValues() {
            return new GeoPointValues(lon, lat);
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final double[] lon;
            private final double[] lat;

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
            private final Iter.Single iter = new Iter.Single();

            StringValues(double[] lon, double[] lat) {
                this.lon = lon;
                this.lat = lat;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return true;
            }

            @Override
            public String getValue(int docId) {
                return GeoHashUtils.encode(lat[docId], lon[docId]);
            }

            @Override
            public StringArrayRef getValues(int docId) {
                arrayScratch.values[0] = GeoHashUtils.encode(lat[docId], lon[docId]);
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(GeoHashUtils.encode(lat[docId], lon[docId]));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, GeoHashUtils.encode(lat[docId], lon[docId]));
            }
        }

        static class GeoPointValues implements org.elasticsearch.index.fielddata.GeoPointValues {

            private final double[] lon;
            private final double[] lat;

            private final GeoPoint scratch = new GeoPoint();
            private final GeoPointArrayRef arrayScratch = new GeoPointArrayRef(new GeoPoint[1]);
            private final Iter.Single iter = new Iter.Single();

            GeoPointValues(double[] lon, double[] lat) {
                this.lon = lon;
                this.lat = lat;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return true;
            }

            @Override
            public GeoPoint getValue(int docId) {
                return scratch.reset(lat[docId], lon[docId]);
            }

            @Override
            public GeoPoint getValueSafe(int docId) {
                return new GeoPoint(lat[docId], lon[docId]);
            }

            @Override
            public GeoPointArrayRef getValues(int docId) {
                arrayScratch.values[0].reset(lat[docId], lon[docId]);
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(scratch.reset(lat[docId], lon[docId]));
            }

            @Override
            public Iter getIterSafe(int docId) {
                return iter.reset(new GeoPoint(lat[docId], lon[docId]));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, scratch.reset(lat[docId], lon[docId]));
            }

            @Override
            public void forEachSafeValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, new GeoPoint(lat[docId], lon[docId]));
            }

            @Override
            public void forEachLatLonValueInDoc(int docId, LatLonValueInDocProc proc) {
                proc.onValue(docId, lat[docId], lon[docId]);
            }
        }
    }
}