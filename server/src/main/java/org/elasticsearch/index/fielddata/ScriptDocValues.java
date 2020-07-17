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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.UnaryOperator;

/**
 * Script level doc values, the assumption is that any implementation will
 * implement a {@link Longs#getValue getValue} method.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> {

    /**
     * Set the current doc ID.
     */
    public abstract void setNextDocId(int docId) throws IOException;

    // Throw meaningful exceptions if someone tries to modify the ScriptDocValues.
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    public static final class Longs extends ScriptDocValues<Long> {
        private final SortedNumericDocValues in;
        private long[] values = new long[0];
        private int count;

        /**
         * Standard constructor.
         */
        public Longs(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public long getValue() {
            return get(0);
        }

        @Override
        public Long get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static final class Dates extends ScriptDocValues<JodaCompatibleZonedDateTime> {

        private final SortedNumericDocValues in;
        private final boolean isNanos;

        /**
         * Values wrapped in {@link java.time.ZonedDateTime} objects.
         */
        private JodaCompatibleZonedDateTime[] dates;
        private int count;

        public Dates(SortedNumericDocValues in, boolean isNanos) {
            this.in = in;
            this.isNanos = isNanos;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public JodaCompatibleZonedDateTime getValue() {
            return get(0);
        }

        @Override
        public JodaCompatibleZonedDateTime get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            if (index >= count) {
                throw new IndexOutOfBoundsException(
                        "attempted to fetch the [" + index + "] date when there are only ["
                                + count + "] dates.");
            }
            return dates[index];
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                count = in.docValueCount();
            } else {
                count = 0;
            }
            refreshArray();
        }

        /**
         * Refresh the backing array. Package private so it can be called when {@link Longs} loads dates.
         */
        void refreshArray() throws IOException {
            if (count == 0) {
                return;
            }
            if (dates == null || count > dates.length) {
                // Happens for the document. We delay allocating dates so we can allocate it with a reasonable size.
                dates = new JodaCompatibleZonedDateTime[count];
            }
            for (int i = 0; i < count; ++i) {
                if (isNanos) {
                    dates[i] = new JodaCompatibleZonedDateTime(DateUtils.toInstant(in.nextValue()), ZoneOffset.UTC);
                } else {
                    dates[i] = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(in.nextValue()), ZoneOffset.UTC);
                }
            }
        }
    }

    public static final class Doubles extends ScriptDocValues<Double> {

        private final SortedNumericDoubleValues in;
        private double[] values = new double[0];
        private int count;

        public Doubles(SortedNumericDoubleValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDoubleValues getInternalValues() {
            return this.in;
        }

        public double getValue() {
            return get(0);
        }

        @Override
        public Double get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static final class GeoPoints extends ScriptDocValues<GeoPoint> {

        private final MultiGeoPointValues in;
        private GeoPoint[] values = new GeoPoint[0];
        private int count;

        public GeoPoints(MultiGeoPointValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    GeoPoint point = in.nextValue();
                    values[i] = new GeoPoint(point.lat(), point.lon());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                values[i] = new GeoPoint();
                }
            }
        }

        public GeoPoint getValue() {
            return get(0);
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            double[] lats = new double[size()];
            for (int i = 0; i < size(); i++) {
                lats[i] = get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            double[] lons = new double[size()];
            for (int i = 0; i < size(); i++) {
                lons[i] = get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            final GeoPoint point = values[index];
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return count;
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), lat, lon);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return arcDistance(lat, lon);
        }

        public double planeDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.planeDistance(point.lat(), point.lon(), lat, lon);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), Geohash.decodeLatitude(geohash),
                Geohash.decodeLongitude(geohash));
        }

        public double geohashDistanceWithDefault(String geohash, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return geohashDistance(geohash);
        }
    }

    public static final class Booleans extends ScriptDocValues<Boolean> {

        private final SortedNumericDocValues in;
        private boolean[] values = new boolean[0];
        private int count;

        public Booleans(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue() == 1;
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = grow(values, count);
        }

        public boolean getValue() {
            return get(0);
        }

        @Override
        public Boolean get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        private static boolean[] grow(boolean[] array, int minSize) {
            assert minSize >= 0 : "size must be positive (got " + minSize
                    + "): likely integer overflow?";
            if (array.length < minSize) {
                return Arrays.copyOf(array, ArrayUtil.oversize(minSize, 1));
            } else
                return array;
        }

    }

    abstract static class BinaryScriptDocValues<T> extends ScriptDocValues<T> {

        private final SortedBinaryDocValues in;
        protected BytesRefBuilder[] values = new BytesRefBuilder[0];
        protected int count;

        BinaryScriptDocValues(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public int size() {
            return count;
        }

    }

    public static final class Strings extends BinaryScriptDocValues<String> {

        public Strings(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public String get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index].get().utf8ToString();
        }

        public String getValue() {
            return get(0);
        }
    }

    public static final class BytesRefs extends BinaryScriptDocValues<BytesRef> {

        public BytesRefs(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public BytesRef get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            /**
             * We need to make a copy here because {@link BinaryScriptDocValues} might reuse the
             * returned value and the same instance might be used to
             * return values from multiple documents.
             **/
            return values[index].toBytesRef();
        }

        public BytesRef getValue() {
            return get(0);
        }

    }
}
