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
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;


/**
 * Script level doc values, the assumption is that any implementation will
 * implement a <code>getValue</code> and a <code>getValues</code> that return
 * the relevant type that then can be used in scripts.
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

    /**
     * Return a copy of the list of the values for the current document.
     */
    public final List<T> getValues() {
        return this;
    }

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
        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(Longs.class));

        private final SortedNumericDocValues in;
        private long[] values = new long[0];
        private int count;
        private Dates dates;
        private int docId = -1;

        public Longs(SortedNumericDocValues in) {
            this.in = in;

        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            this.docId = docId;
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
            if (dates != null) {
                dates.setNextDocId(docId);
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

        public SortedNumericDocValues getInternalValues() {
            return this.in;
        }

        public long getValue() {
            if (count == 0) {
                return 0L;
            }
            return values[0];
        }

        @Deprecated
        public ReadableDateTime getDate() throws IOException {
            deprecationLogger.deprecated("getDate on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in);
                dates.setNextDocId(docId);
            }
            return dates.getValue();
        }

        @Deprecated
        public List<ReadableDateTime> getDates() throws IOException {
            deprecationLogger.deprecated("getDates on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in);
                dates.setNextDocId(docId);
            }
            return dates;
        }

        @Override
        public Long get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static final class Dates extends ScriptDocValues<ReadableDateTime> {
        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(Dates.class));

        private static final ReadableDateTime EPOCH = new DateTime(0, DateTimeZone.UTC);

        private final SortedNumericDocValues in;
        /**
         * Values wrapped in {@link MutableDateTime}. Null by default an allocated on first usage so we allocate a reasonably size. We keep
         * this array so we don't have allocate new {@link MutableDateTime}s on every usage. Instead we reuse them for every document.
         */
        private MutableDateTime[] dates;
        private int count;

        public Dates(SortedNumericDocValues in) {
            this.in = in;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public ReadableDateTime getValue() {
            if (count == 0) {
                return EPOCH;
            }
            return get(0);
        }

        /**
         * Fetch the first value. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public ReadableDateTime getDate() {
            deprecationLogger.deprecated("getDate is no longer necessary on date fields as the value is now a date.");
            return getValue();
        }

        /**
         * Fetch all the values. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public List<ReadableDateTime> getDates() {
            deprecationLogger.deprecated("getDates is no longer necessary on date fields as the values are now dates.");
            return this;
        }

        @Override
        public ReadableDateTime get(int index) {
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
            if (dates == null) {
                // Happens for the document. We delay allocating dates so we can allocate it with a reasonable size.
                dates = new MutableDateTime[count];
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = new MutableDateTime(in.nextValue(), DateTimeZone.UTC);
                }
                return;
            }
            if (count > dates.length) {
                // Happens when we move to a new document and it has more dates than any documents before it.
                MutableDateTime[] backup = dates;
                dates = new MutableDateTime[count];
                System.arraycopy(backup, 0, dates, 0, backup.length);
                for (int i = 0; i < backup.length; i++) {
                    dates[i].setMillis(in.nextValue());
                }
                for (int i = backup.length; i < dates.length; i++) {
                    dates[i] = new MutableDateTime(in.nextValue(), DateTimeZone.UTC);
                }
                return;
            }
            for (int i = 0; i < count; i++) {
                dates[i] = new MutableDateTime(in.nextValue(), DateTimeZone.UTC);
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
            if (count == 0) {
                return 0d;
            }
            return values[0];
        }

        @Override
        public Double get(int index) {
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
            if (count == 0) {
                return null;
            }
            return values[0];
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            List<GeoPoint> points = getValues();
            double[] lats = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            List<GeoPoint> points = getValues();
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lons[i] = points.get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
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
            return GeoUtils.arcDistance(point.lat(), point.lon(), GeoHashUtils.decodeLatitude(geohash),
                GeoHashUtils.decodeLongitude(geohash));
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
            return count != 0 && values[0];
        }

        @Override
        public Boolean get(int index) {
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
                    // We need to make a copy here, because BytesBinaryDVAtomicFieldData's SortedBinaryDocValues
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
            return values[index].get().utf8ToString();
        }

        public BytesRef getBytesValue() {
            if (size() > 0) {
                return values[0].get();
            } else {
                return null;
            }
        }

        public String getValue() {
            BytesRef value = getBytesValue();
            if (value == null) {
                return null;
            } else {
                return value.utf8ToString();
            }
        }

    }

    public static final class BytesRefs extends BinaryScriptDocValues<BytesRef> {

        public BytesRefs(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public BytesRef get(int index) {
            return values[index].get();
        }

        public BytesRef getValue() {
            if (count == 0) {
                return new BytesRef();
            }
            return values[0].get();
        }

    }
}
