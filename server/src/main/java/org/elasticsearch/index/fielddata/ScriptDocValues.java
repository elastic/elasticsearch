/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.script.Field;
import org.elasticsearch.script.FieldValues;
import org.elasticsearch.script.InvalidConversion;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Script level doc values, the assumption is that any implementation will
 * implement a {@link Longs#getValue getValue} method.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> implements FieldValues<T> {

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

    public abstract Field<T> toField(String fieldName);

    public List<T> getValues() {
        return this;
    }

    public T getNonPrimitiveValue() {
        return get(0);
    }

    public long getLongValue() {
        throw new InvalidConversion(this.getClass(), long.class);
    }

    public double getDoubleValue() {
        throw new InvalidConversion(this.getClass(), double.class);
    }

    protected void throwIfEmpty() {
        if (size() == 0) {
            throw new IllegalStateException("A document doesn't have a value for a field! " +
                "Use doc[<field>].size()==0 to check if a document is missing a field!");
        }
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
            throwIfEmpty();
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public long getLongValue() {
            throwIfEmpty();
            return values[0];
        }

        @Override
        public double getDoubleValue() {
            throwIfEmpty();
            return values[0];
        }

        @Override
        public Field<Long> toField(String fieldName) {
            return new Field.LongField(fieldName, this);
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

        @Override
        public long getLongValue() {
            throwIfEmpty();
            Instant dt = dates[0].toInstant();
            if (isNanos) {
                return ChronoUnit.NANOS.between(java.time.Instant.EPOCH, dt);
            }
            return dt.toEpochMilli();
        }

        @Override
        public double getDoubleValue() {
            return getLongValue();
        }

        @Override
        public Field<JodaCompatibleZonedDateTime> toField(String fieldName) {
            if (isNanos) {
                return new Field.DateNanosField(fieldName, this);
            }
            return new Field.DateMillisField(fieldName, this);
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

        @Override
        public long getLongValue() {
            return (long) getDoubleValue();
        }

        @Override
        public double getDoubleValue() {
            throwIfEmpty();
            return values[0];
        }

        @Override
        public Field<Double> toField(String fieldName) {
            return new Field.DoubleField(fieldName, this);
        }
    }

    public abstract static class Geometry<T> extends ScriptDocValues<T> {
        /** Returns the dimensional type of this geometry */
        public abstract int getDimensionalType();
        /** Returns the bounding box of this geometry  */
        public abstract GeoBoundingBox getBoundingBox();
        /** Returns the centroid of this geometry  */
        public abstract GeoPoint getCentroid();
        /** Returns the width of the bounding box diagonal in the spherical Mercator projection (meters)  */
        public abstract double getMercatorWidth();
        /** Returns the height of the bounding box diagonal in the spherical Mercator projection (meters) */
        public abstract double getMercatorHeight();
    }

    public static final class GeoPoints extends Geometry<GeoPoint> {

        private final MultiGeoPointValues in;
        private GeoPoint[] values = new GeoPoint[0];
        private final GeoPoint centroid = new GeoPoint();
        private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        private int count;

        public GeoPoints(MultiGeoPointValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                if (count == 1) {
                    setSingleValue();
                } else {
                    setMultiValue();
                }
            } else {
                resize(0);
            }
        }

        private void setSingleValue() throws IOException {
            GeoPoint point = in.nextValue();
            values[0].reset(point.lat(), point.lon());
            centroid.reset(point.lat(), point.lon());
            boundingBox.topLeft().reset(point.lat(), point.lon());
            boundingBox.bottomRight().reset(point.lat(), point.lon());
        }

        private void setMultiValue() throws IOException {
            double centroidLat = 0;
            double centroidLon = 0;
            double maxLon = Double.NEGATIVE_INFINITY;
            double minLon = Double.POSITIVE_INFINITY;
            double maxLat = Double.NEGATIVE_INFINITY;
            double minLat = Double.POSITIVE_INFINITY;
            for (int i = 0; i < count; i++) {
                GeoPoint point = in.nextValue();
                values[i].reset(point.lat(), point.lon());
                centroidLat += point.getLat();
                centroidLon += point.getLon();
                maxLon = Math.max(maxLon, values[i].getLon());
                minLon = Math.min(minLon, values[i].getLon());
                maxLat = Math.max(maxLat, values[i].getLat());
                minLat = Math.min(minLat, values[i].getLat());
            }
            centroid.reset(centroidLat / count, centroidLon / count);
            boundingBox.topLeft().reset(maxLat, minLon);
            boundingBox.bottomRight().reset(minLat, maxLon);
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

        @Override
        public int getDimensionalType() {
            return size() == 0 ? -1 : 0;
        }

        @Override
        public GeoPoint getCentroid() {
            return size() == 0 ? null : centroid;
        }

        @Override
        public double getMercatorWidth() {
            return 0;
        }

        @Override
        public double getMercatorHeight() {
            return 0;
        }

        @Override
        public GeoBoundingBox getBoundingBox() {
          return size() == 0 ? null : boundingBox;
        }

        @Override
        public Field<GeoPoint> toField(String fieldName) {
            return new Field.GeoPointField(fieldName, this);
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

        @Override
        public long getLongValue() {
            throwIfEmpty();
            return values[0] ? 1L : 0L;
        }

        @Override
        public double getDoubleValue() {
            throwIfEmpty();
            return values[0] ? 1.0D : 0.0D;
        }

        @Override
        public Field<Boolean> toField(String fieldName) {
            return new Field.BooleanField(fieldName, this);
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

    public static class Strings extends BinaryScriptDocValues<String> {
        public Strings(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public final String get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return bytesToString(values[index].get());
        }

        /**
         * Convert the stored bytes to a String.
         */
        protected String bytesToString(BytesRef bytes) {
            return bytes.utf8ToString();
        }

        public final String getValue() {
            return get(0);
        }

        @Override
        public long getLongValue() {
            return Long.parseLong(get(0));
        }

        @Override
        public double getDoubleValue() {
            return Double.parseDouble(get(0));
        }

        @Override
        public Field<String> toField(String fieldName) {
            return new Field.StringField(fieldName, this);
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

        @Override
        public Field<BytesRef> toField(String fieldName) {
            return new Field.BytesRefField(fieldName, this);
        }
    }
}
