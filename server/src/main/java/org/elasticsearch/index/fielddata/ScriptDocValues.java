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
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.BooleanDocValuesField;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractList;
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

    protected void throwIfEmpty() {
        if (size() == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! " + "Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
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
    }

    public static final class Dates extends ScriptDocValues<ZonedDateTime> {

        private final SortedNumericDocValues in;
        private final boolean isNanos;

        /**
         * Values wrapped in {@link java.time.ZonedDateTime} objects.
         */
        private ZonedDateTime[] dates;
        private int count;

        public Dates(SortedNumericDocValues in, boolean isNanos) {
            this.in = in;
            this.isNanos = isNanos;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public ZonedDateTime getValue() {
            return get(0);
        }

        @Override
        public ZonedDateTime get(int index) {
            if (count == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            if (index >= count) {
                throw new IndexOutOfBoundsException(
                    "attempted to fetch the [" + index + "] date when there are only [" + count + "] dates."
                );
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
                dates = new ZonedDateTime[count];
            }
            for (int i = 0; i < count; ++i) {
                if (isNanos) {
                    dates[i] = ZonedDateTime.ofInstant(DateUtils.toInstant(in.nextValue()), ZoneOffset.UTC);
                } else {
                    dates[i] = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.nextValue()), ZoneOffset.UTC);
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
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
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
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
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
            return GeoUtils.arcDistance(point.lat(), point.lon(), Geohash.decodeLatitude(geohash), Geohash.decodeLongitude(geohash));
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
    }

    public static final class Booleans extends ScriptDocValues<Boolean> {

        private final BooleanDocValuesField booleanDocValuesField;

        public Booleans(BooleanDocValuesField booleanDocValuesField) {
            this.booleanDocValuesField = booleanDocValuesField;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            throw new UnsupportedOperationException();
        }

        public boolean getValue() {
            throwIfEmpty();
            return get(0);
        }

        @Override
        public Boolean get(int index) {
            throwIfEmpty();
            return booleanDocValuesField.getInternal(index);
        }

        @Override
        public int size() {
            return booleanDocValuesField.size();
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
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
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
    }

    public static final class BytesRefs extends ScriptDocValues<BytesRef> {

        private final BinaryDocValuesField binaryDocValuesField;

        public BytesRefs(BinaryDocValuesField binaryDocValuesField) {
            this.binaryDocValuesField = binaryDocValuesField;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            throw new UnsupportedOperationException();
        }

        public BytesRef getValue() {
            throwIfEmpty();
            return get(0);
        }

        @Override
        public BytesRef get(int index) {
            throwIfEmpty();
            return binaryDocValuesField.getInternal(index);
        }

        @Override
        public int size() {
            return binaryDocValuesField.size();
        }
    }
}
