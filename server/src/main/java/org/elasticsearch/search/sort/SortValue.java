/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Comparable}, {@link DocValueFormat} aware wrapper around a sort value.
 */
public abstract class SortValue implements NamedWriteable, Comparable<SortValue> {
    private static final SortValue EMPTY_SORT_VALUE = new EmptySortValue();

    /**
     * Get a {@linkplain SortValue} for a double.
     */
    public static SortValue from(double d) {
        if (Double.isNaN(d)) {
            return new EmptySortValue();
        }
        return new DoubleSortValue(d);
    }

    /**
     * Get a {@linkplain SortValue} for a long.
     */
    public static SortValue from(long l) {
        return new LongSortValue(l);
    }

    /**
     * Get a {@linkplain SortValue} for bytes. Callers should be sure that they
     * have a {@link BytesRef#deepCopyOf} of any mutable references.
     */
    public static SortValue from(BytesRef bytes) {
        return new BytesSortValue(bytes);
    }

    /**
     * Get a {@linkplain SortValue} for data which cannot be sorted.
     */
    public static SortValue empty() {
        return EMPTY_SORT_VALUE;
    }

    /**
     * Get the list of {@linkplain NamedWriteable}s that this class needs.
     */
    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(SortValue.class, DoubleSortValue.NAME, DoubleSortValue::new),
            new NamedWriteableRegistry.Entry(SortValue.class, LongSortValue.NAME, LongSortValue::new),
            new NamedWriteableRegistry.Entry(SortValue.class, BytesSortValue.NAME, BytesSortValue::new),
            new NamedWriteableRegistry.Entry(SortValue.class, EmptySortValue.NAME, EmptySortValue::new)
        );
    }

    private SortValue() {
        // All subclasses of this class are defined in this file.
    }

    @Override
    public final int compareTo(SortValue other) {
        int typeComparison = typeComparisonKey() - other.typeComparisonKey();
        return typeComparison == 0 ? compareToSameType(other) : typeComparison;
    }

    /**
     * Write the key as xcontent.
     */
    public final XContentBuilder toXContent(XContentBuilder builder, DocValueFormat format) throws IOException {
        if (format == DocValueFormat.RAW) {
            return rawToXContent(builder);
        }
        return builder.value(format(format));
    }

    /**
     * The java object representing the sort value.
     */
    public abstract Object getKey();

    /**
     * Format this value using the provided format.
     */
    public abstract String format(DocValueFormat format);

    /**
     * Write the key as xcontent using the most native type possible.
     */
    protected abstract XContentBuilder rawToXContent(XContentBuilder builder) throws IOException;

    /**
     * Compare this sort value to another sort value of the same type.
     */
    protected abstract int compareToSameType(SortValue obj);

    // Force implementations to override equals for consistency with compareToSameType
    @Override
    public abstract boolean equals(Object obj);

    // Force implementations to override hashCode for consistency with equals
    @Override
    public abstract int hashCode();

    // Force implementations to override toString so debugging isn't a nightmare.
    @Override
    public abstract String toString();

    // Force implementations to override typeComparisonKey and associate each subclass with an integer key
    protected abstract int typeComparisonKey();

    /**
     * Return this {@linkplain SortValue} as a boxed {@linkplain Number}
     * or {@link Double#NaN} if it isn't a number. Or if it is actually
     * {@link Double#NaN}.
     */
    public abstract Number numberValue();

    private static class DoubleSortValue extends SortValue {
        public static final String NAME = "double";
        private static final int SORT_VALUE = -2;

        private final double key;

        private DoubleSortValue(double key) {
            this.key = key;
        }

        private DoubleSortValue(StreamInput in) throws IOException {
            this.key = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            DoubleSortValue other = (DoubleSortValue) obj;
            return Double.compare(key, other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            DoubleSortValue other = (DoubleSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(key);
        }

        @Override
        public String toString() {
            return Double.toString(key);
        }

        @Override
        public int typeComparisonKey() {
            return SORT_VALUE;
        }

        @Override
        public Number numberValue() {
            return key;
        }
    }

    private static class LongSortValue extends SortValue {
        public static final String NAME = "long";
        private static final int SORT_VALUE = -1;

        private final long key;

        LongSortValue(long key) {
            this.key = key;
        }

        private LongSortValue(StreamInput in) throws IOException {
            key = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            LongSortValue other = (LongSortValue) obj;
            return Long.compare(key, other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            LongSortValue other = (LongSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key);
        }

        @Override
        public String toString() {
            return Long.toString(key);
        }

        @Override
        public int typeComparisonKey() {
            return SORT_VALUE;
        }

        @Override
        public Number numberValue() {
            return key;
        }
    }

    private static class BytesSortValue extends SortValue {
        public static final String NAME = "bytes";
        private static final int SORT_VALUE = -3;

        private final BytesRef key;

        BytesSortValue(BytesRef key) {
            this.key = key;
        }

        private BytesSortValue(StreamInput in) throws IOException {
            key = in.readBytesRef();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().before(TransportVersion.V_7_11_0)) {
                throw new IllegalArgumentException(
                    "transport versions before [7110099] can't handle non-numeric sort values, attempted to send to ["
                        + out.getTransportVersion()
                        + "]"
                );
            }
            out.writeBytesRef(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key.utf8ToString());
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            BytesSortValue other = (BytesSortValue) obj;
            return key.compareTo(other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            BytesSortValue other = (BytesSortValue) obj;
            return key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return key.toString();
        }

        @Override
        public int typeComparisonKey() {
            return SORT_VALUE;
        }

        @Override
        public Number numberValue() {
            return Double.NaN;
        }
    }

    private static class EmptySortValue extends SortValue {

        public static final String NAME = "empty";
        private static final String EMPTY_STRING = "";
        private int sortValue = 0;

        private EmptySortValue() {}

        private EmptySortValue(StreamInput ignoredIn) {}

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public Object getKey() {
            return EMPTY_STRING;
        }

        @Override
        public String format(DocValueFormat format) {
            return EMPTY_STRING;
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder;
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && false != getClass().equals(obj.getClass());
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return EMPTY_STRING;
        }

        @Override
        public int typeComparisonKey() {
            return sortValue;
        }

        @Override
        public Number numberValue() {
            return null;
        }
    }
}
