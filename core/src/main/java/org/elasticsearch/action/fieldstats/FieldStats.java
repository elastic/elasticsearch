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

package org.elasticsearch.action.fieldstats;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;

public abstract class FieldStats<T> implements Writeable, ToXContent {
    private final byte type;
    private long maxDoc;
    private long docCount;
    private long sumDocFreq;
    private long sumTotalTermFreq;
    private boolean isSearchable;
    private boolean isAggregatable;
    protected T minValue;
    protected T maxValue;

    FieldStats(byte type, long maxDoc, boolean isSearchable, boolean isAggregatable) {
        this(type, maxDoc, 0, 0, 0, isSearchable, isAggregatable, null, null);
    }

    FieldStats(byte type,
               long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
               boolean isSearchable, boolean isAggregatable, T minValue, T maxValue) {
        this.type = type;
        this.maxDoc = maxDoc;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    byte getType() {
        return this.type;
    }

    /**
     * @return the total number of documents.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public long getMaxDoc() {
        return maxDoc;
    }

    /**
     * @return the number of documents that have at least one term for this field,
     * or -1 if this measurement isn't available.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public long getDocCount() {
        return docCount;
    }

    /**
     * @return The percentage of documents that have at least one value for this field.
     *
     * This is a derived statistic and is based on: 'doc_count / max_doc'
     */
    public int getDensity() {
        if (docCount < 0 || maxDoc <= 0) {
            return -1;
        }
        return (int) (docCount * 100 / maxDoc);
    }

    /**
     * @return the sum of each term's document frequency in this field, or -1 if this measurement isn't available.
     * Document frequency is the number of documents containing a particular term.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    /**
     * @return the sum of the term frequencies of all terms in this field across all documents,
     * or -1 if this measurement
     * isn't available. Term frequency is the total number of occurrences of a term in a particular document and field.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public long getSumTotalTermFreq() {
        return sumTotalTermFreq;
    }

    /**
     * @return <code>true</code> if any of the instances of the field name is searchable.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * @return <code>true</code> if any of the instances of the field name is aggregatable.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * @return the lowest value in the field.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public T getMinValue() {
        return minValue;
    }

    /**
     * @return the highest value in the field.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public T getMaxValue() {
        return maxValue;
    }

    /**
     * @return the lowest value in the field represented as a string.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public abstract String getMinValueAsString();

    /**
     * @return the highest value in the field represented as a string.
     *
     * Note that, documents marked as deleted that haven't yet been merged way aren't taken into account.
     */
    public abstract String getMaxValueAsString();

    /**
     * @param value The string to be parsed
     * @param optionalFormat A string describing how to parse the specified value. Whether this parameter is supported
     *                       depends on the implementation. If optionalFormat is specified and the implementation
     *                       doesn't support it an {@link UnsupportedOperationException} is thrown
     */
    protected abstract T valueOf(String value, String optionalFormat);

    /**
     * Accumulates the provided stats into this stats instance.
     */
    public final void accumulate(FieldStats other) {
        this.maxDoc += other.maxDoc;
        if (other.docCount == -1) {
            this.docCount = -1;
        } else if (this.docCount != -1) {
            this.docCount += other.docCount;
        }
        if (other.sumDocFreq == -1) {
            this.sumDocFreq = -1;
        } else if (this.sumDocFreq != -1) {
            this.sumDocFreq += other.sumDocFreq;
        }
        if (other.sumTotalTermFreq == -1) {
            this.sumTotalTermFreq = -1;
        } else if (this.sumTotalTermFreq != -1) {
            this.sumTotalTermFreq += other.sumTotalTermFreq;
        }

        isSearchable |= other.isSearchable;
        isAggregatable |= other.isAggregatable;

        assert type == other.getType();
        updateMinMax((T) other.minValue, (T) other.maxValue);
    }

    private void updateMinMax(T min, T max) {
        if (minValue == null) {
            minValue = min;
        } else if (min != null && compare(minValue, min) > 0) {
            minValue = min;
        }
        if (maxValue == null) {
            maxValue = max;
        } else if (max != null && compare(maxValue, max) < 0) {
            maxValue = max;
        }
    }

    protected abstract int compare(T o1, T o2);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.MAX_DOC, maxDoc);
        builder.field(Fields.DOC_COUNT, docCount);
        builder.field(Fields.DENSITY, getDensity());
        builder.field(Fields.SUM_DOC_FREQ, sumDocFreq);
        builder.field(Fields.SUM_TOTAL_TERM_FREQ, sumTotalTermFreq);
        builder.field(Fields.SEARCHABLE, isSearchable);
        builder.field(Fields.AGGREGATABLE, isAggregatable);
        toInnerXContent(builder);
        builder.endObject();
        return builder;
    }

    protected void toInnerXContent(XContentBuilder builder) throws IOException {
        builder.field(Fields.MIN_VALUE, getMinValue());
        builder.field(Fields.MIN_VALUE_AS_STRING, getMinValueAsString());
        builder.field(Fields.MAX_VALUE, getMaxValue());
        builder.field(Fields.MAX_VALUE_AS_STRING, getMaxValueAsString());
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(maxDoc);
        out.writeLong(docCount);
        out.writeLong(sumDocFreq);
        out.writeLong(sumTotalTermFreq);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        boolean hasMinMax = minValue != null;
        out.writeBoolean(hasMinMax);
        if (hasMinMax) {
            writeMinMax(out);
        }
    }

    protected abstract void writeMinMax(StreamOutput out) throws IOException;

    /**
     * @return <code>true</code> if this instance matches with the provided index constraint,
     * otherwise <code>false</code> is returned
     */
    public boolean match(IndexConstraint constraint) {
        if (minValue == null) {
            return false;
        }
        int cmp;
        T value  = valueOf(constraint.getValue(), constraint.getOptionalFormat());
        if (constraint.getProperty() == IndexConstraint.Property.MIN) {
            cmp = compare(minValue, value);
        } else if (constraint.getProperty() == IndexConstraint.Property.MAX) {
            cmp = compare(maxValue, value);
        } else {
            throw new IllegalArgumentException("Unsupported property [" + constraint.getProperty() + "]");
        }

        switch (constraint.getComparison()) {
            case GT:
                return cmp > 0;
            case GTE:
                return cmp >= 0;
            case LT:
                return cmp < 0;
            case LTE:
                return cmp <= 0;
            default:
                throw new IllegalArgumentException("Unsupported comparison [" + constraint.getComparison() + "]");
        }
    }

    public static class Long extends FieldStats<java.lang.Long> {
        public Long(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    long minValue, long maxValue) {
            super((byte) 0, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable, minValue, maxValue);
        }

        public Long(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable) {
            super((byte) 0, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable, null, null);
        }

        public Long(long maxDoc,
                    boolean isSearchable, boolean isAggregatable) {
            super((byte) 0, maxDoc, isSearchable, isAggregatable);
        }

        @Override
        public int compare(java.lang.Long o1, java.lang.Long o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeLong(minValue);
            out.writeLong(maxValue);
        }

        @Override
        public java.lang.Long valueOf(String value, String optionalFormat) {
            return java.lang.Long.parseLong(value);
        }

        @Override
        public String getMinValueAsString() {
            return minValue != null ? java.lang.Long.toString(minValue) : null;
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue != null ? java.lang.Long.toString(maxValue) : null;
        }
    }

    public static class Double extends FieldStats<java.lang.Double> {
        public Double(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                      boolean isSearchable, boolean isAggregatable,
                      double minValue, double maxValue) {
            super((byte) 1, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                minValue, maxValue);
        }

        public Double(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                      boolean isSearchable, boolean isAggregatable) {
            super((byte) 1, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, null, null);
        }

        public Double(long maxDoc, boolean isSearchable, boolean isAggregatable) {
            super((byte) 1, maxDoc, isSearchable, isAggregatable);
        }

        @Override
        public int compare(java.lang.Double o1, java.lang.Double o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeDouble(minValue);
            out.writeDouble(maxValue);
        }

        @Override
        public java.lang.Double valueOf(String value, String optionalFormat) {
            if (optionalFormat != null) {
                throw new UnsupportedOperationException("custom format isn't supported");
            }
            return java.lang.Double.parseDouble(value);
        }

        @Override
        public String getMinValueAsString() {
            return minValue != null ? java.lang.Double.toString(minValue) : null;
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue != null ? java.lang.Double.toString(maxValue) : null;
        }
    }

    public static class Date extends FieldStats<java.lang.Long> {
        private FormatDateTimeFormatter formatter;

        public Date(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    FormatDateTimeFormatter formatter,
                    long minValue, long maxValue) {
            super((byte) 2, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                minValue, maxValue);
            this.formatter = formatter;
        }

        public Date(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    FormatDateTimeFormatter formatter) {
            super((byte) 2, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                null, null);
            this.formatter = formatter;
        }

        public Date(long maxDoc, boolean isSearchable, boolean isAggregatable,
                    FormatDateTimeFormatter formatter) {
            super((byte) 2, maxDoc, isSearchable, isAggregatable);
            this.formatter = formatter;
        }

        @Override
        public int compare(java.lang.Long o1, java.lang.Long o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeString(formatter.format());
            out.writeLong(minValue);
            out.writeLong(maxValue);
        }

        @Override
        public java.lang.Long valueOf(String value, String fmt) {
            FormatDateTimeFormatter f = formatter;
            if (fmt != null) {
                f = Joda.forPattern(fmt);
            }
            return f.parser().parseDateTime(value).getMillis();
        }

        @Override
        public String getMinValueAsString() {
            return minValue != null ? formatter.printer().print(minValue) : null;
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue != null ? formatter.printer().print(maxValue) : null;
        }
    }

    public static class Text extends FieldStats<BytesRef> {
        public Text(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    BytesRef minValue, BytesRef maxValue) {
            super((byte) 3, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable,
                minValue, maxValue);
        }

        public Text(long maxDoc, boolean isSearchable, boolean isAggregatable) {
            super((byte) 3, maxDoc, isSearchable, isAggregatable);
        }

        @Override
        public int compare(BytesRef o1, BytesRef o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeBytesRef(minValue);
            out.writeBytesRef(maxValue);
        }

        @Override
        protected BytesRef valueOf(String value, String optionalFormat) {
            if (optionalFormat != null) {
                throw new UnsupportedOperationException("custom format isn't supported");
            }
            return new BytesRef(value);
        }

        @Override
        public String getMinValueAsString() {
            return minValue != null ? minValue.utf8ToString() : null;
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue != null ? maxValue.utf8ToString() : null;
        }

        @Override
        protected void toInnerXContent(XContentBuilder builder) throws IOException {
            builder.field(Fields.MIN_VALUE, getMinValueAsString());
            builder.field(Fields.MAX_VALUE, getMaxValueAsString());
        }
    }

    public static class Ip extends FieldStats<InetAddress> {
        public Ip(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                  boolean isSearchable, boolean isAggregatable,
                  InetAddress minValue, InetAddress maxValue) {
            super((byte) 4, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable,
                minValue, maxValue);
        }

        public Ip(long maxDoc, boolean isSearchable, boolean isAggregatable) {
            super((byte) 4, maxDoc, isSearchable, isAggregatable);
        }

        @Override
        public int compare(InetAddress o1, InetAddress o2) {
            byte[] b1 = InetAddressPoint.encode(o1);
            byte[] b2 = InetAddressPoint.encode(o2);
            return StringHelper.compare(b1.length, b1, 0, b2, 0);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            byte[] b1 = InetAddressPoint.encode(minValue);
            byte[] b2 = InetAddressPoint.encode(maxValue);
            out.writeByte((byte) b1.length);
            out.writeBytes(b1);
            out.writeByte((byte) b2.length);
            out.writeBytes(b2);
        }

        @Override
        public InetAddress valueOf(String value, String fmt) {
            return InetAddresses.forString(value);
        }

        @Override
        public String getMinValueAsString() {
            return  minValue != null ? NetworkAddress.format(minValue) : null;
        }

        @Override
        public String getMaxValueAsString() {
            return  maxValue != null ? NetworkAddress.format(maxValue) : null;
        }
    }

    public static FieldStats readFrom(StreamInput in) throws IOException {
        byte type = in.readByte();
        long maxDoc = in.readLong();
        long docCount = in.readLong();
        long sumDocFreq = in.readLong();
        long sumTotalTermFreq = in.readLong();
        boolean isSearchable = in.readBoolean();
        boolean isAggregatable = in.readBoolean();
        boolean hasMinMax = in.readBoolean();

        switch (type) {
            case 0:
                if (hasMinMax) {
                    return new Long(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readLong(), in.readLong());
                }
                return new Long(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable);

            case 1:
                if (hasMinMax) {
                    return new Double(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readDouble(), in.readDouble());
                }
                return new Double(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable);

            case 2:
                FormatDateTimeFormatter formatter = Joda.forPattern(in.readString());
                if (hasMinMax) {
                    return new Date(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, formatter, in.readLong(), in.readLong());
                }
                return new Date(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable, formatter);

            case 3:
                if (hasMinMax) {
                    return new Text(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readBytesRef(), in.readBytesRef());
                }
                return new Text(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, null, null);

            case 4:
                InetAddress min = null;
                InetAddress max = null;
                if (hasMinMax) {
                    int l1 = in.readByte();
                    byte[] b1 = new byte[l1];
                    int l2 = in.readByte();
                    byte[] b2 = new byte[l2];
                    min = InetAddressPoint.decode(b1);
                    max = InetAddressPoint.decode(b2);
                }
                return new Ip(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable, min, max);

            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    public static String typeName(byte type) {
        switch (type) {
            case 0:
                return "whole-number";
            case 1:
                return "floating-point";
            case 2:
                return "date";
            case 3:
                return "text";
            case 4:
                return "ip";
            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    private final static class Fields {
        final static String MAX_DOC = new String("max_doc");
        final static String DOC_COUNT = new String("doc_count");
        final static String DENSITY = new String("density");
        final static String SUM_DOC_FREQ = new String("sum_doc_freq");
        final static String SUM_TOTAL_TERM_FREQ = new String("sum_total_term_freq");
        final static String SEARCHABLE = new String("searchable");
        final static String AGGREGATABLE = new String("aggregatable");
        final static String MIN_VALUE = new String("min_value");
        final static String MIN_VALUE_AS_STRING = new String("min_value_as_string");
        final static String MAX_VALUE = new String("max_value");
        final static String MAX_VALUE_AS_STRING = new String("max_value_as_string");
    }
}
