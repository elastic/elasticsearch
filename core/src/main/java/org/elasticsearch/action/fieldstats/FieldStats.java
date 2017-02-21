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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.Version;
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
import java.util.Objects;

public abstract class FieldStats<T> implements Writeable, ToXContent {

    private final byte type;
    private long maxDoc;
    private long docCount;
    private long sumDocFreq;
    private long sumTotalTermFreq;
    private boolean isSearchable;
    private boolean isAggregatable;
    private boolean hasMinMax;
    protected T minValue;
    protected T maxValue;

    /**
     * Builds a FieldStats where min and max value are not available for the field.
     * @param type The native type of this FieldStats
     * @param maxDoc Max number of docs
     * @param docCount  the number of documents that have at least one term for this field,
     *                  or -1 if this information isn't available for this field.
     * @param sumDocFreq  the sum of {@link TermsEnum#docFreq()} for all terms in this field,
     *                    or -1 if this information isn't available for this field.
     * @param sumTotalTermFreq the sum of {@link TermsEnum#totalTermFreq} for all terms in this field,
     *                         or -1 if this measure isn't available for this field.
     * @param isSearchable true if this field is searchable
     * @param isAggregatable true if this field is aggregatable
     */
    FieldStats(byte type, long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
               boolean isSearchable, boolean isAggregatable) {
        this.type = type;
        this.maxDoc = maxDoc;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.hasMinMax = false;
    }

    /**
     * Builds a FieldStats with min and max value for the field.
     * @param type The native type of this FieldStats
     * @param maxDoc Max number of docs
     * @param docCount  the number of documents that have at least one term for this field,
     *                  or -1 if this information isn't available for this field.
     * @param sumDocFreq  the sum of {@link TermsEnum#docFreq()} for all terms in this field,
     *                    or -1 if this information isn't available for this field.
     * @param sumTotalTermFreq the sum of {@link TermsEnum#totalTermFreq} for all terms in this field,
     *                         or -1 if this measure isn't available for this field.
     * @param isSearchable true if this field is searchable
     * @param isAggregatable true if this field is aggregatable
     * @param minValue the minimum value indexed in this field
     * @param maxValue the maximum value indexed in this field
     */
    FieldStats(byte type,
               long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
               boolean isSearchable, boolean isAggregatable, T minValue, T maxValue) {
        Objects.requireNonNull(minValue, "minValue must not be null");
        Objects.requireNonNull(maxValue, "maxValue must not be null");
        this.type = type;
        this.maxDoc = maxDoc;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.hasMinMax = true;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    byte getType() {
        return this.type;
    }

    public String getDisplayType() {
        switch (type) {
            case 0:
                return "integer";
            case 1:
                return "float";
            case 2:
                return "date";
            case 3:
                return "string";
            case 4:
                return "ip";
            case 5:
                return "geo_point";
            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    /**
     * @return true if min/max information is available for this field
     */
    public boolean hasMinMax() {
        return hasMinMax;
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
        if (hasMinMax && other.hasMinMax) {
            updateMinMax((T) other.minValue, (T) other.maxValue);
        } else {
            hasMinMax = false;
            minValue = null;
            maxValue = null;
        }
    }

    protected void updateMinMax(T min, T max) {
        if (compare(minValue, min) > 0) {
            minValue = min;
        }
        if (compare(maxValue, max) < 0) {
            maxValue = max;
        }
    }

    protected abstract int compare(T o1, T o2);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD, getDisplayType());
        builder.field(MAX_DOC_FIELD, maxDoc);
        builder.field(DOC_COUNT_FIELD, docCount);
        builder.field(DENSITY_FIELD, getDensity());
        builder.field(SUM_DOC_FREQ_FIELD, sumDocFreq);
        builder.field(SUM_TOTAL_TERM_FREQ_FIELD, sumTotalTermFreq);
        builder.field(SEARCHABLE_FIELD, isSearchable);
        builder.field(AGGREGATABLE_FIELD, isAggregatable);
        if (hasMinMax) {
            toInnerXContent(builder);
        }
        builder.endObject();
        return builder;
    }

    protected void toInnerXContent(XContentBuilder builder) throws IOException {
        builder.field(MIN_VALUE_FIELD, getMinValue());
        builder.field(MIN_VALUE_AS_STRING_FIELD, getMinValueAsString());
        builder.field(MAX_VALUE_FIELD, getMaxValue());
        builder.field(MAX_VALUE_AS_STRING_FIELD, getMaxValueAsString());
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
        if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            out.writeBoolean(hasMinMax);
            if (hasMinMax) {
                writeMinMax(out);
            }
        } else {
            assert hasMinMax : "cannot serialize null min/max fieldstats in a mixed-cluster " +
                    "with pre-" + Version.V_5_2_0_UNRELEASED + " nodes, remote version [" + out.getVersion() + "]";
            writeMinMax(out);
        }
    }

    protected abstract void writeMinMax(StreamOutput out) throws IOException;

    /**
     * @return <code>true</code> if this instance matches with the provided index constraint,
     * otherwise <code>false</code> is returned
     */
    public boolean match(IndexConstraint constraint) {
        if (hasMinMax == false) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldStats<?> that = (FieldStats<?>) o;

        if (type != that.type) return false;
        if (maxDoc != that.maxDoc) return false;
        if (docCount != that.docCount) return false;
        if (sumDocFreq != that.sumDocFreq) return false;
        if (sumTotalTermFreq != that.sumTotalTermFreq) return false;
        if (isSearchable != that.isSearchable) return false;
        if (isAggregatable != that.isAggregatable) return false;
        if (hasMinMax != that.hasMinMax) return false;
        if (hasMinMax == false) {
            return true;
        }
        if (!minValue.equals(that.minValue)) return false;
        return maxValue.equals(that.maxValue);

    }

    @Override
    public int hashCode() {
        return Objects.hash(type, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
            hasMinMax, minValue, maxValue);
    }

    public static class Long extends FieldStats<java.lang.Long> {
        public Long(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable) {
            super((byte) 0, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable);
        }

        public Long(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    long minValue, long maxValue) {
            super((byte) 0, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable, minValue, maxValue);
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
            return java.lang.Long.toString(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return java.lang.Long.toString(maxValue);
        }
    }

    public static class Double extends FieldStats<java.lang.Double> {
        public Double(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                      boolean isSearchable, boolean isAggregatable) {
            super((byte) 1, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable);
        }

        public Double(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                      boolean isSearchable, boolean isAggregatable,
                      double minValue, double maxValue) {
            super((byte) 1, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                minValue, maxValue);
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
            return java.lang.Double.toString(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return java.lang.Double.toString(maxValue);
        }
    }

    public static class Date extends FieldStats<java.lang.Long> {
        private FormatDateTimeFormatter formatter;

        public Date(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable) {
            super((byte) 2, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable);
            this.formatter = null;
        }

        public Date(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    FormatDateTimeFormatter formatter,
                    long minValue, long maxValue) {
            super((byte) 2, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                minValue, maxValue);
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
            return formatter.printer().print(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return formatter.printer().print(maxValue);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            Date that = (Date) o;
            return Objects.equals(formatter == null ? null : formatter.format(),
                that.formatter == null ? null : that.formatter.format());
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (formatter == null ? 0 : formatter.format().hashCode());
            return result;
        }
    }

    public static class Text extends FieldStats<BytesRef> {
        public Text(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable) {
            super((byte) 3, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable);
        }

        public Text(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                    boolean isSearchable, boolean isAggregatable,
                    BytesRef minValue, BytesRef maxValue) {
            super((byte) 3, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable,
                minValue, maxValue);
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
            return minValue.utf8ToString();
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue.utf8ToString();
        }

        @Override
        protected void toInnerXContent(XContentBuilder builder) throws IOException {
            builder.field(MIN_VALUE_FIELD, getMinValueAsString());
            builder.field(MAX_VALUE_FIELD, getMaxValueAsString());
        }
    }

    public static class Ip extends FieldStats<InetAddress> {
        public Ip(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                  boolean isSearchable, boolean isAggregatable) {
            super((byte) 4, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable);
        }


        public Ip(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                  boolean isSearchable, boolean isAggregatable,
                  InetAddress minValue, InetAddress maxValue) {
            super((byte) 4, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable,
                minValue, maxValue);
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
            return NetworkAddress.format(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return NetworkAddress.format(maxValue);
        }
    }

    public static class GeoPoint extends FieldStats<org.elasticsearch.common.geo.GeoPoint> {
        public GeoPoint(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                  boolean isSearchable, boolean isAggregatable) {
            super((byte) 5, maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                isSearchable, isAggregatable);
        }

        public GeoPoint(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq,
                        boolean isSearchable, boolean isAggregatable,
                        org.elasticsearch.common.geo.GeoPoint minValue, org.elasticsearch.common.geo.GeoPoint maxValue) {
            super((byte) 5, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable,
                minValue, maxValue);
        }

        @Override
        public org.elasticsearch.common.geo.GeoPoint valueOf(String value, String fmt) {
            return org.elasticsearch.common.geo.GeoPoint.parseFromLatLon(value);
        }

        @Override
        protected void updateMinMax(org.elasticsearch.common.geo.GeoPoint min, org.elasticsearch.common.geo.GeoPoint max) {
            minValue.reset(Math.min(min.lat(), minValue.lat()), Math.min(min.lon(), minValue.lon()));
            maxValue.reset(Math.max(max.lat(), maxValue.lat()), Math.max(max.lon(), maxValue.lon()));
        }

        @Override
        public int compare(org.elasticsearch.common.geo.GeoPoint p1, org.elasticsearch.common.geo.GeoPoint p2) {
            throw new IllegalArgumentException("compare is not supported for geo_point field stats");
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeDouble(minValue.lat());
            out.writeDouble(minValue.lon());
            out.writeDouble(maxValue.lat());
            out.writeDouble(maxValue.lon());
        }

        @Override
        public String getMinValueAsString() {
            return minValue.toString();
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue.toString();
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
        boolean hasMinMax = true;
        if (in.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            hasMinMax = in.readBoolean();
        }
        switch (type) {
            case 0:
                if (hasMinMax) {
                    return new Long(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readLong(), in.readLong());
                } else {
                    return new Long(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }
            case 1:
                if (hasMinMax) {
                    return new Double(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readDouble(), in.readDouble());
                } else {
                    return new Double(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }
            case 2:
                if (hasMinMax) {
                    FormatDateTimeFormatter formatter = Joda.forPattern(in.readString());
                    return new Date(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, formatter, in.readLong(), in.readLong());
                } else {
                    return new Date(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }
            case 3:
                if (hasMinMax) {
                    return new Text(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable, in.readBytesRef(), in.readBytesRef());
                } else {
                    return new Text(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }

            case 4: {
                if (hasMinMax == false) {
                    return new Ip(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }
                int l1 = in.readByte();
                byte[] b1 = new byte[l1];
                in.readBytes(b1, 0, l1);
                int l2 = in.readByte();
                byte[] b2 = new byte[l2];
                in.readBytes(b2, 0, l2);
                InetAddress min = InetAddressPoint.decode(b1);
                InetAddress max = InetAddressPoint.decode(b2);
                return new Ip(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable, min, max);
            }
            case 5: {
                if (hasMinMax == false) {
                    return new GeoPoint(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                        isSearchable, isAggregatable);
                }
                org.elasticsearch.common.geo.GeoPoint min = new org.elasticsearch.common.geo.GeoPoint(in.readDouble(), in.readDouble());
                org.elasticsearch.common.geo.GeoPoint max = new org.elasticsearch.common.geo.GeoPoint(in.readDouble(), in.readDouble());
                return new GeoPoint(maxDoc, docCount, sumDocFreq, sumTotalTermFreq,
                    isSearchable, isAggregatable, min, max);
            }
            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    private static final String TYPE_FIELD = "type";
    private static final String MAX_DOC_FIELD = "max_doc";
    private static final String DOC_COUNT_FIELD = "doc_count";
    private static final String DENSITY_FIELD = "density";
    private static final String SUM_DOC_FREQ_FIELD = "sum_doc_freq";
    private static final String SUM_TOTAL_TERM_FREQ_FIELD = "sum_total_term_freq";
    private static final String SEARCHABLE_FIELD = "searchable";
    private static final String AGGREGATABLE_FIELD = "aggregatable";
    private static final String MIN_VALUE_FIELD = "min_value";
    private static final String MIN_VALUE_AS_STRING_FIELD = "min_value_as_string";
    private static final String MAX_VALUE_FIELD = "max_value";
    private static final String MAX_VALUE_AS_STRING_FIELD = "max_value_as_string";
}
