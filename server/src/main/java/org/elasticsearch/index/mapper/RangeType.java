/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/** Enum defining the type of range */
public enum RangeType {
    IP("ip_range", LengthType.FIXED_16) {
        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new InetAddressRange(name, (InetAddress) r.from, (InetAddress) r.to);
        }

        @Override
        public InetAddress parseFrom(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
            throws IOException {
            InetAddress address = InetAddresses.forString(parser.text());
            return included ? address : nextUp(address);
        }

        @Override
        public InetAddress parseTo(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
            throws IOException {
            InetAddress address = InetAddresses.forString(parser.text());
            return included ? address : nextDown(address);
        }

        @Override
        public InetAddress parseValue(Object value, boolean coerce, @Nullable DateMathParser dateMathParser) {
            if (value instanceof InetAddress) {
                return (InetAddress) value;
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return InetAddresses.forString(value.toString());
            }
        }

        @Override
        public Object formatValue(Object value, DateFormatter dateFormatter) {
            return InetAddresses.toAddrString((InetAddress) value);
        }

        @Override
        public InetAddress minValue() {
            return InetAddressPoint.MIN_VALUE;
        }

        @Override
        public InetAddress maxValue() {
            return InetAddressPoint.MAX_VALUE;
        }

        @Override
        public InetAddress nextUp(Object value) {
            return InetAddressPoint.nextUp((InetAddress) value);
        }

        @Override
        public InetAddress nextDown(Object value) {
            return InetAddressPoint.nextDown((InetAddress) value);
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return BinaryRangeUtil.encodeIPRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeIPRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            throw new UnsupportedOperationException("IP ranges cannot be safely converted to doubles");
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            if (includeFrom == false) {
                from = nextUp(from);
            }

            if (includeTo == false) {
                to = nextDown(to);
            }

            byte[] encodedFrom = InetAddressPoint.encode((InetAddress) from);
            byte[] encodedTo = InetAddressPoint.encode((InetAddress) to);
            return new BinaryDocValuesRangeQuery(
                field,
                queryType,
                LengthType.FIXED_16,
                new BytesRef(encodedFrom),
                new BytesRef(encodedTo),
                from,
                to
            );
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(field, from, to, includeFrom, includeTo, (f, t) -> InetAddressRange.newWithinQuery(field, f, t));
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(field, from, to, includeFrom, includeTo, (f, t) -> InetAddressRange.newContainsQuery(field, f, t));
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(field, from, to, includeFrom, includeTo, (f, t) -> InetAddressRange.newIntersectsQuery(field, f, t));
        }

        private Query createQuery(
            String field,
            Object lower,
            Object upper,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<InetAddress, InetAddress, Query> querySupplier
        ) {
            byte[] lowerBytes = InetAddressPoint.encode((InetAddress) lower);
            byte[] upperBytes = InetAddressPoint.encode((InetAddress) upper);
            if (Arrays.compareUnsigned(lowerBytes, 0, lowerBytes.length, upperBytes, 0, upperBytes.length) > 0) {
                throw new IllegalArgumentException("Range query `from` value (" + lower + ") is greater than `to` value (" + upper + ")");
            }
            InetAddress correctedFrom = includeLower ? (InetAddress) lower : nextUp(lower);
            InetAddress correctedTo = includeUpper ? (InetAddress) upper : nextDown(upper);
            ;
            lowerBytes = InetAddressPoint.encode(correctedFrom);
            upperBytes = InetAddressPoint.encode(correctedTo);
            if (Arrays.compareUnsigned(lowerBytes, 0, lowerBytes.length, upperBytes, 0, upperBytes.length) > 0) {
                return new MatchNoDocsQuery("float range didn't intersect anything");
            } else {
                return querySupplier.apply(correctedFrom, correctedTo);
            }
        }
    },
    DATE("date_range", LengthType.VARIABLE, NumberFieldMapper.NumberType.LONG) {
        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new LongRange(name, new long[] { ((Number) r.from).longValue() }, new long[] { ((Number) r.to).longValue() });
        }

        @Override
        public Number parseFrom(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
            throws IOException {
            assert fieldType.dateMathParser != null;
            Number value = fieldType.dateMathParser.parse(parser.text(), () -> {
                throw new IllegalArgumentException("now is not used at indexing time");
            }, included == false, null).toEpochMilli();
            return included ? value : nextUp(value);
        }

        @Override
        public Number parseTo(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
            throws IOException {
            assert fieldType.dateMathParser != null;
            Number value = fieldType.dateMathParser.parse(parser.text(), () -> {
                throw new IllegalArgumentException("now is not used at indexing time");
            }, included, null).toEpochMilli();
            return included ? value : nextDown(value);
        }

        @Override
        public Long parseValue(Object dateStr, boolean coerce, @Nullable DateMathParser dateMathParser) {
            assert dateMathParser != null;
            return dateMathParser.parse(
                dateStr.toString(),
                () -> { throw new IllegalArgumentException("now is not used at indexing time"); }
            ).toEpochMilli();
        }

        @Override
        public Object formatValue(Object value, DateFormatter dateFormatter) {
            long timestamp = (long) value;
            ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC);
            return dateFormatter.format(dateTime);
        }

        @Override
        public Long minValue() {
            return Long.MIN_VALUE;
        }

        @Override
        public Long maxValue() {
            return Long.MAX_VALUE;
        }

        @Override
        public Long nextUp(Object value) {
            return (long) LONG.nextUp(value);
        }

        @Override
        public Long nextDown(Object value) {
            return (long) LONG.nextDown(value);
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return LONG.encodeRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeDateRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            return LONG.doubleValue(endpointValue);
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            return LONG.dvRangeQuery(field, queryType, from, to, includeFrom, includeTo);
        }

        @Override
        public Query rangeQuery(
            String field,
            boolean hasDocValues,
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            @Nullable ZoneId timeZone,
            @Nullable DateMathParser parser,
            SearchExecutionContext context
        ) {
            ZoneId zone = (timeZone == null) ? ZoneOffset.UTC : timeZone;

            DateMathParser dateMathParser = (parser == null) ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser() : parser;
            boolean roundUp = includeLower == false; // using "gt" should round lower bound up
            Long low = lowerTerm == null
                ? minValue()
                : dateMathParser.parse(
                    lowerTerm instanceof BytesRef ? ((BytesRef) lowerTerm).utf8ToString() : lowerTerm.toString(),
                    context::nowInMillis,
                    roundUp,
                    zone
                ).toEpochMilli();

            roundUp = includeUpper; // using "lte" should round upper bound up
            Long high = upperTerm == null
                ? maxValue()
                : dateMathParser.parse(
                    upperTerm instanceof BytesRef ? ((BytesRef) upperTerm).utf8ToString() : upperTerm.toString(),
                    context::nowInMillis,
                    roundUp,
                    zone
                ).toEpochMilli();

            return createRangeQuery(field, hasDocValues, low, high, includeLower, includeUpper, relation);
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return LONG.withinQuery(field, from, to, includeLower, includeUpper);
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return LONG.containsQuery(field, from, to, includeLower, includeUpper);
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return LONG.intersectsQuery(field, from, to, includeLower, includeUpper);
        }
    },
    // todo support half_float
    FLOAT("float_range", LengthType.FIXED_4, NumberFieldMapper.NumberType.FLOAT) {
        @Override
        public Float minValue() {
            return Float.NEGATIVE_INFINITY;
        }

        @Override
        public Float maxValue() {
            return Float.POSITIVE_INFINITY;
        }

        @Override
        public Float nextUp(Object value) {
            return Math.nextUp(((Number) value).floatValue());
        }

        @Override
        public Float nextDown(Object value) {
            return Math.nextDown(((Number) value).floatValue());
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return BinaryRangeUtil.encodeFloatRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeFloatRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            assert endpointValue instanceof Float;
            return ((Float) endpointValue).doubleValue();
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            if (includeFrom == false) {
                from = nextUp(from);
            }

            if (includeTo == false) {
                to = nextDown(to);
            }

            byte[] encodedFrom = BinaryRangeUtil.encodeFloat((Float) from);
            byte[] encodedTo = BinaryRangeUtil.encodeFloat((Float) to);
            return new BinaryDocValuesRangeQuery(
                field,
                queryType,
                LengthType.FIXED_4,
                new BytesRef(encodedFrom),
                new BytesRef(encodedTo),
                from,
                to
            );
        }

        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new FloatRange(name, new float[] { ((Number) r.from).floatValue() }, new float[] { ((Number) r.to).floatValue() });
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Float) from,
                (Float) to,
                includeFrom,
                includeTo,
                (f, t) -> FloatRange.newWithinQuery(field, new float[] { f }, new float[] { t }),
                RangeType.FLOAT
            );
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Float) from,
                (Float) to,
                includeFrom,
                includeTo,
                (f, t) -> FloatRange.newContainsQuery(field, new float[] { f }, new float[] { t }),
                RangeType.FLOAT
            );
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Float) from,
                (Float) to,
                includeFrom,
                includeTo,
                (f, t) -> FloatRange.newIntersectsQuery(field, new float[] { f }, new float[] { t }),
                RangeType.FLOAT
            );
        }
    },
    DOUBLE("double_range", LengthType.FIXED_8, NumberFieldMapper.NumberType.DOUBLE) {
        @Override
        public Double minValue() {
            return Double.NEGATIVE_INFINITY;
        }

        @Override
        public Double maxValue() {
            return Double.POSITIVE_INFINITY;
        }

        @Override
        public Double nextUp(Object value) {
            return Math.nextUp(((Number) value).doubleValue());
        }

        @Override
        public Double nextDown(Object value) {
            return Math.nextDown(((Number) value).doubleValue());
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return BinaryRangeUtil.encodeDoubleRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeDoubleRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            assert endpointValue instanceof Double;
            return (Double) endpointValue;
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            if (includeFrom == false) {
                from = nextUp(from);
            }

            if (includeTo == false) {
                to = nextDown(to);
            }

            byte[] encodedFrom = BinaryRangeUtil.encodeDouble((Double) from);
            byte[] encodedTo = BinaryRangeUtil.encodeDouble((Double) to);
            return new BinaryDocValuesRangeQuery(
                field,
                queryType,
                LengthType.FIXED_8,
                new BytesRef(encodedFrom),
                new BytesRef(encodedTo),
                from,
                to
            );
        }

        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new DoubleRange(name, new double[] { ((Number) r.from).doubleValue() }, new double[] { ((Number) r.to).doubleValue() });
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Double) from,
                (Double) to,
                includeFrom,
                includeTo,
                (f, t) -> DoubleRange.newWithinQuery(field, new double[] { f }, new double[] { t }),
                RangeType.DOUBLE
            );
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Double) from,
                (Double) to,
                includeFrom,
                includeTo,
                (f, t) -> DoubleRange.newContainsQuery(field, new double[] { f }, new double[] { t }),
                RangeType.DOUBLE
            );
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Double) from,
                (Double) to,
                includeFrom,
                includeTo,
                (f, t) -> DoubleRange.newIntersectsQuery(field, new double[] { f }, new double[] { t }),
                RangeType.DOUBLE
            );
        }

    },
    // todo add BYTE support
    // todo add SHORT support
    INTEGER("integer_range", LengthType.VARIABLE, NumberFieldMapper.NumberType.INTEGER) {
        @Override
        public Integer minValue() {
            return Integer.MIN_VALUE;
        }

        @Override
        public Integer maxValue() {
            return Integer.MAX_VALUE;
        }

        @Override
        public Integer nextUp(Object value) {
            return ((Number) value).intValue() + 1;
        }

        @Override
        public Integer nextDown(Object value) {
            return ((Number) value).intValue() - 1;
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return LONG.encodeRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeIntegerRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            assert endpointValue instanceof Integer;
            return ((Integer) endpointValue).doubleValue();
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            return LONG.dvRangeQuery(field, queryType, from, to, includeFrom, includeTo);
        }

        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new IntRange(name, new int[] { ((Number) r.from).intValue() }, new int[] { ((Number) r.to).intValue() });
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Integer) from,
                (Integer) to,
                includeFrom,
                includeTo,
                (f, t) -> IntRange.newWithinQuery(field, new int[] { f }, new int[] { t }),
                RangeType.INTEGER
            );
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Integer) from,
                (Integer) to,
                includeFrom,
                includeTo,
                (f, t) -> IntRange.newContainsQuery(field, new int[] { f }, new int[] { t }),
                RangeType.INTEGER
            );
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Integer) from,
                (Integer) to,
                includeFrom,
                includeTo,
                (f, t) -> IntRange.newIntersectsQuery(field, new int[] { f }, new int[] { t }),
                RangeType.INTEGER
            );
        }
    },
    LONG("long_range", LengthType.VARIABLE, NumberFieldMapper.NumberType.LONG) {
        @Override
        public Long minValue() {
            return Long.MIN_VALUE;
        }

        @Override
        public Long maxValue() {
            return Long.MAX_VALUE;
        }

        @Override
        public Long nextUp(Object value) {
            return ((Number) value).longValue() + 1;
        }

        @Override
        public Long nextDown(Object value) {
            return ((Number) value).longValue() - 1;
        }

        @Override
        public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
            return BinaryRangeUtil.encodeLongRanges(ranges);
        }

        @Override
        public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException {
            return BinaryRangeUtil.decodeLongRanges(bytes);
        }

        @Override
        public Double doubleValue(Object endpointValue) {
            assert endpointValue instanceof Long;
            return ((Long) endpointValue).doubleValue();
        }

        @Override
        public Query dvRangeQuery(
            String field,
            BinaryDocValuesRangeQuery.QueryType queryType,
            Object from,
            Object to,
            boolean includeFrom,
            boolean includeTo
        ) {
            if (includeFrom == false) {
                from = nextUp(from);
            }

            if (includeTo == false) {
                to = nextDown(to);
            }

            byte[] encodedFrom = BinaryRangeUtil.encodeLong(((Number) from).longValue());
            byte[] encodedTo = BinaryRangeUtil.encodeLong(((Number) to).longValue());
            return new BinaryDocValuesRangeQuery(
                field,
                queryType,
                LengthType.VARIABLE,
                new BytesRef(encodedFrom),
                new BytesRef(encodedTo),
                from,
                to
            );
        }

        @Override
        public Field getRangeField(String name, RangeFieldMapper.Range r) {
            return new LongRange(name, new long[] { ((Number) r.from).longValue() }, new long[] { ((Number) r.to).longValue() });
        }

        @Override
        public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Long) from,
                (Long) to,
                includeFrom,
                includeTo,
                (f, t) -> LongRange.newWithinQuery(field, new long[] { f }, new long[] { t }),
                RangeType.LONG
            );
        }

        @Override
        public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Long) from,
                (Long) to,
                includeFrom,
                includeTo,
                (f, t) -> LongRange.newContainsQuery(field, new long[] { f }, new long[] { t }),
                RangeType.LONG
            );
        }

        @Override
        public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
            return createQuery(
                (Long) from,
                (Long) to,
                includeFrom,
                includeTo,
                (f, t) -> LongRange.newIntersectsQuery(field, new long[] { f }, new long[] { t }),
                RangeType.LONG
            );
        }
    };

    public final String name;
    private final NumberFieldMapper.NumberType numberType;
    public final LengthType lengthType;

    RangeType(String name, LengthType lengthType) {
        this.name = name;
        this.numberType = null;
        this.lengthType = lengthType;
    }

    RangeType(String name, LengthType lengthType, NumberFieldMapper.NumberType type) {
        this.name = name;
        this.numberType = type;
        this.lengthType = lengthType;
    }

    /** Get the associated type name. */
    public final String typeName() {
        return name;
    }

    /**
     * Internal helper to create the actual {@link Query} using the provided supplier function. Before creating the query we check if
     * the intervals min &gt; max, in which case an {@link IllegalArgumentException} is raised. The method adapts the interval bounds
     * based on whether the edges should be included or excluded. In case where after this correction the interval would be empty
     * because min &gt; max, we simply return a {@link MatchNoDocsQuery}.
     * This helper handles all {@link Number} cases and dates, the IP range type uses its own logic.
     */
    private static <T extends Comparable<T>> Query createQuery(
        T from,
        T to,
        boolean includeFrom,
        boolean includeTo,
        BiFunction<T, T, Query> querySupplier,
        RangeType rangeType
    ) {
        if (from.compareTo(to) > 0) {
            // wrong argument order, this is an error the user should fix
            throw new IllegalArgumentException("Range query `from` value (" + from + ") is greater than `to` value (" + to + ")");
        }

        @SuppressWarnings("unchecked")
        T correctedFrom = includeFrom ? from : (T) rangeType.nextUp(from);
        @SuppressWarnings("unchecked")
        T correctedTo = includeTo ? to : (T) rangeType.nextDown(to);
        if (correctedFrom.compareTo(correctedTo) > 0) {
            return new MatchNoDocsQuery("range didn't intersect anything");
        } else {
            return querySupplier.apply(correctedFrom, correctedTo);
        }
    }

    public abstract Field getRangeField(String name, RangeFieldMapper.Range range);

    public List<IndexableField> createFields(
        DocumentParserContext context,
        String name,
        RangeFieldMapper.Range range,
        boolean indexed,
        boolean docValued,
        boolean stored
    ) {
        assert range != null : "range cannot be null when creating fields";
        List<IndexableField> fields = new ArrayList<>();
        if (indexed) {
            fields.add(getRangeField(name, range));
        }
        if (docValued) {
            RangeFieldMapper.BinaryRangesDocValuesField field = (RangeFieldMapper.BinaryRangesDocValuesField) context.doc().getByKey(name);
            if (field == null) {
                field = new RangeFieldMapper.BinaryRangesDocValuesField(name, range, this);
                context.doc().addWithKey(name, field);
            } else {
                field.add(range);
            }
        }
        if (stored) {
            fields.add(new StoredField(name, range.toString()));
        }
        return fields;
    }

    public Object parseValue(Object value, boolean coerce, @Nullable DateMathParser dateMathParser) {
        return numberType.parse(value, coerce);
    }

    public Object formatValue(Object value, DateFormatter formatter) {
        return value;
    }

    /** parses from value. rounds according to included flag */
    public Object parseFrom(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
        throws IOException {
        Number value = numberType.parse(parser, coerce);
        return included ? value : (Number) nextUp(value);
    }

    /** parses to value. rounds according to included flag */
    public Object parseTo(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
        throws IOException {
        Number value = numberType.parse(parser, coerce);
        return included ? value : (Number) nextDown(value);
    }

    public Object defaultFrom(boolean included) {
        return included ? minValue() : nextUp(minValue());

    }

    public Object defaultTo(boolean included) {
        return included ? maxValue() : nextDown(maxValue());
    }

    public abstract Object minValue();

    public abstract Object maxValue();

    public abstract Object nextUp(Object value);

    public abstract Object nextDown(Object value);

    public abstract Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    public abstract Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    public abstract Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    public Query rangeQuery(
        String field,
        boolean hasDocValues,
        Object from,
        Object to,
        boolean includeFrom,
        boolean includeTo,
        ShapeRelation relation,
        @Nullable ZoneId timeZone,
        @Nullable DateMathParser dateMathParser,
        SearchExecutionContext context
    ) {
        Object lower = from == null ? minValue() : parseValue(from, false, dateMathParser);
        Object upper = to == null ? maxValue() : parseValue(to, false, dateMathParser);
        return createRangeQuery(field, hasDocValues, lower, upper, includeFrom, includeTo, relation);
    }

    protected final Query createRangeQuery(
        String field,
        boolean hasDocValues,
        Object lower,
        Object upper,
        boolean includeFrom,
        boolean includeTo,
        ShapeRelation relation
    ) {
        Query indexQuery;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = withinQuery(field, lower, upper, includeFrom, includeTo);
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = containsQuery(field, lower, upper, includeFrom, includeTo);
        } else {
            indexQuery = intersectsQuery(field, lower, upper, includeFrom, includeTo);
        }
        if (hasDocValues) {
            final BinaryDocValuesRangeQuery.QueryType queryType;
            if (relation == ShapeRelation.WITHIN) {
                queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
            } else if (relation == ShapeRelation.CONTAINS) {
                queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
            } else {
                queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
            }
            Query dvQuery = dvRangeQuery(field, queryType, lower, upper, includeFrom, includeTo);
            return new IndexOrDocValuesQuery(indexQuery, dvQuery);
        } else {
            return indexQuery;
        }
    }

    // No need to take into account Range#includeFrom or Range#includeTo, because from and to have already been
    // rounded up via parseFrom and parseTo methods.
    public abstract BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException;

    public abstract List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException;

    /**
     * Given the Range.to or Range.from Object value from a Range instance, converts that value into a Double.  Before converting, it
     * asserts that the object is of the expected type.  Operation is not supported on IP ranges (because of loss of precision)
     *
     * @param endpointValue Object value for Range.to or Range.from
     * @return endpointValue as a Double
     */
    public abstract Double doubleValue(Object endpointValue);

    public boolean isNumeric() {
        return numberType != null;
    }

    public abstract Query dvRangeQuery(
        String field,
        BinaryDocValuesRangeQuery.QueryType queryType,
        Object from,
        Object to,
        boolean includeFrom,
        boolean includeTo
    );

    public final Mapper.TypeParser parser() {
        return new FieldMapper.TypeParser((n, c) -> new RangeFieldMapper.Builder(n, this, c.getSettings()));
    }

    NumberFieldMapper.NumberType numberType() {
        return numberType;
    }

    public enum LengthType {
        FIXED_4 {
            @Override
            public int readLength(byte[] bytes, int offset) {
                return 4;
            }
        },
        FIXED_8 {
            @Override
            public int readLength(byte[] bytes, int offset) {
                return 8;
            }
        },
        FIXED_16 {
            @Override
            public int readLength(byte[] bytes, int offset) {
                return 16;
            }
        },
        VARIABLE {
            @Override
            public int readLength(byte[] bytes, int offset) {
                // the first bit encodes the sign and the next 4 bits encode the number
                // of additional bytes
                int token = Byte.toUnsignedInt(bytes[offset]);
                int length = (token >>> 3) & 0x0f;
                if ((token & 0x80) == 0) {
                    length = 0x0f - length;
                }
                return 1 + length;
            }
        };

        /**
         * Return the length of the value that starts at {@code offset} in {@code bytes}.
         */
        public abstract int readLength(byte[] bytes, int offset);
    }
}
