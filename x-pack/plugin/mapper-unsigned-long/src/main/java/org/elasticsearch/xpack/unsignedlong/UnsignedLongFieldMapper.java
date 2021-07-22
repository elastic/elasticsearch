/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongLeafFieldData.convertUnsignedLongToDouble;

public class UnsignedLongFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "unsigned_long";

    private static final long MASK_2_63 = 0x8000000000000000L;
    static final BigInteger BIGINTEGER_2_64_MINUS_ONE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE); // 2^64 -1
    private static final BigDecimal BIGDECIMAL_2_64_MINUS_ONE = new BigDecimal(BIGINTEGER_2_64_MINUS_ONE);

    private static UnsignedLongFieldMapper toType(FieldMapper in) {
        return (UnsignedLongFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<String> nullValue;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, Settings settings) {
            this(name, IGNORE_MALFORMED_SETTING.get(settings));
        }

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.nullValue = new Parameter<>(
                "null_value",
                false,
                () -> null,
                (n, c, o) -> parseNullValueAsString(o),
                m -> toType(m).nullValue
            ).acceptsNull();
        }

        private String parseNullValueAsString(Object o) {
            if (o == null) return null;
            try {
                parseUnsignedLong(o); // confirm that null_value is a proper unsigned_long
                return (o instanceof BytesRef) ? ((BytesRef) o).utf8ToString() : o.toString();
            } catch (Exception e) {
                throw new MapperParsingException("Error parsing [null_value] on field [" + name() + "]: " + e.getMessage(), e);
            }
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(indexed, hasDocValues, stored, ignoreMalformed, nullValue, meta);
        }

        Number parsedNullValue() {
            if (nullValue.getValue() == null) {
                return null;
            }
            long parsed = parseUnsignedLong(nullValue.getValue());
            return parsed >= 0 ? parsed : BigInteger.valueOf(parsed).and(BIGINTEGER_2_64_MINUS_ONE);
        }

        @Override
        public UnsignedLongFieldMapper build(ContentPath contentPath) {
            UnsignedLongFieldType fieldType = new UnsignedLongFieldType(
                buildFullName(contentPath),
                indexed.getValue(),
                stored.getValue(),
                hasDocValues.getValue(),
                parsedNullValue(),
                meta.getValue()
            );
            return new UnsignedLongFieldMapper(name, fieldType, multiFieldsBuilder.build(this, contentPath), copyTo.build(), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getSettings()));

    public static final class UnsignedLongFieldType extends SimpleMappedFieldType {

        private final Number nullValueFormatted;

        public UnsignedLongFieldType(
            String name,
            boolean indexed,
            boolean isStored,
            boolean hasDocValues,
            Number nullValueFormatted,
            Map<String, String> meta
        ) {
            super(name, indexed, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.nullValueFormatted = nullValueFormatted;
        }

        public UnsignedLongFieldType(String name) {
            this(name, true, false, true, null, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexed();
            Long longValue = parseTerm(value);
            if (longValue == null) {
                return new MatchNoDocsQuery();
            }
            return LongPoint.newExactQuery(name(), unsignedToSortableSignedLong(longValue));
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            long[] lvalues = new long[values.size()];
            int upTo = 0;
            for (Object value : values) {
                Long longValue = parseTerm(value);
                if (longValue != null) {
                    lvalues[upTo++] = unsignedToSortableSignedLong(longValue);
                }
            }
            if (upTo == 0) {
                return new MatchNoDocsQuery();
            }
            if (upTo != lvalues.length) {
                lvalues = Arrays.copyOf(lvalues, upTo);
            }
            return LongPoint.newSetQuery(name(), lvalues);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            failIfNotIndexed();
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                Long lt = parseLowerRangeTerm(lowerTerm, includeLower);
                if (lt == null) return new MatchNoDocsQuery();
                l = unsignedToSortableSignedLong(lt);
            }
            if (upperTerm != null) {
                Long ut = parseUpperRangeTerm(upperTerm, includeUpper);
                if (ut == null) return new MatchNoDocsQuery();
                u = unsignedToSortableSignedLong(ut);
            }
            if (l > u) return new MatchNoDocsQuery();

            Query query = LongPoint.newRangeQuery(name(), l, u);
            if (hasDocValues()) {
                Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
                query = new IndexOrDocValuesQuery(query, dvQuery);
                if (context.indexSortedOnField(name())) {
                    query = new IndexSortSortedNumericDocValuesRangeQuery(name(), l, u, query);
                }
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return (cache, breakerService) -> {
                final IndexNumericFieldData signedLongValues = new SortedNumericIndexFieldData.Builder(
                    name(),
                    IndexNumericFieldData.NumericType.LONG
                ).build(cache, breakerService);
                return new UnsignedLongIndexFieldData(signedLongValues);
            };
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValueFormatted) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if (value.equals("")) {
                        return nullValueFormatted;
                    }
                    long ulValue = parseUnsignedLong(value);
                    if (ulValue >= 0) {
                        return ulValue;
                    } else {
                        return BigInteger.valueOf(ulValue).and(BIGINTEGER_2_64_MINUS_ONE);
                    }
                }
            };
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return value;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            return DocValueFormat.UNSIGNED_LONG_SHIFTED;
        }

        @Override
        public Function<byte[], Number> pointReaderIfPossible() {
            if (isSearchable()) {
                // convert from the shifted value back to the original value
                return (value) -> convertUnsignedLongToDouble(LongPoint.decodeDimension(value, 0));
            }
            return null;
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.NUMERIC;
        }

        /**
         * Parses value to unsigned long for Term Query
         * @param value to to parse
         * @return parsed value, if a value represents an unsigned long in the range [0, 18446744073709551615]
         *         null, if a value represents some other number
         *         throws an exception if a value is wrongly formatted number
         */
        protected static Long parseTerm(Object value) {
            if (value instanceof Number) {
                if ((value instanceof Long) || (value instanceof Integer) || (value instanceof Short) || (value instanceof Byte)) {
                    long lv = ((Number) value).longValue();
                    if (lv >= 0) {
                        return lv;
                    }
                } else if (value instanceof BigInteger) {
                    BigInteger bigIntegerValue = (BigInteger) value;
                    if (bigIntegerValue.compareTo(BigInteger.ZERO) >= 0 && bigIntegerValue.compareTo(BIGINTEGER_2_64_MINUS_ONE) <= 0) {
                        return bigIntegerValue.longValue();
                    }
                }
            } else {
                String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
                try {
                    return Long.parseUnsignedLong(stringValue);
                } catch (NumberFormatException e) {
                    // try again in case a number was negative or contained decimal
                    Double.parseDouble(stringValue); // throws an exception if it is an improper number
                }
            }
            return null; // any other number: decimal or beyond the range of unsigned long
        }

        /**
         * Parses a lower term for a range query
         * @param value to parse
         * @param include whether a value should be included
         * @return parsed value to long considering include parameter
         *      0, if value is less than 0
         *      a value truncated to long, if value is in range [0, 18446744073709551615]
         *      null, if value is higher than the maximum allowed value for unsigned long
         *      throws an exception is value represents wrongly formatted number
         */
        protected static Long parseLowerRangeTerm(Object value, boolean include) {
            if ((value instanceof Long) || (value instanceof Integer) || (value instanceof Short) || (value instanceof Byte)) {
                long longValue = ((Number) value).longValue();
                if (longValue < 0) return 0L; // limit lowerTerm to min value for unsigned long: 0
                if (include == false) { // start from the next value
                    // for unsigned long, the next value for Long.MAX_VALUE is -9223372036854775808L
                    longValue = longValue == Long.MAX_VALUE ? Long.MIN_VALUE : ++longValue;
                }
                return longValue;
            }
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            final BigDecimal bigDecimalValue = new BigDecimal(stringValue);  // throws an exception if it is an improper number
            if (bigDecimalValue.compareTo(BigDecimal.ZERO) <= 0) {
                return 0L; // for values <=0, set lowerTerm to 0
            }
            int c = bigDecimalValue.compareTo(BIGDECIMAL_2_64_MINUS_ONE);
            if (c > 0 || (c == 0 && include == false)) {
                return null; // lowerTerm is beyond maximum value
            }
            long longValue = bigDecimalValue.longValue();
            boolean hasDecimal = (bigDecimalValue.scale() > 0 && bigDecimalValue.stripTrailingZeros().scale() > 0);
            if (include == false || hasDecimal) {
                ++longValue;
            }
            return longValue;
        }

        /**
         * Parses an upper term for a range query
         * @param value to parse
         * @param include whether a value should be included
         * @return parsed value to long considering include parameter
         *      null, if value is less that 0, as value is lower than the minimum allowed value for unsigned long
         *      a value truncated to long if value is in range [0, 18446744073709551615]
         *      -1 (unsigned long of 18446744073709551615) for values greater than 18446744073709551615
         *      throws an exception is value represents wrongly formatted number
         */
        protected static Long parseUpperRangeTerm(Object value, boolean include) {
            if ((value instanceof Long) || (value instanceof Integer) || (value instanceof Short) || (value instanceof Byte)) {
                long longValue = ((Number) value).longValue();
                if ((longValue < 0) || (longValue == 0 && include == false)) return null; // upperTerm is below minimum
                longValue = include ? longValue : --longValue;
                return longValue;
            }
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            final BigDecimal bigDecimalValue = new BigDecimal(stringValue);  // throws an exception if it is an improper number
            int c = bigDecimalValue.compareTo(BigDecimal.ZERO);
            if (c < 0 || (c == 0 && include == false)) {
                return null; // upperTerm is below minimum
            }
            if (bigDecimalValue.compareTo(BIGDECIMAL_2_64_MINUS_ONE) > 0) {
                return -1L; // limit upperTerm to max value for unsigned long: 18446744073709551615
            }
            long longValue = bigDecimalValue.longValue();
            boolean hasDecimal = (bigDecimalValue.scale() > 0 && bigDecimalValue.stripTrailingZeros().scale() > 0);
            if (include == false && hasDecimal == false) {
                --longValue;
            }
            return longValue;
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;
    private final String nullValue;
    private final Long nullValueIndexed; // null value to use for indexing, represented as shifted to signed long range

    private UnsignedLongFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.nullValue = builder.nullValue.getValue();
        if (nullValue == null) {
            this.nullValueIndexed = null;
        } else {
            long parsed = parseUnsignedLong(nullValue);
            this.nullValueIndexed = unsignedToSortableSignedLong(parsed);
        }
    }

    boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    public UnsignedLongFieldType fieldType() {
        return (UnsignedLongFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        Long numericValue;
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            numericValue = null;
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0) {
            numericValue = null;
        } else {
            try {
                if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    numericValue = parseUnsignedLong(parser.numberValue());
                } else {
                    numericValue = parseUnsignedLong(parser.text());
                }
            } catch (InputCoercionException | IllegalArgumentException | JsonParseException e) {
                if (ignoreMalformed.value() && parser.currentToken().isValue()) {
                    context.addIgnoredField(mappedFieldType.name());
                    return;
                } else {
                    throw e;
                }
            }
        }
        boolean isNullValue = false;
        if (numericValue == null) {
            numericValue = nullValueIndexed;
            if (numericValue == null) return;
            isNullValue = true;
        } else {
            numericValue = unsignedToSortableSignedLong(numericValue);
        }

        List<Field> fields = new ArrayList<>();
        if (indexed) {
            fields.add(new LongPoint(fieldType().name(), numericValue));
        }
        if (hasDocValues) {
            fields.add(new SortedNumericDocValuesField(fieldType().name(), numericValue));
        }
        if (stored) {
            // for stored field, keeping original unsigned_long value in the String form
            String storedValued = isNullValue ? nullValue : Long.toUnsignedString(unsignedToSortableSignedLong(numericValue));
            fields.add(new StoredField(fieldType().name(), storedValued));
        }
        context.doc().addAll(fields);
        if (hasDocValues == false && (stored || indexed)) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), ignoreMalformedByDefault).init(this);
    }

    /**
     * Parse object to unsigned long
     * @param value must represent an unsigned long in rage [0;18446744073709551615] or an exception will be thrown
     */
    private static long parseUnsignedLong(Object value) {
        if (value instanceof Number) {
            if ((value instanceof Long) || (value instanceof Integer) || (value instanceof Short) || (value instanceof Byte)) {
                long lv = ((Number) value).longValue();
                if (lv < 0) {
                    throw new IllegalArgumentException("Value [" + lv + "] is out of range for unsigned long.");
                }
                return lv;
            } else if (value instanceof BigInteger) {
                BigInteger bigIntegerValue = (BigInteger) value;
                if (bigIntegerValue.compareTo(BIGINTEGER_2_64_MINUS_ONE) > 0 || bigIntegerValue.compareTo(BigInteger.ZERO) < 0) {
                    throw new IllegalArgumentException("Value [" + bigIntegerValue + "] is out of range for unsigned long");
                }
                return bigIntegerValue.longValue();
            }
            // throw exception for all other numeric types with decimal parts
            throw new IllegalArgumentException("For input string: [" + value + "].");
        } else {
            final String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            try {
                return Long.parseUnsignedLong(stringValue);
            } catch (NumberFormatException ignored) {
                final BigInteger bigInteger;
                try {
                    final BigDecimal bigDecimal = new BigDecimal(stringValue);
                    bigInteger = bigDecimal.toBigIntegerExact();
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("Value \"" + stringValue + "\" has a decimal part");
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
                }
                return parseUnsignedLong(bigInteger);
            }
        }
    }

    /**
     * Convert an unsigned long to the signed long by subtract 2^63 from it
     * @param value – unsigned long value in the range [0; 2^64-1], values greater than 2^63-1 are negative
     * @return signed long value in the range [-2^63; 2^63-1]
     */
    private static long unsignedToSortableSignedLong(long value) {
        // subtracting 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
        // equivalent to flipping the first bit
        return value ^ MASK_2_63;
    }

    /**
     * Convert a signed long to unsigned by adding 2^63 to it
     * @param value – signed long value in the range [-2^63; 2^63-1]
     * @return unsigned long value in the range [0; 2^64-1],  values greater then 2^63-1 are negative
     */
    protected static long sortableSignedLongToUnsigned(long value) {
        // adding 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
        // equivalent to flipping the first bit
        return value ^ MASK_2_63;
    }

}
