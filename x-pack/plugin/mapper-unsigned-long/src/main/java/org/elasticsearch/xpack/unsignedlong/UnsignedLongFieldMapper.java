/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.unsignedlong;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class UnsignedLongFieldMapper extends FieldMapper {
    protected static long MASK_2_63 = 0x8000000000000000L;
    private static BigInteger BIGINTEGER_2_64_MINUS_ONE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE); // 2^64 -1
    private static BigDecimal BIGDECIMAL_2_64_MINUS_ONE = new BigDecimal(BIGINTEGER_2_64_MINUS_ONE);

    public static final String CONTENT_TYPE = "unsigned_long";
    // use the same default as numbers
    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private Boolean ignoreMalformed;
        private String nullValue;

        public Builder(String name) {
            super(name, FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            throw new MapperParsingException("index_options not allowed in field [" + name + "] of type [" + CONTENT_TYPE + "]");
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return NumberFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public UnsignedLongFieldMapper build(BuilderContext context) {
            UnsignedLongFieldType type = new UnsignedLongFieldType(buildFullName(context), indexed, hasDocValues, meta);
            return new UnsignedLongFieldMapper(
                name,
                fieldType,
                type,
                ignoreMalformed(context),
                multiFieldsBuilder.build(this, context),
                copyTo,
                nullValue
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    parseUnsignedLong(propNode); // confirm that null_value is a proper unsigned_long
                    String nullValue = (propNode instanceof BytesRef) ? ((BytesRef) propNode).utf8ToString() : propNode.toString();
                    builder.nullValue(nullValue);
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class UnsignedLongFieldType extends SimpleMappedFieldType {

        public UnsignedLongFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        }

        public UnsignedLongFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexed();
            Long longValue = parseTerm(value);
            if (longValue == null) {
                return new MatchNoDocsQuery();
            }
            Query query = LongPoint.newExactQuery(name(), convertToSignedLong(longValue));
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            long[] lvalues = new long[values.size()];
            int upTo = 0;
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                Long longValue = parseTerm(value);
                if (longValue != null) {
                    lvalues[upTo++] = convertToSignedLong(longValue);
                }
            }
            if (upTo == 0) {
                return new MatchNoDocsQuery();
            }
            if (upTo != lvalues.length) {
                lvalues = Arrays.copyOf(lvalues, upTo);
            }
            Query query = LongPoint.newSetQuery(name(), lvalues);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                Long lt = parseLowerRangeTerm(lowerTerm, includeLower);
                if (lt == null) return new MatchNoDocsQuery();
                l = convertToSignedLong(lt);
            }
            if (upperTerm != null) {
                Long ut = parseUpperRangeTerm(upperTerm, includeUpper);
                if (ut == null) return new MatchNoDocsQuery();
                u = convertToSignedLong(ut);
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
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return (cache, breakerService, mapperService) -> {
                final IndexNumericFieldData signedLongValues = new SortedNumericIndexFieldData.Builder(
                    name(),
                    IndexNumericFieldData.NumericType.LONG
                ).build(cache, breakerService, mapperService);
                return new UnsignedLongIndexFieldData(signedLongValues);
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
                return (value) -> LongPoint.decodeDimension(value, 0);
            }
            return null;
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

    private Explicit<Boolean> ignoreMalformed;
    private final String nullValue;
    private final Long nullValueNumeric;

    private UnsignedLongFieldMapper(
        String simpleName,
        FieldType fieldType,
        UnsignedLongFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        MultiFields multiFields,
        CopyTo copyTo,
        String nullValue
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueNumeric = nullValue == null ? null : convertToSignedLong(parseUnsignedLong(nullValue));
        this.ignoreMalformed = ignoreMalformed;
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
    protected UnsignedLongFieldMapper clone() {
        return (UnsignedLongFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        Long numericValue;
        if (context.externalValueSet()) {
            numericValue = parseUnsignedLong(context.externalValue());
        } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
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
        if (numericValue == null) {
            numericValue = nullValueNumeric;
            if (numericValue == null) return;
        } else {
            numericValue = convertToSignedLong(numericValue);
        }

        boolean docValued = fieldType().hasDocValues();
        boolean indexed = fieldType().isSearchable();
        boolean stored = fieldType.stored();

        List<Field> fields = NumberFieldMapper.NumberType.LONG.createFields(fieldType().name(), numericValue, indexed, docValued, stored);
        context.doc().addAll(fields);
        if (docValued == false && (indexed || stored)) {
            createFieldNamesField(context);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        UnsignedLongFieldMapper mergeWith = (UnsignedLongFieldMapper) other;
        if (mergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = mergeWith.ignoreMalformed;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
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
            throw new IllegalArgumentException("For input string: [" + value.toString() + "].");
        } else {
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            try {
                return Long.parseUnsignedLong(stringValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
            }
        }
    }

    /**
     * Convert an unsigned long to the singed long by subtract 2^63 from it
     * @param value – unsigned long value in the range [0; 2^64-1], values greater than 2^63-1 are negative
     * @return signed long value in the range [-2^63; 2^63-1]
     */
    private static long convertToSignedLong(long value) {
        // subtracting 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
        // equivalent to flipping the first bit
        return value ^ MASK_2_63;
    }

    /**
     * Convert a signed long to unsigned by adding 2^63 to it
     * @param value – signed long value in the range [-2^63; 2^63-1]
     * @return unsigned long value in the range [0; 2^64-1],  values greater then 2^63-1 are negative
     */
    protected static long convertToOriginal(long value) {
        // adding 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
        // equivalent to flipping the first bit
        return value ^ MASK_2_63;
    }

}
