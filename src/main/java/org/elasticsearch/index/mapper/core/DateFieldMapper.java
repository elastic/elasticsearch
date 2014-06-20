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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.MapperBuilders.dateField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class DateFieldMapper extends NumberFieldMapper<Long> {

    public static final String CONTENT_TYPE = "date";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime", Locale.ROOT);

        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;

        public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
        public static final boolean ROUND_CEIL = true;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, DateFieldMapper> {

        protected TimeUnit timeUnit = Defaults.TIME_UNIT;

        protected String nullValue = Defaults.NULL_VALUE;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        private Locale locale;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_64_BIT);
            builder = this;
            // do *NOT* rely on the default locale
            locale = Locale.ROOT;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            boolean roundCeil = Defaults.ROUND_CEIL;
            if (context.indexSettings() != null) {
                Settings settings = context.indexSettings();
                roundCeil =  settings.getAsBoolean("index.mapping.date.round_ceil", settings.getAsBoolean("index.mapping.date.parse_upper_inclusive", Defaults.ROUND_CEIL));
            }
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            if (!locale.equals(dateTimeFormatter.locale())) {
                dateTimeFormatter = new FormatDateTimeFormatter(dateTimeFormatter.format(), dateTimeFormatter.parser(), dateTimeFormatter.printer(), locale);
            }
            DateFieldMapper fieldMapper = new DateFieldMapper(buildNames(context), dateTimeFormatter,
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, timeUnit, roundCeil, ignoreMalformed(context), coerce(context),
                    postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        public Builder locale(Locale locale) {
            this.locale = locale;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DateFieldMapper.Builder builder = dateField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString().toUpperCase(Locale.ROOT)));
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                }
            }
            return builder;
        }
    }

    protected FormatDateTimeFormatter dateTimeFormatter;

    // Triggers rounding up of the upper bound for range queries and filters if
    // set to true.
    // Rounding up a date here has the following meaning: If a date is not
    // defined with full precision, for example, no milliseconds given, the date
    // will be filled up to the next larger date with that precision.
    // Example: An upper bound given as "2000-01-01", will be converted to
    // "2000-01-01T23.59.59.999"
    private final boolean roundCeil;

    private final DateMathParser dateMathParser;

    private String nullValue;

    protected final TimeUnit timeUnit;

    protected DateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                              String nullValue, TimeUnit timeUnit, boolean roundCeil, Explicit<Boolean> ignoreMalformed,Explicit<Boolean> coerce,
                              PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity,

                              Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, ignoreMalformed, coerce, new NamedAnalyzer("_date/" + precisionStep,
                new NumericDateAnalyzer(precisionStep, dateTimeFormatter.parser())),
                new NamedAnalyzer("_date/max", new NumericDateAnalyzer(Integer.MAX_VALUE, dateTimeFormatter.parser())),
                postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.dateTimeFormatter = dateTimeFormatter;
        this.nullValue = nullValue;
        this.timeUnit = timeUnit;
        this.roundCeil = roundCeil;
        this.dateMathParser = new DateMathParser(dateTimeFormatter, timeUnit);
    }

    public FormatDateTimeFormatter dateTimeFormatter() {
        return dateTimeFormatter;
    }

    public DateMathParser dateMathParser() {
        return dateMathParser;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected int maxPrecisionStep() {
        return 64;
    }

    @Override
    public Long value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToLong((BytesRef) value);
        }
        return parseStringValue(value.toString());
    }

    /** Dates should return as a string. */
    @Override
    public Object valueForSearch(Object value) {
        if (value instanceof String) {
            // assume its the string that was indexed, just return it... (for example, with get)
            return value;
        }
        Long val = value(value);
        if (val == null) {
            return null;
        }
        return dateTimeFormatter.printer().print(val);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        BytesRef bytesRef = new BytesRef();
        NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
        return bytesRef;
    }

    private long parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return dateTimeFormatter.parser().parseMillis(((BytesRef) value).utf8ToString());
        }
        return dateTimeFormatter.parser().parseMillis(value.toString());
    }

    private String convertToString(Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        long iValue = dateMathParser.parse(value, System.currentTimeMillis());
        long iSim;
        try {
            iSim = fuzziness.asTimeValue().millis();
        } catch (Exception e) {
            // not a time format
            iSim =  fuzziness.asLong();
        }
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        long lValue = parseToMilliseconds(value, context);
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lValue, lValue, true, true);
    }

    public long parseToMilliseconds(Object value, @Nullable QueryParseContext context) {
        return parseToMilliseconds(value, context, false);
    }

    public long parseToMilliseconds(Object value, @Nullable QueryParseContext context, boolean includeUpper) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        return includeUpper && roundCeil ? dateMathParser.parseRoundCeil(convertToString(value), now) : dateMathParser.parse(convertToString(value), now);
    }

    public long parseToMilliseconds(String value, @Nullable QueryParseContext context, boolean includeUpper) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        return includeUpper && roundCeil ? dateMathParser.parseRoundCeil(value, now) : dateMathParser.parse(value, now);
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        final long lValue = parseToMilliseconds(value, context);
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                lValue, lValue, true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseToMilliseconds(lowerTerm, context),
                upperTerm == null ? null : parseToMilliseconds(upperTerm, context, includeUpper),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return rangeFilter(lowerTerm, upperTerm, includeLower, includeUpper, context, false);
    }

    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context, boolean explicitCaching) {
        boolean cache = explicitCaching;
        Long lowerVal = null;
        Long upperVal = null;
        if (lowerTerm != null) {
            if (lowerTerm instanceof Number) {
                lowerVal = ((Number) lowerTerm).longValue();
            } else {
                String value = convertToString(lowerTerm);
                cache = explicitCaching || !hasNowExpressionWithNoRounding(value);
                lowerVal = parseToMilliseconds(value, context, false);
            }
        }
        if (upperTerm != null) {
            if (upperTerm instanceof Number) {
                upperVal = ((Number) upperTerm).longValue();
            } else {
                String value = convertToString(upperTerm);
                cache = explicitCaching || !hasNowExpressionWithNoRounding(value);
                upperVal = parseToMilliseconds(value, context, includeUpper);
            }
        }

        Filter filter =  NumericRangeFilter.newLongRange(
            names.indexName(), precisionStep, lowerVal, upperVal, includeLower, includeUpper
        );
        if (!cache) {
            // We don't cache range filter if `now` date expression is used and also when a compound filter wraps
            // a range filter with a `now` date expressions.
            return NoCacheFilter.wrap(filter);
        } else {
            return filter;
        }
    }

    @Override
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return rangeFilter(parseContext, lowerTerm, upperTerm, includeLower, includeUpper, context, false);
    }

    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context, boolean explicitCaching) {
        boolean cache = explicitCaching;
        Long lowerVal = null;
        Long upperVal = null;
        if (lowerTerm != null) {
            if (lowerTerm instanceof Number) {
                lowerVal = ((Number) lowerTerm).longValue();
            } else {
                String value = convertToString(lowerTerm);
                cache = explicitCaching || !hasNowExpressionWithNoRounding(value);
                lowerVal = parseToMilliseconds(value, context, false);
            }
        }
        if (upperTerm != null) {
            if (upperTerm instanceof Number) {
                upperVal = ((Number) upperTerm).longValue();
            } else {
                String value = convertToString(upperTerm);
                cache = explicitCaching || !hasNowExpressionWithNoRounding(value);
                upperVal = parseToMilliseconds(value, context, includeUpper);
            }
        }

        Filter filter =  NumericRangeFieldDataFilter.newLongRange(
            (IndexNumericFieldData<?>) parseContext.getForField(this), lowerVal,upperVal, includeLower, includeUpper
        );
        if (!cache) {
            // We don't cache range filter if `now` date expression is used and also when a compound filter wraps
            // a range filter with a `now` date expressions.
            return NoCacheFilter.wrap(filter);
        } else {
            return filter;
        }
    }

    private boolean hasNowExpressionWithNoRounding(String value) {
        int index = value.indexOf("now");
        if (index != -1) {
            if (value.length() == 3) {
                return true;
            } else {
                int indexOfPotentialRounding = index + 3;
                if (indexOfPotentialRounding >= value.length()) {
                    return true;
                } else {
                    char potentialRoundingChar;
                    do {
                        potentialRoundingChar = value.charAt(indexOfPotentialRounding++);
                        if (potentialRoundingChar == '/') {
                            return false; // We found the rounding char, so we shouldn't forcefully disable caching
                        } else if (potentialRoundingChar == ' ') {
                            return true; // Next token in the date math expression and no rounding found, so we should not cache.
                        }
                    } while (indexOfPotentialRounding < value.length());
                    return true; // Couldn't find rounding char, so we should not cache
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        long value = parseStringValue(nullValue);
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                value,
                value,
                true, true);
    }


    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String dateAsString = null;
        Long value = null;
        float boost = this.boost;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue instanceof Number) {
                value = ((Number) externalValue).longValue();
            } else {
                dateAsString = (String) externalValue;
                if (dateAsString == null) {
                    dateAsString = nullValue;
                }
            }
        } else {
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                dateAsString = nullValue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.longValue(coerce.value());
            } else if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                dateAsString = nullValue;
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                value = parser.longValue(coerce.value());
                            } else {
                                dateAsString = parser.text();
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticsearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                dateAsString = parser.text();
            }
        }

        if (dateAsString != null) {
            assert value == null;
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), dateAsString, boost);
            }
            value = parseStringValue(dateAsString);
        }

        if (value != null) {
            if (fieldType.indexed() || fieldType.stored()) {
                CustomLongNumericField field = new CustomLongNumericField(this, value, fieldType);
                field.setBoost(boost);
                fields.add(field);
            }
            if (hasDocValues()) {
                addDocValue(context, value);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.nullValue = ((DateFieldMapper) mergeWith).nullValue;
            this.dateTimeFormatter = ((DateFieldMapper) mergeWith).dateTimeFormatter;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", precisionStep);
        }
        builder.field("format", dateTimeFormatter.format());
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

        if (includeDefaults || timeUnit != Defaults.TIME_UNIT) {
            builder.field("numeric_resolution", timeUnit.name().toLowerCase(Locale.ROOT));
        }
        // only serialize locale if needed, ROOT is the default, so no need to serialize that case as well...
        if (dateTimeFormatter.locale() != null && dateTimeFormatter.locale() != Locale.ROOT) {
            builder.field("locale", dateTimeFormatter.locale());
        } else if (includeDefaults) {
            if (dateTimeFormatter.locale() == null) {
                builder.field("locale", Locale.ROOT);
            } else {
                builder.field("locale", dateTimeFormatter.locale());
            }
        }
    }

    private long parseStringValue(String value) {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (RuntimeException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new MapperParsingException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number with locale [" + dateTimeFormatter.locale() + "]", e);
            }
        }
    }
}
