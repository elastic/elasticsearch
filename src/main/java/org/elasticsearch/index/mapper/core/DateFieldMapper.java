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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.search.NoCacheQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.ResolvableFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.MapperBuilders.dateField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

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
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            if (!locale.equals(dateTimeFormatter.locale())) {
                dateTimeFormatter = new FormatDateTimeFormatter(dateTimeFormatter.format(), dateTimeFormatter.parser(), dateTimeFormatter.printer(), locale);
            }
            DateFieldMapper fieldMapper = new DateFieldMapper(buildNames(context), dateTimeFormatter,
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, timeUnit, ignoreMalformed(context), coerce(context),
                    similarity, normsLoading, fieldDataSettings, context.indexSettings(),
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
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propNode));
                    iterator.remove();
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString().toUpperCase(Locale.ROOT)));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    protected FormatDateTimeFormatter dateTimeFormatter;

    private final DateMathParser dateMathParser;

    private String nullValue;

    protected final TimeUnit timeUnit;

    protected DateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                              String nullValue, TimeUnit timeUnit, Explicit<Boolean> ignoreMalformed,Explicit<Boolean> coerce,
                              SimilarityProvider similarity,
                              Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, ignoreMalformed, coerce, NumericDateAnalyzer.buildNamedAnalyzer(dateTimeFormatter, precisionStep),
                NumericDateAnalyzer.buildNamedAnalyzer(dateTimeFormatter, Integer.MAX_VALUE),
                similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.dateTimeFormatter = dateTimeFormatter;
        this.nullValue = nullValue;
        this.timeUnit = timeUnit;
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
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
        return bytesRef.get();
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

    private static Callable<Long> now() {
        return new Callable<Long>() {
            @Override
            public Long call() {
                final SearchContext context = SearchContext.current();
                return context != null
                    ? context.nowInMillis()
                    : System.currentTimeMillis();
            }
        };
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        long iValue = dateMathParser.parse(value, now());
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

    public long parseToMilliseconds(Object value) {
        return parseToMilliseconds(value, false, null, dateMathParser);
    }

    public long parseToMilliseconds(Object value, boolean inclusive, @Nullable DateTimeZone zone, @Nullable DateMathParser forcedDateParser) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return parseToMilliseconds(convertToString(value), inclusive, zone, forcedDateParser);
    }

    public long parseToMilliseconds(String value, boolean inclusive, @Nullable DateTimeZone zone, @Nullable DateMathParser forcedDateParser) {
        DateMathParser dateParser = dateMathParser;
        if (forcedDateParser != null) {
            dateParser = forcedDateParser;
        }
        return dateParser.parse(value, now(), inclusive, zone);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, null, null, context);
    }

    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser, @Nullable QueryParseContext context) {
        // If the current search context is null we're parsing percolator query or a index alias filter.
        if (SearchContext.current() == null) {
            return new LateParsingQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        } else {
            return innerRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        }
    }

    private Query innerRangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseToMilliseconds(lowerTerm, !includeLower, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                upperTerm == null ? null : parseToMilliseconds(upperTerm, includeUpper, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return rangeFilter(lowerTerm, upperTerm, includeLower, includeUpper, null, null, context);
    }

    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser, @Nullable QueryParseContext context) {
        return rangeFilter(null, lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser, context);
    }

    @Override
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return rangeFilter(parseContext, lowerTerm, upperTerm, includeLower, includeUpper, null, null, context);
    }

    /*
     * `timeZone` parameter is only applied when:
     * - not null
     * - the object to parse is a String (does not apply to ms since epoch which are UTC based time values)
     * - the String to parse does not have already a timezone defined (ie. `2014-01-01T00:00:00+03:00`)
     */
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser, @Nullable QueryParseContext context) {
        IndexNumericFieldData fieldData = parseContext != null ? (IndexNumericFieldData) parseContext.getForField(this) : null;
        // If the current search context is null we're parsing percolator query or a index alias filter.
        if (SearchContext.current() == null) {
            return new LateParsingFilter(fieldData, lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        } else {
            return innerRangeFilter(fieldData, lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        }
    }

    private Filter innerRangeFilter(IndexNumericFieldData fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
        Long lowerVal = null;
        Long upperVal = null;
        if (lowerTerm != null) {
            if (lowerTerm instanceof Number) {
                lowerVal = ((Number) lowerTerm).longValue();
            } else {
                String value = convertToString(lowerTerm);
                lowerVal = parseToMilliseconds(value, false, timeZone, forcedDateParser);
            }
        }
        if (upperTerm != null) {
            if (upperTerm instanceof Number) {
                upperVal = ((Number) upperTerm).longValue();
            } else {
                String value = convertToString(upperTerm);
                upperVal = parseToMilliseconds(value, includeUpper, timeZone, forcedDateParser);
            }
        }

        Filter filter;
        if (fieldData != null) {
            filter = NumericRangeFieldDataFilter.newLongRange(fieldData, lowerVal,upperVal, includeLower, includeUpper);
        } else {
            filter = Queries.wrap(NumericRangeQuery.newLongRange(
                    names.indexName(), precisionStep, lowerVal, upperVal, includeLower, includeUpper
            ));
        }

        return filter;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        long value = parseStringValue(nullValue);
        return Queries.wrap(NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                value,
                value,
                true, true));
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
            final long timestamp = timeUnit.toMillis(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                CustomLongNumericField field = new CustomLongNumericField(this, timestamp, fieldType);
                field.setBoost(boost);
                fields.add(field);
            }
            if (hasDocValues()) {
                addDocValue(context, fields, timestamp);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
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

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        long minValue = NumericUtils.getMinLong(terms);
        long maxValue = NumericUtils.getMaxLong(terms);
        return new FieldStats.Date(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue, dateTimeFormatter
        );
    }

    private long parseStringValue(String value) {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (RuntimeException e) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e1) {
                throw new MapperParsingException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number with locale [" + dateTimeFormatter.locale() + "]", e);
            }
        }
    }

    private final class LateParsingFilter extends ResolvableFilter {

        final IndexNumericFieldData fieldData;
        final Object lowerTerm;
        final Object upperTerm;
        final boolean includeLower;
        final boolean includeUpper;
        final DateTimeZone timeZone;
        final DateMathParser forcedDateParser;

        public LateParsingFilter(IndexNumericFieldData fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, DateTimeZone timeZone, DateMathParser forcedDateParser) {
            this.fieldData = fieldData;
            this.lowerTerm = lowerTerm;
            this.upperTerm = upperTerm;
            this.includeLower = includeLower;
            this.includeUpper = includeUpper;
            this.timeZone = timeZone;
            this.forcedDateParser = forcedDateParser;
        }

        @Override
        public Filter resolve() {
            return innerRangeFilter(fieldData, lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        }

        @Override
        public String toString(String field) {
            return "late(lower=" + lowerTerm + ",upper=" + upperTerm + ")";
        }
    }

    public final class LateParsingQuery extends NoCacheQuery {

        final Object lowerTerm;
        final Object upperTerm;
        final boolean includeLower;
        final boolean includeUpper;
        final DateTimeZone timeZone;
        final DateMathParser forcedDateParser;

        public LateParsingQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, DateTimeZone timeZone, DateMathParser forcedDateParser) {
            this.lowerTerm = lowerTerm;
            this.upperTerm = upperTerm;
            this.includeLower = includeLower;
            this.includeUpper = includeUpper;
            this.timeZone = timeZone;
            this.forcedDateParser = forcedDateParser;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            Query query = innerRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
            return query.rewrite(reader);
        }

        @Override
        public String innerToString(String s) {
            final StringBuilder sb = new StringBuilder();
            return sb.append(names.indexName()).append(':')
                    .append(includeLower ? '[' : '{')
                    .append((lowerTerm == null) ? "*" : lowerTerm.toString())
                    .append(" TO ")
                    .append((upperTerm == null) ? "*" : upperTerm.toString())
                    .append(includeUpper ? ']' : '}')
                    .append(ToStringUtils.boost(getBoost()))
                    .toString();
        }
    }
}
