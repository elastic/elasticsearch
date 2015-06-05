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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.query.QueryParseContext;
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

public class DateFieldMapper extends NumberFieldMapper {

    public static final String CONTENT_TYPE = "date";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime", Locale.ROOT);
        public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
        public static final DateFieldType FIELD_TYPE = new DateFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, DateFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        private Locale locale;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
            // do *NOT* rely on the default locale
            locale = Locale.ROOT;
        }

        DateFieldType fieldType() {
            return (DateFieldType)fieldType;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            fieldType().setTimeUnit(timeUnit);
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            fieldType().setDateTimeFormatter(dateTimeFormatter);
            return this;
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            DateFieldMapper fieldMapper = new DateFieldMapper(fieldType,
                    docValues, nullValue, ignoreMalformed(context), coerce(context),
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        protected void setupFieldType(BuilderContext context) {
            FormatDateTimeFormatter dateTimeFormatter = fieldType().dateTimeFormatter;
            if (!locale.equals(dateTimeFormatter.locale())) {
                fieldType().setDateTimeFormatter(new FormatDateTimeFormatter(dateTimeFormatter.format(), dateTimeFormatter.parser(), dateTimeFormatter.printer(), locale));
            }
            super.setupFieldType(context);
        }

        public Builder locale(Locale locale) {
            this.locale = locale;
            return this;
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            return NumericDateAnalyzer.buildNamedAnalyzer(fieldType().dateTimeFormatter, precisionStep);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
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

    public static class DateFieldType extends NumberFieldType {

        final class LateParsingQuery extends Query {

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
            public String toString(String s) {
                final StringBuilder sb = new StringBuilder();
                return sb.append(names().indexName()).append(':')
                    .append(includeLower ? '[' : '{')
                    .append((lowerTerm == null) ? "*" : lowerTerm.toString())
                    .append(" TO ")
                    .append((upperTerm == null) ? "*" : upperTerm.toString())
                    .append(includeUpper ? ']' : '}')
                    .append(ToStringUtils.boost(getBoost()))
                    .toString();
            }
        }

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;
        protected TimeUnit timeUnit = Defaults.TIME_UNIT;
        protected DateMathParser dateMathParser = new DateMathParser(dateTimeFormatter);

        public DateFieldType() {}

        protected DateFieldType(DateFieldType ref) {
            super(ref);
            this.dateTimeFormatter = ref.dateTimeFormatter;
            this.timeUnit = ref.timeUnit;
            this.dateMathParser = ref.dateMathParser;
        }

        public DateFieldType clone() {
            return new DateFieldType(this);
        }

        public FormatDateTimeFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        public void setDateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            checkIfFrozen();
            this.dateTimeFormatter = dateTimeFormatter;
            this.dateMathParser = new DateMathParser(dateTimeFormatter);
        }

        public TimeUnit timeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(TimeUnit timeUnit) {
            checkIfFrozen();
            this.timeUnit = timeUnit;
            this.dateMathParser = new DateMathParser(dateTimeFormatter);
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        private long parseValue(Object value) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                return dateTimeFormatter().parser().parseMillis(((BytesRef) value).utf8ToString());
            }
            return dateTimeFormatter().parser().parseMillis(value.toString());
        }

        protected long parseStringValue(String value) {
            try {
                return dateTimeFormatter().parser().parseMillis(value);
            } catch (RuntimeException e) {
                try {
                    return timeUnit().toMillis(Long.parseLong(value));
                } catch (NumberFormatException e1) {
                    throw new MapperParsingException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter().format() + "], and timestamp number with locale [" + dateTimeFormatter().locale() + "]", e);
                }
            }
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

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

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
            return dateTimeFormatter().printer().print(val);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
            return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, null, null, context);
        }

        @Override
        public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            long iValue = dateMathParser().parse(value, now());
            long iSim;
            try {
                iSim = fuzziness.asTimeValue().millis();
            } catch (Exception e) {
                // not a time format
                iSim =  fuzziness.asLong();
            }
            return NumericRangeQuery.newLongRange(names().indexName(), numericPrecisionStep(),
                iValue - iSim,
                iValue + iSim,
                true, true);
        }

        @Override
        public FieldStats stats(Terms terms, int maxDoc) throws IOException {
            long minValue = NumericUtils.getMinLong(terms);
            long maxValue = NumericUtils.getMaxLong(terms);
            return new FieldStats.Date(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue, dateTimeFormatter()
            );
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
            return NumericRangeQuery.newLongRange(names().indexName(), numericPrecisionStep(),
                lowerTerm == null ? null : parseToMilliseconds(lowerTerm, !includeLower, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                upperTerm == null ? null : parseToMilliseconds(upperTerm, includeUpper, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                includeLower, includeUpper);
        }

        public long parseToMilliseconds(Object value, boolean inclusive, @Nullable DateTimeZone zone, @Nullable DateMathParser forcedDateParser) {
            DateMathParser dateParser = dateMathParser();
            if (forcedDateParser != null) {
                dateParser = forcedDateParser;
            }
            String strValue;
            if (value instanceof BytesRef) {
                strValue = ((BytesRef) value).utf8ToString();
            } else {
                strValue = value.toString();
            }
            return dateParser.parse(strValue, now(), inclusive, zone);
        }
    }

    private String nullValue;

    protected DateFieldMapper(MappedFieldType fieldType, Boolean docValues, String nullValue, Explicit<Boolean> ignoreMalformed,Explicit<Boolean> coerce,
                              @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(fieldType, docValues, ignoreMalformed, coerce, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
    }

    @Override
    public DateFieldType fieldType() {
        return (DateFieldType)fieldType;
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
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
    public Query nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return new ConstantScoreQuery(termQuery(nullValue, null));
    }


    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String dateAsString = null;
        float boost = this.fieldType.boost();
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            dateAsString = (String) externalValue;
            if (dateAsString == null) {
                dateAsString = nullValue;
            }
        } else {
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                dateAsString = nullValue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                dateAsString = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                dateAsString = nullValue;
                            } else {
                                dateAsString = parser.text();
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new IllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                dateAsString = parser.text();
            }
        }

        Long value = null;
        if (dateAsString != null) {
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(fieldType.names().fullName(), dateAsString, boost);
            }
            value = fieldType().parseStringValue(dateAsString);
        }

        if (value != null) {
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                CustomLongNumericField field = new CustomLongNumericField(this, value, (NumberFieldType)fieldType);
                field.setBoost(boost);
                fields.add(field);
            }
            if (fieldType().hasDocValues()) {
                addDocValue(context, fields, value);
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
            this.fieldType = this.fieldType.clone();
            fieldType().setDateTimeFormatter(((DateFieldMapper) mergeWith).fieldType().dateTimeFormatter());
            this.fieldType.freeze();
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType.numericPrecisionStep() != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", fieldType.numericPrecisionStep());
        }
        builder.field("format", fieldType().dateTimeFormatter().format());
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

        if (includeDefaults || fieldType().timeUnit() != Defaults.TIME_UNIT) {
            builder.field("numeric_resolution", fieldType().timeUnit().name().toLowerCase(Locale.ROOT));
        }
        // only serialize locale if needed, ROOT is the default, so no need to serialize that case as well...
        if (fieldType().dateTimeFormatter().locale() != null && fieldType().dateTimeFormatter().locale() != Locale.ROOT) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        } else if (includeDefaults) {
            if (fieldType().dateTimeFormatter().locale() == null) {
                builder.field("locale", Locale.ROOT);
            } else {
                builder.field("locale", fieldType().dateTimeFormatter().locale());
            }
        }
    }
}
