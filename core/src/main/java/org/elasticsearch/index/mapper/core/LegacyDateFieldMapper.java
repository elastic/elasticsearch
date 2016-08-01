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
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyLongFieldMapper.CustomLongNumericField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

public class LegacyDateFieldMapper extends LegacyNumberFieldMapper {

    public static final String CONTENT_TYPE = "date";

    public static class Defaults extends LegacyNumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("strict_date_optional_time||epoch_millis", Locale.ROOT);
        public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
        public static final DateFieldType FIELD_TYPE = new DateFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends LegacyNumberFieldMapper.Builder<Builder, LegacyDateFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        private Locale locale;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
            // do *NOT* rely on the default locale
            locale = Locale.ROOT;
        }

        @Override
        public DateFieldType fieldType() {
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
        public LegacyDateFieldMapper build(BuilderContext context) {
            if (context.indexCreatedVersion().onOrAfter(Version.V_5_0_0_alpha2)) {
                throw new IllegalStateException("Cannot use legacy numeric types after 5.0");
            }
            setupFieldType(context);
            fieldType.setNullValue(nullValue);
            LegacyDateFieldMapper fieldMapper = new LegacyDateFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (LegacyDateFieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
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
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            LegacyDateFieldMapper.Builder builder = new LegacyDateFieldMapper.Builder(name);
            parseNumberField(builder, name, node, parserContext);
            boolean configuredFormat = false;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propNode));
                    configuredFormat = true;
                    iterator.remove();
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString().toUpperCase(Locale.ROOT)));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                    iterator.remove();
                }
            }
            if (!configuredFormat) {
                builder.dateTimeFormatter(Defaults.DATE_TIME_FORMATTER);
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
                Query rewritten = super.rewrite(reader);
                if (rewritten != this) {
                    return rewritten;
                }
                return innerRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
            }

            // Even though we only cache rewritten queries it is good to let all queries implement hashCode() and equals():
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (sameClassAs(o) == false) return false;

                LateParsingQuery that = (LateParsingQuery) o;
                if (includeLower != that.includeLower) return false;
                if (includeUpper != that.includeUpper) return false;
                if (lowerTerm != null ? !lowerTerm.equals(that.lowerTerm) : that.lowerTerm != null) return false;
                if (upperTerm != null ? !upperTerm.equals(that.upperTerm) : that.upperTerm != null) return false;
                if (timeZone != null ? !timeZone.equals(that.timeZone) : that.timeZone != null) return false;

                return true;
            }

            @Override
            public int hashCode() {
                return Objects.hash(classHash(), lowerTerm, upperTerm, includeLower, includeUpper, timeZone);
            }

            @Override
            public String toString(String s) {
                final StringBuilder sb = new StringBuilder();
                return sb.append(name()).append(':')
                    .append(includeLower ? '[' : '{')
                    .append((lowerTerm == null) ? "*" : lowerTerm.toString())
                    .append(" TO ")
                    .append((upperTerm == null) ? "*" : upperTerm.toString())
                    .append(includeUpper ? ']' : '}')
                    .toString();
            }
        }

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;
        protected TimeUnit timeUnit = Defaults.TIME_UNIT;
        protected DateMathParser dateMathParser = new DateMathParser(dateTimeFormatter);

        public DateFieldType() {
            super(LegacyNumericType.LONG);
        }

        protected DateFieldType(DateFieldType ref) {
            super(ref);
            this.dateTimeFormatter = ref.dateTimeFormatter;
            this.timeUnit = ref.timeUnit;
            this.dateMathParser = ref.dateMathParser;
        }

        @Override
        public DateFieldType clone() {
            return new DateFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            DateFieldType that = (DateFieldType) o;
            return Objects.equals(dateTimeFormatter.format(), that.dateTimeFormatter.format()) &&
                   Objects.equals(dateTimeFormatter.locale(), that.dateTimeFormatter.locale()) &&
                   Objects.equals(timeUnit, that.timeUnit);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dateTimeFormatter.format(), timeUnit);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            if (strict) {
                DateFieldType other = (DateFieldType)fieldType;
                if (Objects.equals(dateTimeFormatter().format(), other.dateTimeFormatter().format()) == false) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [format] across all types.");
                }
                if (Objects.equals(dateTimeFormatter().locale(), other.dateTimeFormatter().locale()) == false) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [locale] across all types.");
                }
                if (Objects.equals(timeUnit(), other.timeUnit()) == false) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [numeric_resolution] across all types.");
                }
            }
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
            return dateTimeFormatter().parser().parseMillis(value);
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            LegacyNumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Object valueForSearch(Object value) {
            Long val = (Long) value;
            if (val == null) {
                return null;
            }
            return dateTimeFormatter().printer().print(val);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, null, null);
        }

        @Override
        public FieldStats.Date stats(IndexReader reader) throws IOException {
            int maxDoc = reader.maxDoc();
            Terms terms = org.apache.lucene.index.MultiFields.getTerms(reader, name());
            if (terms == null) {
                return null;
            }
            long minValue = LegacyNumericUtils.getMinLong(terms);
            long maxValue = LegacyNumericUtils.getMaxLong(terms);
            return new FieldStats.Date(maxDoc, terms.getDocCount(),
                terms.getSumDocFreq(), terms.getSumTotalTermFreq(), isSearchable(), isAggregatable(),
                dateTimeFormatter(), minValue, maxValue);
        }

        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
            return new LateParsingQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        }

        private Query innerRangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
            return LegacyNumericRangeQuery.newLongRange(name(), numericPrecisionStep(),
                lowerTerm == null ? null : parseToMilliseconds(lowerTerm, !includeLower, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                upperTerm == null ? null : parseToMilliseconds(upperTerm, includeUpper, timeZone, forcedDateParser == null ? dateMathParser : forcedDateParser),
                includeLower, includeUpper);
        }

        @Override
        public Relation isFieldWithinQuery(IndexReader reader,
                Object from, Object to,
                boolean includeLower, boolean includeUpper,
                DateTimeZone timeZone, DateMathParser dateParser) throws IOException {
            if (dateParser == null) {
                dateParser = this.dateMathParser;
            }

            Terms terms = org.apache.lucene.index.MultiFields.getTerms(reader, name());
            if (terms == null) {
                // no terms, so nothing matches
                return Relation.DISJOINT;
            }

            long minValue = LegacyNumericUtils.getMinLong(terms);
            long maxValue = LegacyNumericUtils.getMaxLong(terms);

            long fromInclusive = Long.MIN_VALUE;
            if (from != null) {
                fromInclusive = parseToMilliseconds(from, !includeLower, timeZone, dateParser);
                if (includeLower == false) {
                    if (fromInclusive == Long.MAX_VALUE) {
                        return Relation.DISJOINT;
                    }
                    ++fromInclusive;
                }
            }

            long toInclusive = Long.MAX_VALUE;
            if (to != null) {
                toInclusive = parseToMilliseconds(to, includeUpper, timeZone, dateParser);
                if (includeUpper == false) {
                    if (toInclusive == Long.MIN_VALUE) {
                        return Relation.DISJOINT;
                    }
                    --toInclusive;
                }
            }

            if (minValue >= fromInclusive && maxValue <= toInclusive) {
                return Relation.WITHIN;
            } else if (maxValue < fromInclusive || minValue > toInclusive) {
                return Relation.DISJOINT;
            } else {
                return Relation.INTERSECTS;
            }
        }

        public long parseToMilliseconds(Object value, boolean inclusive, @Nullable DateTimeZone zone, @Nullable DateMathParser forcedDateParser) {
            if (value instanceof Long) {
                return ((Long) value).longValue();
            }

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

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.LONG);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, DateTimeZone timeZone) {
            FormatDateTimeFormatter dateTimeFormatter = this.dateTimeFormatter;
            if (format != null) {
                dateTimeFormatter = Joda.forPattern(format);
            }
            if (timeZone == null) {
                timeZone = DateTimeZone.UTC;
            }
            return new DocValueFormat.DateTime(dateTimeFormatter, timeZone);
        }
    }

    protected LegacyDateFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Explicit<Boolean> ignoreMalformed,Explicit<Boolean> coerce,
                              Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, indexSettings, multiFields, copyTo);
    }

    @Override
    public DateFieldType fieldType() {
        return (DateFieldType) super.fieldType();
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
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String dateAsString = null;
        float boost = fieldType().boost();
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            dateAsString = (String) externalValue;
            if (dateAsString == null) {
                dateAsString = fieldType().nullValueAsString();
            }
        } else {
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                dateAsString = fieldType().nullValueAsString();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                dateAsString = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT
                    && Version.indexCreated(context.indexSettings()).before(Version.V_5_0_0_alpha1)) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                dateAsString = fieldType().nullValueAsString();
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
                context.allEntries().addText(fieldType().name(), dateAsString, boost);
            }
            value = fieldType().parseStringValue(dateAsString);
        }

        if (value != null) {
            if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
                CustomLongNumericField field = new CustomLongNumericField(value, fieldType());
                if (boost != 1f && Version.indexCreated(context.indexSettings()).before(Version.V_5_0_0_alpha1)) {
                    field.setBoost(boost);
                }
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
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().numericPrecisionStep() != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", fieldType().numericPrecisionStep());
        }
        builder.field("format", fieldType().dateTimeFormatter().format());
        if (includeDefaults || fieldType().nullValueAsString() != null) {
            builder.field("null_value", fieldType().nullValueAsString());
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
