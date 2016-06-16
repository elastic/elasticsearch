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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.XPointValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyNumberFieldMapper.Defaults;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
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

import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;

/** A {@link FieldMapper} for ip addresses. */
public class DateFieldMapper extends FieldMapper implements AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "date";
    public static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = Joda.forPattern(
            "strict_date_optional_time||epoch_millis", Locale.ROOT);

    public static class Builder extends FieldMapper.Builder<Builder, DateFieldMapper> {

        private Boolean ignoreMalformed;
        private Locale locale;

        public Builder(String name) {
            super(name, new DateFieldType(), new DateFieldType());
            builder = this;
            locale = Locale.ROOT;
        }

        @Override
        public DateFieldType fieldType() {
            return (DateFieldType)fieldType;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            fieldType().setDateTimeFormatter(dateTimeFormatter);
            return this;
        }

        public void locale(Locale locale) {
            this.locale = locale;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            FormatDateTimeFormatter dateTimeFormatter = fieldType().dateTimeFormatter;
            if (!locale.equals(dateTimeFormatter.locale())) {
                fieldType().setDateTimeFormatter( new FormatDateTimeFormatter(dateTimeFormatter.format(),
                        dateTimeFormatter.parser(), dateTimeFormatter.printer(), locale));
            }
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            DateFieldMapper fieldMapper = new DateFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (DateFieldMapper) fieldMapper.includeInAll(includeInAll);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha2)) {
                return new LegacyDateFieldMapper.TypeParser().parse(name, node, parserContext);
            }
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
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue("ignore_malformed", propNode, parserContext));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                    iterator.remove();
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propNode));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class DateFieldType extends MappedFieldType {

        final class LateParsingQuery extends Query {

            final Object lowerTerm;
            final Object upperTerm;
            final boolean includeLower;
            final boolean includeUpper;
            final DateTimeZone timeZone;
            final DateMathParser forcedDateParser;

            public LateParsingQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                    DateTimeZone timeZone, DateMathParser forcedDateParser) {
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

        protected FormatDateTimeFormatter dateTimeFormatter;
        protected DateMathParser dateMathParser;

        DateFieldType() {
            super();
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
            setDateTimeFormatter(DEFAULT_DATE_TIME_FORMATTER);
        }

        DateFieldType(DateFieldType other) {
            super(other);
            setDateTimeFormatter(other.dateTimeFormatter);
        }

        @Override
        public MappedFieldType clone() {
            return new DateFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            DateFieldType that = (DateFieldType) o;
            return Objects.equals(dateTimeFormatter.format(), that.dateTimeFormatter.format()) &&
                   Objects.equals(dateTimeFormatter.locale(), that.dateTimeFormatter.locale());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dateTimeFormatter.format(), dateTimeFormatter.locale());
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
                    conflicts.add("mapper [" + name()
                        + "] is used by multiple types. Set update_all_types to true to update [format] across all types.");
                }
                if (Objects.equals(dateTimeFormatter().locale(), other.dateTimeFormatter().locale()) == false) {
                    conflicts.add("mapper [" + name()
                        + "] is used by multiple types. Set update_all_types to true to update [locale] across all types.");
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

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        long parse(String value) {
            return dateTimeFormatter().parser().parseMillis(value);
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            Query query = innerRangeQuery(value, value, true, true, null, null);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            failIfNotIndexed();
            return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, null, null);
        }

        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
            failIfNotIndexed();
            return new LateParsingQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, forcedDateParser);
        }

        Query innerRangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                @Nullable DateTimeZone timeZone, @Nullable DateMathParser forcedDateParser) {
            failIfNotIndexed();
            DateMathParser parser = forcedDateParser == null
                    ? dateMathParser
                    : forcedDateParser;
            long l, u;
            if (lowerTerm == null) {
                l = Long.MIN_VALUE;
            } else {
                l = parseToMilliseconds(lowerTerm, !includeLower, timeZone, parser);
                if (includeLower == false) {
                    ++l;
                }
            }
            if (upperTerm == null) {
                u = Long.MAX_VALUE;
            } else {
                u = parseToMilliseconds(upperTerm, includeUpper, timeZone, parser);
                if (includeUpper == false) {
                    --u;
                }
            }
            return LongPoint.newRangeQuery(name(), l, u);
        }

        public long parseToMilliseconds(Object value, boolean roundUp,
                @Nullable DateTimeZone zone, @Nullable DateMathParser forcedDateParser) {
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
            return dateParser.parse(strValue, now(), roundUp, zone);
        }

        private static Callable<Long> now() {
            return () -> {
                final SearchContext context = SearchContext.current();
                return context != null
                        ? context.nowInMillis()
                        : System.currentTimeMillis();
            };
        }

        @Override
        public FieldStats.Date stats(IndexReader reader) throws IOException {
            String field = name();
            long size = XPointValues.size(reader, field);
            if (size == 0) {
                return null;
            }
            int docCount = XPointValues.getDocCount(reader, field);
            byte[] min = XPointValues.getMinPackedValue(reader, field);
            byte[] max = XPointValues.getMaxPackedValue(reader, field);
            return new FieldStats.Date(reader.maxDoc(),docCount, -1L, size,
                isSearchable(), isAggregatable(),
                dateTimeFormatter(), LongPoint.decodeDimension(min, 0), LongPoint.decodeDimension(max, 0));
        }

        @Override
        public Relation isFieldWithinQuery(IndexReader reader,
                Object from, Object to,
                boolean includeLower, boolean includeUpper,
                DateTimeZone timeZone, DateMathParser dateParser) throws IOException {
            if (dateParser == null) {
                dateParser = this.dateMathParser;
            }

            if (XPointValues.size(reader, name()) == 0) {
                // no points, so nothing matches
                return Relation.DISJOINT;
            }

            long minValue = LongPoint.decodeDimension(XPointValues.getMinPackedValue(reader, name()), 0);
            long maxValue = LongPoint.decodeDimension(XPointValues.getMaxPackedValue(reader, name()), 0);

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

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.LONG);
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

    private Boolean includeInAll;

    private Explicit<Boolean> ignoreMalformed;

    private DateFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public DateFieldType fieldType() {
        return (DateFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected DateFieldMapper clone() {
        return (DateFieldMapper) super.clone();
    }

    @Override
    public Mapper includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            DateFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            DateFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper unsetIncludeInAll() {
        if (includeInAll != null) {
            DateFieldMapper clone = clone();
            clone.includeInAll = null;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String dateAsString;
        if (context.externalValueSet()) {
            Object dateAsObject = context.externalValue();
            if (dateAsObject == null) {
                dateAsString = null;
            } else {
                dateAsString = dateAsObject.toString();
            }
        } else {
            dateAsString = context.parser().textOrNull();
        }

        if (dateAsString == null) {
            dateAsString = fieldType().nullValueAsString();
        }

        if (dateAsString == null) {
            return;
        }

        long timestamp;
        try {
            timestamp = fieldType().parse(dateAsString);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed.value()) {
                return;
            } else {
                throw e;
            }
        }

        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().name(), dateAsString, fieldType().boost());
        }

        if (fieldType().indexOptions() != IndexOptions.NONE) {
            fields.add(new LongPoint(fieldType().name(), timestamp));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedNumericDocValuesField(fieldType().name(), timestamp));
        }
        if (fieldType().stored()) {
            fields.add(new StoredField(fieldType().name(), timestamp));
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        DateFieldMapper other = (DateFieldMapper) mergeWith;
        this.includeInAll = other.includeInAll;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValueAsString());
        }

        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }
        if (includeDefaults
                || fieldType().dateTimeFormatter().format().equals(DEFAULT_DATE_TIME_FORMATTER.format()) == false) {
            builder.field("format", fieldType().dateTimeFormatter().format());
        }
        if (includeDefaults
                || fieldType().dateTimeFormatter().locale() != Locale.ROOT) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        }
    }
}
