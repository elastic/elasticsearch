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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.time.DateUtils.toLong;

/** A {@link FieldMapper} for dates. */
public final class DateFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "date";
    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
    }

    public enum Resolution {
        MILLISECONDS(CONTENT_TYPE, NumericType.DATE) {
            @Override
            public long convert(Instant instant) {
                return instant.toEpochMilli();
            }

            @Override
            public Instant toInstant(long value) {
                return Instant.ofEpochMilli(value);
            }
        },
        NANOSECONDS("date_nanos", NumericType.DATE_NANOSECONDS) {
            @Override
            public long convert(Instant instant) {
                return toLong(instant);
            }

            @Override
            public Instant toInstant(long value) {
                return DateUtils.toInstant(value);
            }
        };

        private final String type;
        private final NumericType numericType;

        Resolution(String type, NumericType numericType) {
            this.type = type;
            this.numericType = numericType;
        }

        public String type() {
            return type;
        }

        NumericType numericType() {
            return numericType;
        }

        public abstract long convert(Instant instant);

        public abstract Instant toInstant(long value);

        public static Resolution ofOrdinal(int ord) {
            for (Resolution resolution : values()) {
                if (ord == resolution.ordinal()) {
                    return resolution;
                }
            }
            throw new IllegalArgumentException("unknown resolution ordinal [" + ord + "]");
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DateFieldMapper> {

        private Boolean ignoreMalformed;
        private Explicit<String> format = new Explicit<>(DEFAULT_DATE_TIME_FORMATTER.pattern(), false);
        private Locale locale;
        private Resolution resolution = Resolution.MILLISECONDS;

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

        public Builder locale(Locale locale) {
            this.locale = locale;
            return this;
        }

        public Locale locale() {
            return locale;
        }

        public String format() {
            return format.value();
        }

        public Builder format(String format) {
            this.format = new Explicit<>(format, true);
            return this;
        }

        public Builder withResolution(Resolution resolution) {
            this.resolution = resolution;
            return this;
        }

        public boolean isFormatterSet() {
            return format.explicit();
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            String pattern = this.format.value();
            DateFormatter dateTimeFormatter = fieldType().dateTimeFormatter;

            boolean hasPatternChanged = Strings.hasLength(pattern) && Objects.equals(pattern, dateTimeFormatter.pattern()) == false;
            if (hasPatternChanged || Objects.equals(builder.locale, dateTimeFormatter.locale()) == false) {
                fieldType().setDateTimeFormatter(DateFormatter.forPattern(pattern).withLocale(locale));
            }

            fieldType().setResolution(resolution);
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DateFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final Resolution resolution;

        public TypeParser(Resolution resolution) {
            this.resolution = resolution;
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.withResolution(resolution);
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
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                    iterator.remove();
                } else if (propName.equals("format")) {
                    builder.format(propNode.toString());
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class DateFieldType extends MappedFieldType {
        protected DateFormatter dateTimeFormatter;
        protected DateMathParser dateMathParser;
        protected Resolution resolution;

        public DateFieldType() {
            super();
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
            setDateTimeFormatter(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
            setResolution(Resolution.MILLISECONDS);
        }

        DateFieldType(DateFieldType other) {
            super(other);
            setDateTimeFormatter(other.dateTimeFormatter);
            setResolution(other.resolution);
        }

        @Override
        public MappedFieldType clone() {
            return new DateFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) {
                return false;
            }
            DateFieldType that = (DateFieldType) o;
            return Objects.equals(dateTimeFormatter, that.dateTimeFormatter) && Objects.equals(resolution, that.resolution);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dateTimeFormatter, resolution);
        }

        @Override
        public String typeName() {
            return resolution.type();
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts) {
            super.checkCompatibility(fieldType, conflicts);
            DateFieldType other = (DateFieldType) fieldType;
            if (Objects.equals(dateTimeFormatter.pattern(), other.dateTimeFormatter.pattern()) == false) {
                conflicts.add("mapper [" + name() + "] has different [format] values");
            }
            if (Objects.equals(dateTimeFormatter.locale(), other.dateTimeFormatter.locale()) == false) {
                conflicts.add("mapper [" + name() + "] has different [locale] values");
            }
            if (Objects.equals(resolution.type(), other.resolution.type()) == false) {
                conflicts.add("mapper [" + name() + "] cannot change between milliseconds and nanoseconds");
            }
        }

        public DateFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        public Resolution resolution() {
            return resolution;
        }

        void setDateTimeFormatter(DateFormatter formatter) {
            checkIfFrozen();
            this.dateTimeFormatter = formatter;
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
        }

        void setResolution(Resolution resolution) {
            checkIfFrozen();
            this.resolution = resolution;
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        public long parse(String value) {
            return resolution.convert(DateFormatters.from(dateTimeFormatter().parse(value)).toInstant());
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
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            Query query = rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, ShapeRelation relation,
                                @Nullable ZoneId timeZone, @Nullable DateMathParser forcedDateParser, QueryShardContext context) {
            failIfNotIndexed();
            if (relation == ShapeRelation.DISJOINT) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                        "] does not support DISJOINT ranges");
            }
            DateMathParser parser = forcedDateParser == null
                    ? dateMathParser
                    : forcedDateParser;
            long l, u;
            if (lowerTerm == null) {
                l = Long.MIN_VALUE;
            } else {
                l = parseToLong(lowerTerm, !includeLower, timeZone, parser, context);
                if (includeLower == false) {
                    ++l;
                }
            }
            if (upperTerm == null) {
                u = Long.MAX_VALUE;
            } else {
                u = parseToLong(upperTerm, includeUpper, timeZone, parser, context);
                if (includeUpper == false) {
                    --u;
                }
            }
            Query query = LongPoint.newRangeQuery(name(), l, u);
            if (hasDocValues()) {
                Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }

        public long parseToLong(Object value, boolean roundUp,
                                @Nullable ZoneId zone, @Nullable DateMathParser forcedDateParser, QueryRewriteContext context) {
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
            Instant instant = dateParser.parse(strValue, context::nowInMillis, roundUp, zone);
            return resolution.convert(instant);
        }

        @Override
        public Relation isFieldWithinQuery(IndexReader reader,
                                           Object from, Object to, boolean includeLower, boolean includeUpper,
                                           ZoneId timeZone, DateMathParser dateParser, QueryRewriteContext context) throws IOException {
            if (dateParser == null) {
                dateParser = this.dateMathParser;
            }

            long fromInclusive = Long.MIN_VALUE;
            if (from != null) {
                fromInclusive = parseToLong(from, !includeLower, timeZone, dateParser, context);
                if (includeLower == false) {
                    if (fromInclusive == Long.MAX_VALUE) {
                        return Relation.DISJOINT;
                    }
                    ++fromInclusive;
                }
            }

            long toInclusive = Long.MAX_VALUE;
            if (to != null) {
                toInclusive = parseToLong(to, includeUpper, timeZone, dateParser, context);
                if (includeUpper == false) {
                    if (toInclusive == Long.MIN_VALUE) {
                        return Relation.DISJOINT;
                    }
                    --toInclusive;
                }
            }

            // This check needs to be done after fromInclusive and toInclusive
            // are resolved so we can throw an exception if they are invalid
            // even if there are no points in the shard
            if (PointValues.size(reader, name()) == 0) {
                // no points, so nothing matches
                return Relation.DISJOINT;
            }

            long minValue = LongPoint.decodeDimension(PointValues.getMinPackedValue(reader, name()), 0);
            long maxValue = LongPoint.decodeDimension(PointValues.getMaxPackedValue(reader, name()), 0);

            if (minValue >= fromInclusive && maxValue <= toInclusive) {
                return Relation.WITHIN;
            } else if (maxValue < fromInclusive || minValue > toInclusive) {
                return Relation.DISJOINT;
            } else {
                return Relation.INTERSECTS;
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(resolution.numericType());
        }

        @Override
        public Object valueForDisplay(Object value) {
            Long val = (Long) value;
            if (val == null) {
                return null;
            }
            return dateTimeFormatter().format(resolution.toInstant(val).atZone(ZoneOffset.UTC));
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            DateFormatter dateTimeFormatter = this.dateTimeFormatter;
            if (format != null) {
                dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
            }
            if (timeZone == null) {
                timeZone = ZoneOffset.UTC;
            }
            // the resolution here is always set to milliseconds, as aggregations use this formatter mainly and those are always in
            // milliseconds. The only special case here is docvalue fields, which are handled somewhere else
            return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, Resolution.MILLISECONDS);
        }
    }

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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
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
        } catch (IllegalArgumentException | ElasticsearchParseException | DateTimeException e) {
            if (ignoreMalformed.value()) {
                context.addIgnoredField(fieldType.name());
                return;
            } else {
                throw e;
            }
        }

        if (fieldType().indexOptions() != IndexOptions.NONE) {
            fields.add(new LongPoint(fieldType().name(), timestamp));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedNumericDocValuesField(fieldType().name(), timestamp));
        } else if (fieldType().stored() || fieldType().indexOptions() != IndexOptions.NONE) {
            createFieldNamesField(context, fields);
        }
        if (fieldType().stored()) {
            fields.add(new StoredField(fieldType().name(), timestamp));
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        final DateFieldMapper other = (DateFieldMapper) mergeWith;
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

        if (includeDefaults
                || fieldType().dateTimeFormatter().pattern().equals(DEFAULT_DATE_TIME_FORMATTER.pattern()) == false) {
            builder.field("format", fieldType().dateTimeFormatter().pattern());
        }

        if (includeDefaults
            || fieldType().dateTimeFormatter().locale().equals(DEFAULT_DATE_TIME_FORMATTER.locale()) == false) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        }
    }
}
