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
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

/** A {@link FieldMapper} for ip addresses. */
public class DateFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "date";
    public static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = Joda.forPattern(
            "strict_date_optional_time||epoch_millis", Locale.ROOT);

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
    }

    public static class Builder extends FieldMapper.Builder<Builder, DateFieldMapper> {

        private Boolean ignoreMalformed;
        private Locale locale;
        private boolean dateTimeFormatterSet = false;

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

        /** Whether an explicit format for this date field has been set already. */
        public boolean isDateTimeFormatterSet() {
            return dateTimeFormatterSet;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            fieldType().setDateTimeFormatter(dateTimeFormatter);
            dateTimeFormatterSet = true;
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
            return new DateFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue(name, "ignore_malformed", propNode, parserContext));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    Locale locale;
                    if (parserContext.indexVersionCreated().onOrAfter(Version.V_6_0_0_beta2)) {
                        locale = LocaleUtils.parse(propNode.toString());
                    } else {
                        locale = LocaleUtils.parse5x(propNode.toString());
                    }
                    builder.locale(locale);
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
            DateFieldType other = (DateFieldType) fieldType;
            if (Objects.equals(dateTimeFormatter().format(), other.dateTimeFormatter().format()) == false) {
                conflicts.add("mapper [" + name() + "] has different [format] values");
            }
            if (Objects.equals(dateTimeFormatter().locale(), other.dateTimeFormatter().locale()) == false) {
                conflicts.add("mapper [" + name() + "] has different [locale] values");
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
                l = parseToMilliseconds(lowerTerm, !includeLower, timeZone, parser, context);
                if (includeLower == false) {
                    ++l;
                }
            }
            if (upperTerm == null) {
                u = Long.MAX_VALUE;
            } else {
                u = parseToMilliseconds(upperTerm, includeUpper, timeZone, parser, context);
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

        public long parseToMilliseconds(Object value, boolean roundUp,
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
            return dateParser.parse(strValue, context::nowInMillis, roundUp, zone);
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
                fromInclusive = parseToMilliseconds(from, !includeLower, timeZone, dateParser, context);
                if (includeLower == false) {
                    if (fromInclusive == Long.MAX_VALUE) {
                        return Relation.DISJOINT;
                    }
                    ++fromInclusive;
                }
            }

            long toInclusive = Long.MAX_VALUE;
            if (to != null) {
                toInclusive = parseToMilliseconds(to, includeUpper, timeZone, dateParser, context);
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
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.DATE);
        }

        @Override
        public Object valueForDisplay(Object value) {
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
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed.value()) {
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
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
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
                || fieldType().dateTimeFormatter().format().equals(DEFAULT_DATE_TIME_FORMATTER.format()) == false) {
            builder.field("format", fieldType().dateTimeFormatter().format());
        }
        if (includeDefaults
                || fieldType().dateTimeFormatter().locale() != Locale.ROOT) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        }
    }
}
