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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.DateRangeIncludingNowQuery;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.common.time.DateUtils.toLong;

/** A {@link FieldMapper} for dates. */
public final class DateFieldMapper extends FieldMapper {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(DateFieldMapper.class);

    public static final String CONTENT_TYPE = "date";
    public static final String DATE_NANOS_CONTENT_TYPE = "date_nanos";
    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");

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

            @Override
            public Instant clampToValidRange(Instant instant) {
                return instant;
            }

            @Override
            public long parsePointAsMillis(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            protected Query distanceFeatureQuery(String field, float boost, long origin, TimeValue pivot) {
                return LongPoint.newDistanceFeatureQuery(field, boost, origin, pivot.getMillis());
            }
        },
        NANOSECONDS(DATE_NANOS_CONTENT_TYPE, NumericType.DATE_NANOSECONDS) {
            @Override
            public long convert(Instant instant) {
                return toLong(instant);
            }

            @Override
            public Instant toInstant(long value) {
                return DateUtils.toInstant(value);
            }

            @Override
            public Instant clampToValidRange(Instant instant) {
                return DateUtils.clampToNanosRange(instant);
            }

            @Override
            public long parsePointAsMillis(byte[] value) {
                return DateUtils.toMilliSeconds(LongPoint.decodeDimension(value, 0));
            }

            @Override
            protected Query distanceFeatureQuery(String field, float boost, long origin, TimeValue pivot) {
                return LongPoint.newDistanceFeatureQuery(field, boost, origin, pivot.getNanos());
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

        /**
         * Convert an {@linkplain Instant} into a long value in this resolution.
         */
        public abstract long convert(Instant instant);

        /**
         * Convert a long value in this resolution into an instant.
         */
        public abstract Instant toInstant(long value);

        /**
         * Return the instant that this range can represent that is closest to
         * the provided instant.
         */
        public abstract Instant clampToValidRange(Instant instant);

        /**
         * Decode the points representation of this field as milliseconds.
         */
        public abstract long parsePointAsMillis(byte[] value);

        public static Resolution ofOrdinal(int ord) {
            for (Resolution resolution : values()) {
                if (ord == resolution.ordinal()) {
                    return resolution;
                }
            }
            throw new IllegalArgumentException("unknown resolution ordinal [" + ord + "]");
        }

        protected abstract Query distanceFeatureQuery(String field, float boost, long origin, TimeValue pivot);
    }

    private static DateFieldMapper toType(FieldMapper in) {
        return (DateFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> toType(m).store, false);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<String> format
            = Parameter.stringParam("format", false, m -> toType(m).format, DEFAULT_DATE_TIME_FORMATTER.pattern());
        private final Parameter<Locale> locale = new Parameter<>("locale", false, () -> Locale.ROOT,
            (n, c, o) -> LocaleUtils.parse(o.toString()), m -> toType(m).locale);

        private final Parameter<String> nullValue
            = Parameter.stringParam("null_value", false, m -> toType(m).nullValueAsString, null).acceptsNull();
        private final Parameter<Boolean> ignoreMalformed;

        private final Resolution resolution;
        private final Version indexCreatedVersion;

        public Builder(String name, Resolution resolution, DateFormatter dateFormatter,
                       boolean ignoreMalformedByDefault, Version indexCreatedVersion) {
            super(name);
            this.resolution = resolution;
            this.indexCreatedVersion = indexCreatedVersion;
            this.ignoreMalformed
                = Parameter.boolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);
            if (dateFormatter != null) {
                this.format.setValue(dateFormatter.pattern());
                this.locale.setValue(dateFormatter.locale());
            }
        }

        private DateFormatter buildFormatter() {
            try {
                return DateFormatter.forPattern(format.getValue()).withLocale(locale.getValue());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Error parsing [format] on field [" + name() + "]: " + e.getMessage(), e);
            }
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(index, docValues, store, format, locale, nullValue, ignoreMalformed, meta);
        }

        private Long parseNullValue(DateFieldType fieldType) {
            if (nullValue.getValue() == null) {
                return null;
            }
            try {
                return fieldType.parse(nullValue.getValue());
            } catch (Exception e) {
                if (indexCreatedVersion.onOrAfter(Version.V_8_0_0)) {
                    throw new MapperParsingException("Error parsing [null_value] on field [" + name() + "]: " + e.getMessage(), e);
                } else {
                    DEPRECATION_LOGGER.deprecate("date_mapper_null_field", "Error parsing [" + nullValue.getValue()
                        + "] as date in [null_value] on field [" + name() + "]); [null_value] will be ignored");
                    return null;
                }
            }
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            DateFieldType ft = new DateFieldType(buildFullName(context), index.getValue(), store.getValue(), docValues.getValue(),
                buildFormatter(), resolution, nullValue.getValue(), meta.getValue());
            Long nullTimestamp = parseNullValue(ft);
            return new DateFieldMapper(name, ft, multiFieldsBuilder.build(this, context),
                copyTo.build(), nullTimestamp, resolution, this);
        }
    }

    public static final TypeParser MILLIS_PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(n, Resolution.MILLISECONDS, c.getDateFormatter(), ignoreMalformedByDefault, c.indexVersionCreated());
    });

    public static final TypeParser NANOS_PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(n, Resolution.NANOSECONDS, c.getDateFormatter(), ignoreMalformedByDefault, c.indexVersionCreated());
    });

    public static final class DateFieldType extends MappedFieldType {
        protected final DateFormatter dateTimeFormatter;
        protected final DateMathParser dateMathParser;
        protected final Resolution resolution;
        protected final String nullValue;

        public DateFieldType(String name, boolean isSearchable, boolean isStored, boolean hasDocValues,
                             DateFormatter dateTimeFormatter, Resolution resolution, String nullValue,
                             Map<String, String> meta) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.dateTimeFormatter = dateTimeFormatter;
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
            this.resolution = resolution;
            this.nullValue = nullValue;
        }

        public DateFieldType(String name) {
            this(name, true, false, true, DEFAULT_DATE_TIME_FORMATTER, Resolution.MILLISECONDS, null, Collections.emptyMap());
        }

        public DateFieldType(String name, DateFormatter dateFormatter) {
            this(name, true, false, true, dateFormatter, Resolution.MILLISECONDS, null, Collections.emptyMap());
        }

        public DateFieldType(String name, Resolution resolution) {
            this(name, true, false, true, DEFAULT_DATE_TIME_FORMATTER, resolution, null, Collections.emptyMap());
        }

        public DateFieldType(String name, Resolution resolution, DateFormatter dateFormatter) {
            this(name, true, false, true, dateFormatter, resolution, null, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return resolution.type();
        }

        public DateFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        public Resolution resolution() {
            return resolution;
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        // Visible for testing.
        public long parse(String value) {
            return resolution.convert(DateFormatters.from(dateTimeFormatter().parse(value), dateTimeFormatter().locale()).toInstant());
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            DateFormatter defaultFormatter = dateTimeFormatter();
            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format).withLocale(defaultFormatter.locale())
                : defaultFormatter;

            return new SourceValueFetcher(name(), mapperService, nullValue) {
                @Override
                public String parseSourceValue(Object value) {
                    String date = value.toString();
                    long timestamp = parse(date);
                    ZonedDateTime dateTime = resolution().toInstant(timestamp).atZone(ZoneOffset.UTC);
                    return formatter.format(dateTime);
                }
            };
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            return rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
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
            return dateRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, parser, context, resolution, (l, u) -> {
                Query query = LongPoint.newRangeQuery(name(), l, u);
                if (hasDocValues()) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);

                    if (context.indexSortedOnField(name())) {
                        query = new IndexSortSortedNumericDocValuesRangeQuery(name(), l, u, query);
                    }
                }
                return query;
            });
        }

        public static Query dateRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            @Nullable ZoneId timeZone,
            DateMathParser parser,
            QueryShardContext context,
            Resolution resolution,
            BiFunction<Long, Long, Query> builder
        ) {
            return handleNow(context, nowSupplier -> {
                long l, u;
                if (lowerTerm == null) {
                    l = Long.MIN_VALUE;
                } else {
                    l = parseToLong(lowerTerm, !includeLower, timeZone, parser, nowSupplier, resolution);
                    if (includeLower == false) {
                        ++l;
                    }
                }
                if (upperTerm == null) {
                    u = Long.MAX_VALUE;
                } else {
                    u = parseToLong(upperTerm, includeUpper, timeZone, parser, nowSupplier, resolution);
                    if (includeUpper == false) {
                        --u;
                    }
                }
                return builder.apply(l, u);
            });
        }

        /**
         * Handle {@code now} in queries.
         * @param context context from which to read the current time
         * @param builder build the query
         * @return the result of the builder, wrapped in {@link DateRangeIncludingNowQuery} if {@code now} was used.
         */
        public static Query handleNow(QueryShardContext context, Function<LongSupplier, Query> builder) {
            boolean[] nowUsed = new boolean[1];
            LongSupplier nowSupplier = () -> {
                nowUsed[0] = true;
                return context.nowInMillis();
            };
            Query query = builder.apply(nowSupplier);
            return nowUsed[0] ? new DateRangeIncludingNowQuery(query) : query;
        }

        public long parseToLong(Object value, boolean roundUp, @Nullable ZoneId zone, DateMathParser dateParser, LongSupplier now) {
            dateParser = dateParser == null ? dateMathParser() : dateParser;
            return parseToLong(value, roundUp, zone, dateParser, now, resolution);
        }

        public static long parseToLong(
            Object value,
            boolean roundUp,
            @Nullable ZoneId zone,
            DateMathParser dateParser,
            LongSupplier now,
            Resolution resolution
        ) {
            return resolution.convert(dateParser.parse(BytesRefs.toString(value), now, roundUp, zone));
        }

        @Override
        public Query distanceFeatureQuery(Object origin, String pivot, QueryShardContext context) {
            long originLong = parseToLong(origin, true, null, null, context::nowInMillis);
            TimeValue pivotTime = TimeValue.parseTimeValue(pivot, "distance_feature.pivot");
            // As we already apply boost in AbstractQueryBuilder::toQuery, we always passing a boost of 1.0 to distanceFeatureQuery
            return resolution.distanceFeatureQuery(name(), 1.0f, originLong, pivotTime);
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
                fromInclusive = parseToLong(from, !includeLower, timeZone, dateParser, context::nowInMillis, resolution);
                if (includeLower == false) {
                    if (fromInclusive == Long.MAX_VALUE) {
                        return Relation.DISJOINT;
                    }
                    ++fromInclusive;
                }
            }

            long toInclusive = Long.MAX_VALUE;
            if (to != null) {
                toInclusive = parseToLong(to, includeUpper, timeZone, dateParser, context::nowInMillis, resolution);
                if (includeUpper == false) {
                    if (toInclusive == Long.MIN_VALUE) {
                        return Relation.DISJOINT;
                    }
                    --toInclusive;
                }
            }

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
        public Function<byte[], Number> pointReaderIfPossible() {
            if (isSearchable()) {
                return resolution()::parsePointAsMillis;
            }
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), resolution.numericType());
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
            // TODO maybe aggs should force millis because lots so of other places want nanos?
            return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, Resolution.MILLISECONDS);
        }
    }

    private final boolean store;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final Locale locale;
    private final String format;
    private final boolean ignoreMalformed;
    private final Long nullValue;
    private final String nullValueAsString;
    private final Resolution resolution;

    private final boolean ignoreMalformedByDefault;
    private final Version indexCreatedVersion;

    private DateFieldMapper(
            String simpleName,
            MappedFieldType mappedFieldType,
            MultiFields multiFields,
            CopyTo copyTo,
            Long nullValue,
            Resolution resolution,
            Builder builder) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.store = builder.store.getValue();
        this.indexed = builder.index.getValue();
        this.hasDocValues = builder.docValues.getValue();
        this.locale = builder.locale.getValue();
        this.format = builder.format.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.nullValueAsString = builder.nullValue.getValue();
        this.nullValue = nullValue;
        this.resolution = resolution;
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue();
        this.indexCreatedVersion = builder.indexCreatedVersion;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), resolution, null, ignoreMalformedByDefault, indexCreatedVersion).init(this);
    }

    @Override
    public DateFieldType fieldType() {
        return (DateFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().resolution.type();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
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

        long timestamp;
        if (dateAsString == null) {
            if (nullValue == null) {
                return;
            }
            timestamp = nullValue;
        } else {
            try {
                timestamp = fieldType().parse(dateAsString);
            } catch (IllegalArgumentException | ElasticsearchParseException | DateTimeException | ArithmeticException e) {
                if (ignoreMalformed) {
                    context.addIgnoredField(mappedFieldType.name());
                    return;
                } else {
                    throw e;
                }
            }
        }

        if (indexed) {
            context.doc().add(new LongPoint(fieldType().name(), timestamp));
        }
        if (hasDocValues) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), timestamp));
        } else if (store || indexed) {
            createFieldNamesField(context);
        }
        if (store) {
            context.doc().add(new StoredField(fieldType().name(), timestamp));
        }
    }

    public boolean getIgnoreMalformed() {
        return ignoreMalformed;
    }

    public Long getNullValue() {
        return nullValue;
    }
}
