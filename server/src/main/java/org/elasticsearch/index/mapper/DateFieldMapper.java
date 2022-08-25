/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.sandbox.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.DateRangeIncludingNowQuery;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.SortedNumericDocValuesLongFieldScript;
import org.elasticsearch.script.field.DateMillisDocValuesField;
import org.elasticsearch.script.field.DateNanosDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.LongScriptFieldDistanceFeatureQuery;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.text.NumberFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.time.DateUtils.toLong;

/** A {@link FieldMapper} for dates. */
public final class DateFieldMapper extends FieldMapper {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(DateFieldMapper.class);
    private static final Logger logger = LogManager.getLogger(DateFieldMapper.class);

    public static final String CONTENT_TYPE = "date";
    public static final String DATE_NANOS_CONTENT_TYPE = "date_nanos";
    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
    public static final DateFormatter DEFAULT_DATE_TIME_NANOS_FORMATTER = DateFormatter.forPattern(
        "strict_date_optional_time_nanos||epoch_millis"
    );
    private static final DateMathParser EPOCH_MILLIS_PARSER = DateFormatter.forPattern("epoch_millis").toDateMathParser();

    public enum Resolution {
        MILLISECONDS(CONTENT_TYPE, NumericType.DATE, DateMillisDocValuesField::new) {
            @Override
            public long convert(Instant instant) {
                return instant.toEpochMilli();
            }

            @Override
            public long convert(TimeValue timeValue) {
                return timeValue.millis();
            }

            @Override
            public Instant toInstant(long value) {
                return Instant.ofEpochMilli(value);
            }

            @Override
            public long parsePointAsMillis(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            public long roundDownToMillis(long value) {
                return value;
            }

            @Override
            public long roundUpToMillis(long value) {
                return value;
            }
        },
        NANOSECONDS(DATE_NANOS_CONTENT_TYPE, NumericType.DATE_NANOSECONDS, DateNanosDocValuesField::new) {
            @Override
            public long convert(Instant instant) {
                return toLong(instant);
            }

            @Override
            public long convert(TimeValue timeValue) {
                return timeValue.nanos();
            }

            @Override
            public Instant toInstant(long value) {
                return DateUtils.toInstant(value);
            }

            @Override
            public long parsePointAsMillis(byte[] value) {
                return roundDownToMillis(LongPoint.decodeDimension(value, 0));
            }

            @Override
            public long roundDownToMillis(long value) {
                return DateUtils.toMilliSeconds(value);
            }

            @Override
            public long roundUpToMillis(long value) {
                if (value <= 0L) {
                    // if negative then throws an IAE; if zero then return zero
                    return DateUtils.toMilliSeconds(value);
                } else {
                    return DateUtils.toMilliSeconds(value - 1L) + 1L;
                }
            }
        };

        private final String type;
        private final NumericType numericType;
        private final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        Resolution(String type, NumericType numericType, ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory) {
            this.type = type;
            this.numericType = numericType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        public String type() {
            return type;
        }

        NumericType numericType() {
            return numericType;
        }

        ToScriptFieldFactory<SortedNumericDocValues> getDefaultToScriptFieldFactory() {
            return toScriptFieldFactory;
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
         * Convert an {@linkplain TimeValue} into a long value in this resolution.
         */
        public abstract long convert(TimeValue timeValue);

        /**
         * Decode the points representation of this field as milliseconds.
         */
        public abstract long parsePointAsMillis(byte[] value);

        /**
         * Round the given raw value down to a number of milliseconds since the epoch.
         */
        public abstract long roundDownToMillis(long value);

        /**
         * Round the given raw value up to a number of milliseconds since the epoch.
         */
        public abstract long roundUpToMillis(long value);

        public static Resolution ofOrdinal(int ord) {
            for (Resolution resolution : values()) {
                if (ord == resolution.ordinal()) {
                    return resolution;
                }
            }
            throw new IllegalArgumentException("unknown resolution ordinal [" + ord + "]");
        }
    }

    private static DateFieldMapper toType(FieldMapper in) {
        return (DateFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> toType(m).store, false);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<String> format;
        private final Parameter<Locale> locale = new Parameter<>(
            "locale",
            false,
            () -> Locale.ROOT,
            (n, c, o) -> LocaleUtils.parse(o.toString()),
            m -> toType(m).locale,
            (xContentBuilder, n, v) -> xContentBuilder.field(n, v.toString()),
            Objects::toString
        );

        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValueAsString, null)
            .acceptsNull();
        private final Parameter<Boolean> ignoreMalformed;

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);

        private final Resolution resolution;
        private final Version indexCreatedVersion;
        private final ScriptCompiler scriptCompiler;

        public Builder(
            String name,
            Resolution resolution,
            DateFormatter dateFormatter,
            ScriptCompiler scriptCompiler,
            boolean ignoreMalformedByDefault,
            Version indexCreatedVersion
        ) {
            super(name);
            this.resolution = resolution;
            this.indexCreatedVersion = indexCreatedVersion;
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.ignoreMalformed = Parameter.boolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);

            this.script.precludesParameters(nullValue, ignoreMalformed);
            addScriptValidation(script, index, docValues);

            DateFormatter defaultFormat = resolution == Resolution.MILLISECONDS
                ? DEFAULT_DATE_TIME_FORMATTER
                : DEFAULT_DATE_TIME_NANOS_FORMATTER;
            this.format = Parameter.stringParam(
                "format",
                indexCreatedVersion.isLegacyIndexVersion(),
                m -> toType(m).format,
                defaultFormat.pattern()
            );
            if (dateFormatter != null) {
                this.format.setValue(dateFormatter.pattern());
                this.locale.setValue(dateFormatter.locale());
            }
        }

        DateFormatter buildFormatter() {
            try {
                return DateFormatter.forPattern(format.getValue(), indexCreatedVersion).withLocale(locale.getValue());
            } catch (IllegalArgumentException e) {
                if (indexCreatedVersion.isLegacyIndexVersion()) {
                    logger.warn(() -> "Error parsing format [" + format.getValue() + "] of legacy index, falling back to default", e);
                    return DateFormatter.forPattern(format.getDefaultValue()).withLocale(locale.getValue());
                } else {
                    throw new IllegalArgumentException("Error parsing [format] on field [" + name() + "]: " + e.getMessage(), e);
                }
            }
        }

        private FieldValues<Long> scriptValues() {
            if (script.get() == null) {
                return null;
            }
            DateFieldScript.Factory factory = scriptCompiler.compile(script.get(), DateFieldScript.CONTEXT);
            return factory == null
                ? null
                : (lookup, ctx, doc, consumer) -> factory.newFactory(name, script.get().getParams(), lookup, buildFormatter())
                    .newInstance(ctx)
                    .runForDoc(doc, consumer::accept);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { index, docValues, store, format, locale, nullValue, ignoreMalformed, script, onScriptError, meta };
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
                    DEPRECATION_LOGGER.warn(
                        DeprecationCategory.MAPPINGS,
                        "date_mapper_null_field",
                        "Error parsing ["
                            + nullValue.getValue()
                            + "] as date in [null_value] on field ["
                            + name()
                            + "]); [null_value] will be ignored"
                    );
                    return null;
                }
            }
        }

        @Override
        public DateFieldMapper build(MapperBuilderContext context) {
            DateFieldType ft = new DateFieldType(
                context.buildFullName(name()),
                index.getValue() && indexCreatedVersion.isLegacyIndexVersion() == false,
                index.getValue(),
                store.getValue(),
                docValues.getValue(),
                buildFormatter(),
                resolution,
                nullValue.getValue(),
                scriptValues(),
                meta.getValue()
            );

            Long nullTimestamp = parseNullValue(ft);
            return new DateFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), nullTimestamp, resolution, this);
        }
    }

    private static final Version MINIMUM_COMPATIBILITY_VERSION = Version.fromString("5.0.0");

    public static final TypeParser MILLIS_PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(
            n,
            Resolution.MILLISECONDS,
            c.getDateFormatter(),
            c.scriptCompiler(),
            ignoreMalformedByDefault,
            c.indexVersionCreated()
        );
    }, MINIMUM_COMPATIBILITY_VERSION);

    public static final TypeParser NANOS_PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(
            n,
            Resolution.NANOSECONDS,
            c.getDateFormatter(),
            c.scriptCompiler(),
            ignoreMalformedByDefault,
            c.indexVersionCreated()
        );
    }, MINIMUM_COMPATIBILITY_VERSION);

    public static final class DateFieldType extends MappedFieldType {
        protected final DateFormatter dateTimeFormatter;
        protected final DateMathParser dateMathParser;
        protected final Resolution resolution;
        protected final String nullValue;
        protected final FieldValues<Long> scriptValues;
        private final boolean pointsMetadataAvailable;

        public DateFieldType(
            String name,
            boolean isIndexed,
            boolean pointsMetadataAvailable,
            boolean isStored,
            boolean hasDocValues,
            DateFormatter dateTimeFormatter,
            Resolution resolution,
            String nullValue,
            FieldValues<Long> scriptValues,
            Map<String, String> meta
        ) {
            super(name, isIndexed, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.dateTimeFormatter = dateTimeFormatter;
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
            this.resolution = resolution;
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
            this.pointsMetadataAvailable = pointsMetadataAvailable;
        }

        public DateFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            DateFormatter dateTimeFormatter,
            Resolution resolution,
            String nullValue,
            FieldValues<Long> scriptValues,
            Map<String, String> meta
        ) {
            this(name, isIndexed, isIndexed, isStored, hasDocValues, dateTimeFormatter, resolution, nullValue, scriptValues, meta);
        }

        public DateFieldType(String name) {
            this(name, true, true, false, true, DEFAULT_DATE_TIME_FORMATTER, Resolution.MILLISECONDS, null, null, Collections.emptyMap());
        }

        public DateFieldType(String name, boolean isIndexed) {
            this(
                name,
                isIndexed,
                isIndexed,
                false,
                true,
                DEFAULT_DATE_TIME_FORMATTER,
                Resolution.MILLISECONDS,
                null,
                null,
                Collections.emptyMap()
            );
        }

        public DateFieldType(String name, DateFormatter dateFormatter) {
            this(name, true, true, false, true, dateFormatter, Resolution.MILLISECONDS, null, null, Collections.emptyMap());
        }

        public DateFieldType(String name, Resolution resolution) {
            this(name, true, true, false, true, DEFAULT_DATE_TIME_FORMATTER, resolution, null, null, Collections.emptyMap());
        }

        public DateFieldType(String name, Resolution resolution, DateFormatter dateFormatter) {
            this(name, true, true, false, true, dateFormatter, resolution, null, null, Collections.emptyMap());
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

        /**
         * Format to use to resolve {@link Number}s from the source. Its valid
         * to send the numbers with up to six digits after the decimal place
         * and we'll parse them as {@code millis.nanos}. The source
         * deseralization code isn't particularly careful here and can return
         * {@link double} instead of the exact string in the {@code _source}.
         * So we have to *get* that string.
         * <p>
         * Nik chose not to use {@link String#format} for this because it feels
         * a little wasteful. It'd probably be fine but this makes Nik feel a
         * less bad about the {@code instanceof} and the string allocation.
         */
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.ROOT);
        static {
            NUMBER_FORMAT.setGroupingUsed(false);
            NUMBER_FORMAT.setMaximumFractionDigits(6);
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return context.fieldExistsInIndex(this.name());
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            DateFormatter defaultFormatter = dateTimeFormatter();
            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format).withLocale(defaultFormatter.locale())
                : defaultFormatter;
            if (scriptValues != null) {
                return FieldValues.valueFetcher(scriptValues, v -> format((long) v, formatter), context);
            }
            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                public String parseSourceValue(Object value) {
                    String date = value instanceof Number ? NUMBER_FORMAT.format(value) : value.toString();
                    // TODO can we emit a warning if we're losing precision here? I'm not sure we can.
                    return format(parse(date), formatter);
                }
            };
        }

        // returns a Long to support source fallback which emulates numeric doc values for dates
        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths) {
            return new SourceValueFetcher(sourcePaths, nullValue) {
                @Override
                public Long parseSourceValue(Object value) {
                    String date = value instanceof Number ? NUMBER_FORMAT.format(value) : value.toString();
                    return parse(date);
                }
            };
        }

        private String format(long timestamp, DateFormatter formatter) {
            ZonedDateTime dateTime = resolution().toInstant(timestamp).atZone(ZoneOffset.UTC);
            return formatter.format(dateTime);
        }

        @Override
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            return rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            @Nullable ZoneId timeZone,
            @Nullable DateMathParser forcedDateParser,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (relation == ShapeRelation.DISJOINT) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support DISJOINT ranges");
            }
            DateMathParser parser;
            if (forcedDateParser == null) {
                if (lowerTerm instanceof Number || upperTerm instanceof Number) {
                    // force epoch_millis
                    parser = EPOCH_MILLIS_PARSER;
                } else {
                    parser = dateMathParser;
                }
            } else {
                parser = forcedDateParser;
            }
            return dateRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, parser, context, resolution, (l, u) -> {
                Query query;
                if (isIndexed()) {
                    query = LongPoint.newRangeQuery(name(), l, u);
                    if (hasDocValues()) {
                        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                } else {
                    query = SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
                }
                if (hasDocValues() && context.indexSortedOnField(name())) {
                    query = new IndexSortSortedNumericDocValuesRangeQuery(name(), l, u, query);
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
            SearchExecutionContext context,
            Resolution resolution,
            BiFunction<Long, Long, Query> builder
        ) {
            return handleNow(context, nowSupplier -> {
                long l, u;
                if (lowerTerm == null) {
                    l = Long.MIN_VALUE;
                } else {
                    l = parseToLong(lowerTerm, includeLower == false, timeZone, parser, nowSupplier, resolution);
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
        public static Query handleNow(SearchExecutionContext context, Function<LongSupplier, Query> builder) {
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
        public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            long originLong = parseToLong(origin, true, null, null, context::nowInMillis);
            TimeValue pivotTime = TimeValue.parseTimeValue(pivot, "distance_feature.pivot");
            long pivotLong = resolution.convert(pivotTime);
            // As we already apply boost in AbstractQueryBuilder::toQuery, we always passing a boost of 1.0 to distanceFeatureQuery
            if (isIndexed()) {
                return LongPoint.newDistanceFeatureQuery(name(), 1.0f, originLong, pivotLong);
            } else {
                return new LongScriptFieldDistanceFeatureQuery(
                    new Script(""),
                    ctx -> new SortedNumericDocValuesLongFieldScript(name(), context.lookup(), ctx),
                    name(),
                    originLong,
                    pivotLong
                );
            }
        }

        @Override
        public Relation isFieldWithinQuery(
            IndexReader reader,
            Object from,
            Object to,
            boolean includeLower,
            boolean includeUpper,
            ZoneId timeZone,
            DateMathParser dateParser,
            QueryRewriteContext context
        ) throws IOException {
            if (isIndexed() == false && pointsMetadataAvailable == false && hasDocValues()) {
                // we don't have a quick way to run this check on doc values, so fall back to default assuming we are within bounds
                return Relation.INTERSECTS;
            }
            byte[] minPackedValue = PointValues.getMinPackedValue(reader, name());
            if (minPackedValue == null) {
                // no points, so nothing matches
                return Relation.DISJOINT;
            }
            long minValue = LongPoint.decodeDimension(minPackedValue, 0);
            long maxValue = LongPoint.decodeDimension(PointValues.getMaxPackedValue(reader, name()), 0);

            return isFieldWithinQuery(minValue, maxValue, from, to, includeLower, includeUpper, timeZone, dateParser, context);
        }

        public Relation isFieldWithinQuery(
            long minValue,
            long maxValue,
            Object from,
            Object to,
            boolean includeLower,
            boolean includeUpper,
            ZoneId timeZone,
            DateMathParser dateParser,
            QueryRewriteContext context
        ) {
            if (dateParser == null) {
                if (from instanceof Number || to instanceof Number) {
                    // force epoch_millis
                    dateParser = EPOCH_MILLIS_PARSER;
                } else {
                    dateParser = this.dateMathParser;
                }
            }

            long fromInclusive = Long.MIN_VALUE;
            if (from != null) {
                fromInclusive = parseToLong(from, includeLower == false, timeZone, dateParser, context::nowInMillis, resolution);
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
            if (isIndexed()) {
                return resolution()::parsePointAsMillis;
            }
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return new SortedNumericIndexFieldData.Builder(
                    name(),
                    resolution.numericType(),
                    resolution.getDefaultToScriptFieldFactory()
                );
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherSortedNumericIndexFieldData.Builder(
                    name(),
                    resolution.numericType().getValuesSourceType(),
                    sourceValueFetcher(sourcePaths),
                    searchLookup.source(),
                    resolution.getDefaultToScriptFieldFactory()
                );
            }

            throw new IllegalStateException("unknown field data operation [" + operation.name() + "]");
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

    private final Script script;
    private final ScriptCompiler scriptCompiler;
    private final FieldValues<Long> scriptValues;

    private DateFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Long nullValue,
        Resolution resolution,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, builder.script.get() != null, builder.onScriptError.get());
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
        this.script = builder.script.get();
        this.scriptCompiler = builder.scriptCompiler;
        this.scriptValues = builder.scriptValues();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), resolution, null, scriptCompiler, ignoreMalformedByDefault, indexCreatedVersion).init(this);
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
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        String dateAsString = context.parser().textOrNull();

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

        indexValue(context, timestamp);
    }

    private void indexValue(DocumentParserContext context, long timestamp) {
        if (indexed) {
            context.doc().add(new LongPoint(fieldType().name(), timestamp));
        }
        if (hasDocValues) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), timestamp));
        } else if (store || indexed) {
            context.addToFieldNames(fieldType().name());
        }
        if (store) {
            context.doc().add(new StoredField(fieldType().name(), timestamp));
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, v -> indexValue(documentParserContext, v));
    }

    public boolean getIgnoreMalformed() {
        return ignoreMalformed;
    }

    public Long getNullValue() {
        return nullValue;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (hasScript) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }
        if (hasDocValues == false) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it doesn't have doc values"
            );
        }
        if (ignoreMalformed) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it ignores malformed dates"
            );
        }
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        return new SortedNumericDocValuesSyntheticFieldLoader(name(), simpleName()) {
            @Override
            protected void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(fieldType().format(value, fieldType().dateTimeFormatter()));
            }
        };
    }
}
