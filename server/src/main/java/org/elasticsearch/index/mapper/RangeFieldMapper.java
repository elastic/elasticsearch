/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BinaryIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;

/** A {@link FieldMapper} for indexing numeric and date ranges, and creating queries */
public class RangeFieldMapper extends FieldMapper {
    public static final NodeFeature NULL_VALUES_OFF_BY_ONE_FIX = new NodeFeature("mapper.range.null_values_off_by_one_fix");
    public static final NodeFeature DATE_RANGE_INDEXING_FIX = new NodeFeature("mapper.range.date_range_indexing_fix");

    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    public static class Defaults {
        public static final DateFormatter DATE_FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
        public static final Locale LOCALE = DateFieldMapper.DEFAULT_LOCALE;
    }

    // this is private since it has a different default
    static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", true, Setting.Property.IndexScope);

    private static RangeFieldMapper toType(FieldMapper in) {
        return (RangeFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).index, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> toType(m).store, false);
        private final Parameter<Explicit<Boolean>> coerce;
        private final Parameter<String> format = Parameter.stringParam(
            "format",
            false,
            m -> toType(m).format,
            Defaults.DATE_FORMATTER.pattern()
        );
        private final Parameter<Locale> locale = new Parameter<>(
            "locale",
            false,
            () -> Defaults.LOCALE,
            (n, c, o) -> LocaleUtils.parse(o.toString()),
            m -> toType(m).locale,
            (xContentBuilder, n, v) -> xContentBuilder.field(n, v.toString()),
            Objects::toString
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final RangeType type;

        public Builder(String name, RangeType type, Settings settings) {
            this(name, type, COERCE_SETTING.get(settings));
        }

        public Builder(String name, RangeType type, boolean coerceByDefault) {
            super(name);
            this.type = type;
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            if (this.type != RangeType.DATE) {
                format.neverSerialize();
                locale.neverSerialize();
            }
        }

        public void docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
        }

        Builder format(String format) {
            this.format.setValue(format);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { index, hasDocValues, store, coerce, format, locale, meta };
        }

        protected RangeFieldType setupFieldType(MapperBuilderContext context) {
            String fullName = context.buildFullName(leafName());
            if (format.isConfigured()) {
                if (type != RangeType.DATE) {
                    throw new IllegalArgumentException(
                        "field ["
                            + leafName()
                            + "] of type [range]"
                            + " should not define a dateTimeFormatter unless it is a "
                            + RangeType.DATE
                            + " type"
                    );
                }
                return new RangeFieldType(
                    fullName,
                    index.getValue(),
                    store.getValue(),
                    hasDocValues.getValue(),
                    DateFormatter.forPattern(format.getValue()).withLocale(locale.getValue()),
                    coerce.getValue().value(),
                    meta.getValue()
                );
            }
            if (type == RangeType.DATE) {
                return new RangeFieldType(
                    fullName,
                    index.getValue(),
                    store.getValue(),
                    hasDocValues.getValue(),
                    Defaults.DATE_FORMATTER,
                    coerce.getValue().value(),
                    meta.getValue()
                );
            }
            return new RangeFieldType(
                fullName,
                type,
                index.getValue(),
                store.getValue(),
                hasDocValues.getValue(),
                coerce.getValue().value(),
                meta.getValue()
            );
        }

        @Override
        public RangeFieldMapper build(MapperBuilderContext context) {
            RangeFieldType ft = setupFieldType(context);
            return new RangeFieldMapper(leafName(), ft, builderParams(this, context), type, this);
        }
    }

    public static final class RangeFieldType extends MappedFieldType {
        protected final RangeType rangeType;
        protected final DateFormatter dateTimeFormatter;
        protected final DateMathParser dateMathParser;
        protected final boolean coerce;

        public RangeFieldType(
            String name,
            RangeType type,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            boolean coerce,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            assert type != RangeType.DATE;
            this.rangeType = Objects.requireNonNull(type);
            dateTimeFormatter = null;
            dateMathParser = null;
            this.coerce = coerce;
        }

        public RangeFieldType(String name, RangeType type) {
            this(name, type, true, false, true, false, Collections.emptyMap());
        }

        public RangeFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            DateFormatter formatter,
            boolean coerce,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.rangeType = RangeType.DATE;
            this.dateTimeFormatter = Objects.requireNonNull(formatter);
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
            this.coerce = coerce;
        }

        public RangeFieldType(String name, DateFormatter formatter) {
            this(name, true, false, true, formatter, false, Collections.emptyMap());
        }

        public RangeType rangeType() {
            return rangeType;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new BinaryIndexFieldData.Builder(name(), CoreValuesSourceType.RANGE);
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

            return new SourceValueFetcher(name(), context) {

                @Override
                @SuppressWarnings("unchecked")
                protected Object parseSourceValue(Object value) {
                    RangeType rangeType = rangeType();
                    if ((value instanceof Map) == false) {
                        assert rangeType == RangeType.IP;
                        Tuple<InetAddress, Integer> ipRange = InetAddresses.parseCidr(value.toString());
                        return InetAddresses.toCidrString(ipRange.v1(), ipRange.v2());
                    }

                    Map<String, Object> range = (Map<String, Object>) value;
                    Map<String, Object> parsedRange = new HashMap<>();
                    for (Map.Entry<String, Object> entry : range.entrySet()) {
                        Object parsedValue = rangeType.parseValue(entry.getValue(), coerce, dateMathParser);
                        Object formattedValue = rangeType.formatValue(parsedValue, formatter);
                        parsedRange.put(entry.getKey(), formattedValue);
                    }
                    return parsedRange;
                }
            };
        }

        @Override
        public String typeName() {
            return rangeType.name;
        }

        public DateFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (rangeType == RangeType.DATE) {
                DateFormatter dateTimeFormatter = this.dateTimeFormatter;
                if (format != null) {
                    dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
                }
                if (timeZone == null) {
                    timeZone = ZoneOffset.UTC;
                }
                // the resolution here is always set to milliseconds, as aggregations use this formatter mainly and those are always in
                // milliseconds. The only special case here is docvalue fields, which are handled somewhere else
                return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, DateFieldMapper.Resolution.MILLISECONDS);
            }
            return super.docValueFormat(format, timeZone);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            SearchExecutionContext context
        ) {
            failIfNotIndexed();
            if (parser == null) {
                parser = dateMathParser();
            }
            return rangeType.rangeQuery(
                name(),
                hasDocValues(),
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                relation,
                timeZone,
                parser,
                context
            );
        }
    }

    private final RangeType type;
    private final boolean index;
    private final boolean hasDocValues;
    private final boolean store;
    private final Explicit<Boolean> coerce;
    private final String format;
    private final Locale locale;

    private final boolean coerceByDefault;

    private RangeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        RangeType type,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.type = type;
        this.index = builder.index.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.store = builder.store.getValue();
        this.coerce = builder.coerce.getValue();
        this.format = builder.format.getValue();
        this.locale = builder.locale.getValue();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
    }

    boolean coerce() {
        return coerce.value();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), type, coerceByDefault).init(this);
    }

    @Override
    public RangeFieldType fieldType() {
        return (RangeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }

        Range range = parseRange(parser);
        context.doc().addAll(fieldType().rangeType.createFields(context, fullPath(), range, index, hasDocValues, store));

        if (hasDocValues == false && (index || store)) {
            context.addToFieldNames(fieldType().name());
        }
    }

    private Range parseRange(XContentParser parser) throws IOException {
        final XContentParser.Token start = parser.currentToken();
        if (fieldType().rangeType == RangeType.IP && start == XContentParser.Token.VALUE_STRING) {
            return parseIpRangeFromCidr(parser);
        }

        if (start != XContentParser.Token.START_OBJECT) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], expected an object but got " + parser.currentName()
            );
        }

        RangeFieldType fieldType = fieldType();
        RangeType rangeType = fieldType.rangeType;
        String fieldName = null;
        Object parsedFrom = null;
        Object parsedTo = null;
        boolean includeFrom = DEFAULT_INCLUDE_LOWER;
        boolean includeTo = DEFAULT_INCLUDE_UPPER;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else {
                if (fieldName.equals(GT_FIELD.getPreferredName())) {
                    includeFrom = false;
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        parsedFrom = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                    }
                } else if (fieldName.equals(GTE_FIELD.getPreferredName())) {
                    includeFrom = true;
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        parsedFrom = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                    }
                } else if (fieldName.equals(LT_FIELD.getPreferredName())) {
                    includeTo = false;
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        parsedTo = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                    }
                } else if (fieldName.equals(LTE_FIELD.getPreferredName())) {
                    includeTo = true;
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        parsedTo = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                    }
                } else {
                    throw new DocumentParsingException(
                        parser.getTokenLocation(),
                        "error parsing field [" + fullPath() + "], with unknown parameter [" + fieldName + "]"
                    );
                }
            }
        }

        Object from = parsedFrom != null ? parsedFrom : rangeType.defaultFrom(includeFrom);
        Object to = parsedTo != null ? parsedTo : rangeType.defaultTo(includeTo);

        return new Range(rangeType, from, to, includeFrom, includeTo);
    }

    private static Range parseIpRangeFromCidr(final XContentParser parser) throws IOException {
        final InetAddresses.IpRange range = InetAddresses.parseIpRangeFromCidr(parser.text());
        return new Range(RangeType.IP, range.lowerBound(), range.upperBound(), true, true);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (hasDocValues) {
            var loader = new BinaryDocValuesSyntheticFieldLoader(fullPath()) {
                @Override
                protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                    List<Range> ranges = type.decodeRanges(value);

                    switch (ranges.size()) {
                        case 0:
                            return;
                        case 1:
                            b.field(leafName());
                            ranges.get(0).toXContent(b, fieldType().dateTimeFormatter);
                            break;
                        default:
                            b.startArray(leafName());
                            for (var range : ranges) {
                                range.toXContent(b, fieldType().dateTimeFormatter);
                            }
                            b.endArray();
                    }
                }
            };

            return new SyntheticSourceSupport.Native(loader);
        }

        return super.syntheticSourceSupport();
    }

    /** Class defining a range */
    public static class Range {
        RangeType type;
        Object from;
        Object to;
        private final boolean includeFrom;
        private final boolean includeTo;

        public Range(RangeType type, Object from, Object to, boolean includeFrom, boolean includeTo) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.includeFrom = includeFrom;
            this.includeTo = includeTo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return includeFrom == range.includeFrom
                && includeTo == range.includeTo
                && type == range.type
                && from.equals(range.from)
                && to.equals(range.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, from, to, includeFrom, includeTo);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(includeFrom ? '[' : '(');
            Object f = includeFrom || from.equals(type.minValue()) ? from : type.nextDown(from);
            Object t = includeTo || to.equals(type.maxValue()) ? to : type.nextUp(to);
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress) f) : f.toString());
            sb.append(" : ");
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress) t) : t.toString());
            sb.append(includeTo ? ']' : ')');
            return sb.toString();
        }

        public Object getFrom() {
            return from;
        }

        public Object getTo() {
            return to;
        }

        public XContentBuilder toXContent(XContentBuilder builder, DateFormatter dateFormatter) throws IOException {
            builder.startObject();

            // Default range bounds for double and float ranges
            // are infinities which are not valid inputs for range field.
            // As such it is not possible to specify them manually,
            // and they must come from defaults kicking in
            // when the bound is null or not present.
            // Therefore, range should be represented in that way in source too
            // to enable reindexing.
            //
            // We apply this logic to all range types for consistency.
            if (from.equals(type.minValue())) {
                assert includeFrom : "Range bounds were not properly adjusted during parsing";
                // Null value which will be parsed as a default
                builder.nullField("gte");
            } else {
                if (includeFrom) {
                    builder.field("gte");
                } else {
                    builder.field("gt");
                }
                var valueWithAdjustment = includeFrom ? from : type.nextDown(from);
                builder.value(type.formatValue(valueWithAdjustment, dateFormatter));
            }

            if (to.equals(type.maxValue())) {
                assert includeTo : "Range bounds were not properly adjusted during parsing";
                // Null value which will be parsed as a default
                builder.nullField("lte");
            } else {
                if (includeTo) {
                    builder.field("lte");
                } else {
                    builder.field("lt");
                }
                var valueWithAdjustment = includeTo ? to : type.nextUp(to);
                builder.value(type.formatValue(valueWithAdjustment, dateFormatter));
            }

            builder.endObject();

            return builder;
        }
    }

    static class BinaryRangesDocValuesField extends CustomDocValuesField {

        private final Set<Range> ranges;
        private final RangeType rangeType;

        BinaryRangesDocValuesField(String name, Range range, RangeType rangeType) {
            super(name);
            this.rangeType = rangeType;
            ranges = new HashSet<>();
            add(range);
        }

        void add(Range range) {
            ranges.add(range);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                return rangeType.encodeRanges(ranges);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to encode ranges", e);
            }
        }
    }
}
