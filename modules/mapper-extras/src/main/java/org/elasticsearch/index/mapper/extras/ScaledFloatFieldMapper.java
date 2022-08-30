/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedDoubleIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SortedNumericDocValuesSyntheticFieldLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ScaledFloatDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A {@link FieldMapper} for scaled floats. Values are internally multiplied
 *  by a scaling factor and rounded to the closest long. */
public class ScaledFloatFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "scaled_float";

    // use the same default as numbers
    private static final Setting<Boolean> COERCE_SETTING = NumberFieldMapper.COERCE_SETTING;

    private static ScaledFloatFieldMapper toType(FieldMapper in) {
        return (ScaledFloatFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Explicit<Boolean>> coerce;

        private final Parameter<Double> scalingFactor = new Parameter<>(
            "scaling_factor",
            false,
            () -> null,
            (n, c, o) -> XContentMapValues.nodeDoubleValue(o),
            m -> toType(m).scalingFactor,
            XContentBuilder::field,
            Objects::toString
        ).addValidator(v -> {
            if (v == null) {
                throw new IllegalArgumentException("Field [scaling_factor] is required");
            }
            if (Double.isFinite(v) == false || v <= 0) {
                throw new IllegalArgumentException("[scaling_factor] must be a positive number, got [" + v + "]");
            }
        });
        private final Parameter<Double> nullValue = new Parameter<>(
            "null_value",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeDoubleValue(o),
            m -> toType(m).nullValue,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull();

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * For the numeric fields gauge and counter metric types are
         * supported
         */
        private final Parameter<TimeSeriesParams.MetricType> metric;

        public Builder(String name, Settings settings) {
            this(name, IGNORE_MALFORMED_SETTING.get(settings), COERCE_SETTING.get(settings));
        }

        public Builder(String name, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);

            this.metric = TimeSeriesParams.metricParam(
                m -> toType(m).metricType,
                TimeSeriesParams.MetricType.gauge,
                TimeSeriesParams.MetricType.counter
            ).addValidator(v -> {
                if (v != null && hasDocValues.getValue() == false) {
                    throw new IllegalArgumentException(
                        "Field [" + TimeSeriesParams.TIME_SERIES_METRIC_PARAM + "] requires that [" + hasDocValues.name + "] is true"
                    );
                }
            });
        }

        Builder scalingFactor(double scalingFactor) {
            this.scalingFactor.setValue(scalingFactor);
            return this;
        }

        Builder nullValue(double nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder metric(TimeSeriesParams.MetricType metric) {
            this.metric.setValue(metric);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, stored, ignoreMalformed, meta, scalingFactor, coerce, nullValue, metric };
        }

        @Override
        public ScaledFloatFieldMapper build(MapperBuilderContext context) {
            ScaledFloatFieldType type = new ScaledFloatFieldType(
                context.buildFullName(name),
                indexed.getValue(),
                stored.getValue(),
                hasDocValues.getValue(),
                meta.getValue(),
                scalingFactor.getValue(),
                nullValue.getValue(),
                metric.getValue()
            );
            return new ScaledFloatFieldMapper(name, type, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getSettings()));

    public static final class ScaledFloatFieldType extends SimpleMappedFieldType {

        private final double scalingFactor;
        private final Double nullValue;
        private final TimeSeriesParams.MetricType metricType;

        public ScaledFloatFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Map<String, String> meta,
            double scalingFactor,
            Double nullValue,
            TimeSeriesParams.MetricType metricType
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.scalingFactor = scalingFactor;
            this.nullValue = nullValue;
            this.metricType = metricType;
        }

        public ScaledFloatFieldType(String name, double scalingFactor) {
            this(name, scalingFactor, true);
        }

        public ScaledFloatFieldType(String name, double scalingFactor, boolean indexed) {
            this(name, indexed, false, true, Collections.emptyMap(), scalingFactor, null, null);
        }

        public double getScalingFactor() {
            return scalingFactor;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return context.fieldExistsInIndex(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            long scaledValue = Math.round(scale(value));
            return NumberFieldMapper.NumberType.LONG.termQuery(name(), scaledValue, isIndexed());
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (isIndexed()) {
                List<Long> scaledValues = new ArrayList<>(values.size());
                for (Object value : values) {
                    long scaledValue = Math.round(scale(value));
                    scaledValues.add(scaledValue);
                }
                return NumberFieldMapper.NumberType.LONG.termsQuery(name(), Collections.unmodifiableList(scaledValues));
            } else {
                return super.termsQuery(values, context);
            }
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            Long lo = null;
            if (lowerTerm != null) {
                double dValue = scale(lowerTerm);
                if (includeLower == false) {
                    dValue = Math.nextUp(dValue);
                }
                lo = Math.round(Math.ceil(dValue));
            }
            Long hi = null;
            if (upperTerm != null) {
                double dValue = scale(upperTerm);
                if (includeUpper == false) {
                    dValue = Math.nextDown(dValue);
                }
                hi = Math.round(Math.floor(dValue));
            }
            return NumberFieldMapper.NumberType.LONG.rangeQuery(name(), lo, hi, true, true, hasDocValues(), context, isIndexed());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return (cache, breakerService) -> {
                    final IndexNumericFieldData scaledValues = new SortedNumericIndexFieldData.Builder(
                        name(),
                        IndexNumericFieldData.NumericType.LONG,
                        (dv, n) -> { throw new UnsupportedOperationException(); }
                    ).build(cache, breakerService);
                    return new ScaledFloatIndexFieldData(scaledValues, scalingFactor, ScaledFloatDocValuesField::new);
                };
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherSortedDoubleIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.NUMERIC,
                    sourceValueFetcher(sourcePaths),
                    searchLookup.source(),
                    ScaledFloatDocValuesField::new
                );
            }

            throw new IllegalStateException("unknown field data type [" + operation.name() + "]");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return sourceValueFetcher(context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet());
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths) {
            return new SourceValueFetcher(sourcePaths, nullValue) {
                @Override
                protected Double parseSourceValue(Object value) {
                    double doubleValue;
                    if (value.equals("")) {
                        if (nullValue == null) {
                            return null;
                        }
                        doubleValue = nullValue;
                    } else {
                        doubleValue = objectToDouble(value);
                    }

                    double factor = getScalingFactor();
                    return Math.round(doubleValue * factor) / factor;
                }
            };
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return ((Number) value).longValue() / scalingFactor;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            checkNoTimeZone(timeZone);
            if (format == null) {
                return DocValueFormat.RAW;
            }
            return new DocValueFormat.Decimal(format);
        }

        /**
         * Parses input value and multiplies it with the scaling factor.
         * Uses the round-trip of creating a {@link BigDecimal} from the stringified {@code double}
         * input to ensure intuitively exact floating point operations.
         * (e.g. for a scaling factor of 100, JVM behaviour results in {@code 79.99D * 100 ==> 7998.99..} compared to
         * {@code scale(79.99) ==> 7999})
         * @param input Input value to parse floating point num from
         * @return Scaled value
         */
        private double scale(Object input) {
            return new BigDecimal(Double.toString(parse(input))).multiply(BigDecimal.valueOf(scalingFactor)).doubleValue();
        }

        /**
         * If field is a time series metric field, returns its metric type
         * @return the metric type or null
         */
        public TimeSeriesParams.MetricType getMetricType() {
            return metricType;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("ScaledFloatFieldType[").append(scalingFactor);
            if (nullValue != null) {
                b.append(", nullValue=").append(nullValue);
                ;
            }
            if (metricType != null) {
                b.append(", metricType=").append(metricType);
            }
            return b.append("]").toString();
        }
    }

    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> coerce;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Double nullValue;
    private final double scalingFactor;

    private final boolean ignoreMalformedByDefault;
    private final boolean coerceByDefault;
    private final TimeSeriesParams.MetricType metricType;

    private ScaledFloatFieldMapper(
        String simpleName,
        ScaledFloatFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.stored = builder.stored.getValue();
        this.scalingFactor = builder.scalingFactor.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.coerce = builder.coerce.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
        this.metricType = builder.metric.getValue();
    }

    boolean coerce() {
        return coerce.value();
    }

    boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    public ScaledFloatFieldType fieldType() {
        return (ScaledFloatFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), ignoreMalformedByDefault, coerceByDefault).metric(metricType).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        Object value;
        Number numericValue = null;
        if (parser.currentToken() == Token.VALUE_NULL) {
            value = null;
        } else if (coerce.value() && parser.currentToken() == Token.VALUE_STRING && parser.textLength() == 0) {
            value = null;
        } else {
            try {
                numericValue = parse(parser, coerce.value());
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    return;
                } else {
                    throw e;
                }
            }
            value = numericValue;
        }

        if (value == null) {
            value = nullValue;
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = parse(value);
        }

        double doubleValue = numericValue.doubleValue();
        if (Double.isFinite(doubleValue) == false) {
            if (ignoreMalformed.value()) {
                return;
            } else {
                // since we encode to a long, we have no way to carry NaNs and infinities
                throw new IllegalArgumentException("[scaled_float] only supports finite values, but got [" + doubleValue + "]");
            }
        }
        long scaledValue = encode(doubleValue, scalingFactor);

        NumberFieldMapper.NumberType.LONG.addFields(context.doc(), fieldType().name(), scaledValue, indexed, hasDocValues, stored);

        if (hasDocValues == false && (indexed || stored)) {
            context.addToFieldNames(fieldType().name());
        }
    }

    static long encode(double value, double scalingFactor) {
        return Math.round(value * scalingFactor);
    }

    static Double parse(Object value) {
        return objectToDouble(value);
    }

    private static Double parse(XContentParser parser, boolean coerce) throws IOException {
        return parser.doubleValue(coerce);
    }

    /**
     * Converts an Object to a double by checking it against known types first
     */
    private static double objectToDouble(Object value) {
        double doubleValue;

        if (value instanceof Number) {
            doubleValue = ((Number) value).doubleValue();
        } else if (value instanceof BytesRef) {
            doubleValue = Double.parseDouble(((BytesRef) value).utf8ToString());
        } else {
            doubleValue = Double.parseDouble(value.toString());
        }

        return doubleValue;
    }

    private static class ScaledFloatIndexFieldData extends IndexNumericFieldData {

        private final IndexNumericFieldData scaledFieldData;
        private final double scalingFactor;
        private final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        ScaledFloatIndexFieldData(
            IndexNumericFieldData scaledFieldData,
            double scalingFactor,
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
        ) {
            this.scaledFieldData = scaledFieldData;
            this.scalingFactor = scalingFactor;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public String getFieldName() {
            return scaledFieldData.getFieldName();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return scaledFieldData.getValuesSourceType();
        }

        @Override
        public LeafNumericFieldData load(LeafReaderContext context) {
            return new ScaledFloatLeafFieldData(scaledFieldData.load(context), scalingFactor, toScriptFieldFactory);
        }

        @Override
        public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
            return new ScaledFloatLeafFieldData(scaledFieldData.loadDirect(context), scalingFactor, toScriptFieldFactory);
        }

        @Override
        protected boolean sortRequiresCustomComparator() {
            /*
             * We need to use a custom comparator because the non-custom
             * comparator wouldn't properly decode the long bits into the
             * double. Sorting on the long representation *would* put the
             * docs in order. We just don't have a way to convert the long
             * into a double the right way afterwords.
             */
            return true;
        }

        @Override
        public NumericType getNumericType() {
            /*
             * {@link ScaledFloatLeafFieldData#getDoubleValues()} transforms the raw long values in `scaled` floats.
             */
            return NumericType.DOUBLE;
        }

    }

    private static class ScaledFloatLeafFieldData implements LeafNumericFieldData {

        private final LeafNumericFieldData scaledFieldData;
        private final double scalingFactorInverse;
        private final ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory;

        ScaledFloatLeafFieldData(
            LeafNumericFieldData scaledFieldData,
            double scalingFactor,
            ToScriptFieldFactory<SortedNumericDoubleValues> toScriptFieldFactory
        ) {
            this.scaledFieldData = scaledFieldData;
            this.scalingFactorInverse = 1d / scalingFactor;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getDoubleValues(), name);
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return FieldData.toString(getDoubleValues());
        }

        @Override
        public long ramBytesUsed() {
            return scaledFieldData.ramBytesUsed();
        }

        @Override
        public void close() {
            scaledFieldData.close();
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return FieldData.castToLong(getDoubleValues());
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            final SortedNumericDocValues values = scaledFieldData.getLongValues();
            final NumericDocValues singleValues = DocValues.unwrapSingleton(values);
            if (singleValues != null) {
                return FieldData.singleton(new NumericDoubleValues() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return singleValues.advanceExact(doc);
                    }

                    @Override
                    public double doubleValue() throws IOException {
                        return singleValues.longValue() * scalingFactorInverse;
                    }
                });
            } else {
                return new SortedNumericDoubleValues() {

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return values.advanceExact(target);
                    }

                    @Override
                    public double nextValue() throws IOException {
                        return values.nextValue() * scalingFactorInverse;
                    }

                    @Override
                    public int docValueCount() {
                        return values.docValueCount();
                    }
                };
            }
        }

        @Override
        public FormattedDocValues getFormattedValues(DocValueFormat format) {
            SortedNumericDoubleValues values = getDoubleValues();
            return new FormattedDocValues() {
                @Override
                public boolean advanceExact(int docId) throws IOException {
                    return values.advanceExact(docId);
                }

                @Override
                public int docValueCount() throws IOException {
                    return values.docValueCount();
                }

                @Override
                public Object nextValue() throws IOException {
                    return format.format(values.nextValue());
                }
            };
        }
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (hasDocValues == false) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it doesn't have doc values"
            );
        }
        if (ignoreMalformed.value()) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it ignores malformed numbers"
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
                b.value(decodeForSyntheticSource(value, scalingFactor));
            }
        };
    }

    /**
     * Convert the scaled value back into it's {@code double} representation,
     * attempting to undo {@code Math.round(value * scalingFactor)}. It's
     * important that "round tripping" a value into the index, back out, and
     * then back in yields the same index. That's important because we use
     * the synthetic source produced by these this for reindex.
     * <p>
     * The tricky thing about undoing {@link Math#round} is that it
     * "saturates" to {@link Long#MAX_VALUE} when the {@code double} that
     * it's given is too large to fit into a {@code long}. And
     * {@link Long#MIN_VALUE} for {@code double}s that are too small. But
     * {@code Long.MAX_VALUE / scalingFactor} doesn't always yield a value
     * that would saturate. In other words, sometimes:
     * <pre>{@code
     *   long scaled1 = Math.round(BIG * scalingFactor);
     *   assert scaled1 == Long.MAX_VALUE;
     *   double decoded = scaled1 / scalingFactor;
     *   long scaled2 = Math.round(decoded * scalingFactor);
     *   assert scaled2 != Long.MAX_VALUE;
     * }</pre>
     * <p>
     * This can happen sometimes with regular old rounding too, in situations that
     * aren't entirely clear at the moment. We work around this by detecting when
     * the round trip wouldn't produce the same encoded value and artificially
     * bumping them up by a single digit in the last place towards the direction
     * that would make the round trip consistent. Bumping by a single digit in
     * the last place is always enough to correct the tiny errors that can sneak
     * in from the unexpected rounding.
     */
    static double decodeForSyntheticSource(long scaledValue, double scalingFactor) {
        double v = scaledValue / scalingFactor;
        long reenc = Math.round(v * scalingFactor);
        if (reenc != scaledValue) {
            if (reenc > scaledValue) {
                v -= Math.ulp(v);
            } else {
                v += Math.ulp(v);
            }
            assert Math.round(v * scalingFactor) == scaledValue : Math.round(v * scalingFactor) + " != " + scaledValue;
        }
        return v;
    }
}
