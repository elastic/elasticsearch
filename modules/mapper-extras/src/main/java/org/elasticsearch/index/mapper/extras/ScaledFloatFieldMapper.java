/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedDoubleIndexFieldData;
import org.elasticsearch.index.fielddata.plain.LeafDoubleFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.BlockDocValuesReader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SortedNumericDocValuesSyntheticFieldLoader;
import org.elasticsearch.index.mapper.SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer;
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
import org.elasticsearch.search.aggregations.support.TimeSeriesValuesSourceType;
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

import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;

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

        private final Parameter<Boolean> indexed;
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

        private final IndexMode indexMode;
        private final IndexVersion indexCreatedVersion;
        private final SourceKeepMode indexSourceKeepMode;

        public Builder(
            String name,
            Settings settings,
            IndexMode indexMode,
            IndexVersion indexCreatedVersion,
            SourceKeepMode indexSourceKeepMode
        ) {
            this(
                name,
                IGNORE_MALFORMED_SETTING.get(settings),
                COERCE_SETTING.get(settings),
                indexMode,
                indexCreatedVersion,
                indexSourceKeepMode
            );
        }

        public Builder(
            String name,
            boolean ignoreMalformedByDefault,
            boolean coerceByDefault,
            IndexMode indexMode,
            IndexVersion indexCreatedVersion,
            SourceKeepMode indexSourceKeepMode
        ) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            this.indexMode = indexMode;
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, () -> {
                if (indexMode == IndexMode.TIME_SERIES) {
                    var metricType = getMetric().getValue();
                    return metricType != TimeSeriesParams.MetricType.COUNTER && metricType != TimeSeriesParams.MetricType.GAUGE;
                } else {
                    return true;
                }
            });
            this.metric = TimeSeriesParams.metricParam(
                m -> toType(m).metricType,
                TimeSeriesParams.MetricType.GAUGE,
                TimeSeriesParams.MetricType.COUNTER
            ).addValidator(v -> {
                if (v != null && hasDocValues.getValue() == false) {
                    throw new IllegalArgumentException(
                        "Field [" + TimeSeriesParams.TIME_SERIES_METRIC_PARAM + "] requires that [" + hasDocValues.name + "] is true"
                    );
                }
            });
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexSourceKeepMode = indexSourceKeepMode;
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

        private Parameter<TimeSeriesParams.MetricType> getMetric() {
            return metric;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, stored, ignoreMalformed, meta, scalingFactor, coerce, nullValue, metric };
        }

        @Override
        public ScaledFloatFieldMapper build(MapperBuilderContext context) {
            ScaledFloatFieldType type = new ScaledFloatFieldType(
                context.buildFullName(leafName()),
                indexed.getValue(),
                stored.getValue(),
                hasDocValues.getValue(),
                meta.getValue(),
                scalingFactor.getValue(),
                nullValue.getValue(),
                metric.getValue(),
                indexMode,
                coerce.getValue().value(),
                context.isSourceSynthetic()
            );
            String offsetsFieldName = getOffsetsFieldName(
                context,
                indexSourceKeepMode,
                hasDocValues.getValue(),
                stored.getValue(),
                this,
                indexCreatedVersion,
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_SCALED_FLOAT
            );
            return new ScaledFloatFieldMapper(
                leafName(),
                type,
                builderParams(this, context),
                context.isSourceSynthetic(),
                this,
                offsetsFieldName
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            c.getSettings(),
            c.getIndexSettings().getMode(),
            c.indexVersionCreated(),
            c.getIndexSettings().sourceKeepMode()
        )
    );

    public static final class ScaledFloatFieldType extends SimpleMappedFieldType {

        private final double scalingFactor;
        private final Double nullValue;
        private final TimeSeriesParams.MetricType metricType;
        private final IndexMode indexMode;
        private final boolean coerce;
        private final boolean isSyntheticSource;

        public ScaledFloatFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Map<String, String> meta,
            double scalingFactor,
            Double nullValue,
            TimeSeriesParams.MetricType metricType,
            IndexMode indexMode,
            boolean coerce,
            boolean isSyntheticSource
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.scalingFactor = scalingFactor;
            this.nullValue = nullValue;
            this.metricType = metricType;
            this.indexMode = indexMode;
            this.coerce = coerce;
            this.isSyntheticSource = isSyntheticSource;
        }

        public ScaledFloatFieldType(String name, double scalingFactor) {
            this(name, scalingFactor, true);
        }

        public ScaledFloatFieldType(String name, double scalingFactor, boolean indexed) {
            this(name, indexed, false, true, Collections.emptyMap(), scalingFactor, null, null, null, false, false);
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
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            long scaledValue = Math.round(scale(value));
            return NumberFieldMapper.NumberType.LONG.termQuery(name(), scaledValue, isIndexed(), hasDocValues());
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
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (indexMode == IndexMode.TIME_SERIES && metricType == TimeSeriesParams.MetricType.COUNTER) {
                // Counters are not supported by ESQL so we load them in null
                return BlockLoader.CONSTANT_NULLS;
            }
            if (hasDocValues() && (blContext.fieldExtractPreference() != FieldExtractPreference.STORED || isSyntheticSource)) {
                return new BlockDocValuesReader.DoublesBlockLoader(name(), l -> l / scalingFactor);
            }
            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSource && blContext.parentField(name()) == null) {
                return new FallbackSyntheticSourceBlockLoader(fallbackSyntheticSourceBlockLoaderReader(), name()) {
                    @Override
                    public Builder builder(BlockFactory factory, int expectedCount) {
                        return factory.doubles(expectedCount);
                    }
                };
            }

            ValueFetcher valueFetcher = sourceValueFetcher(blContext.sourcePaths(name()));
            BlockSourceReader.LeafIteratorLookup lookup = hasDocValues() == false && (isStored() || isIndexed())
                // We only write the field names field if there aren't doc values or norms
                ? BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name())
                : BlockSourceReader.lookupMatchingAll();
            return new BlockSourceReader.DoublesBlockLoader(valueFetcher, lookup);
        }

        private FallbackSyntheticSourceBlockLoader.Reader<?> fallbackSyntheticSourceBlockLoaderReader() {
            var nullValueAdjusted = nullValue != null ? adjustSourceValue(nullValue, scalingFactor) : null;

            return new FallbackSyntheticSourceBlockLoader.SingleValueReader<Double>(nullValue) {
                @Override
                public void convertValue(Object value, List<Double> accumulator) {
                    if (coerce && value.equals("")) {
                        if (nullValueAdjusted != null) {
                            accumulator.add(nullValueAdjusted);
                        }
                    }

                    try {
                        // Convert to doc_values format
                        var converted = adjustSourceValue(NumberFieldMapper.NumberType.objectToDouble(value), scalingFactor);
                        accumulator.add(converted);
                    } catch (Exception e) {
                        // Malformed value, skip it
                    }
                }

                @Override
                protected void parseNonNullValue(XContentParser parser, List<Double> accumulator) throws IOException {
                    // Aligned with implementation of `parseCreateField(XContentParser)`
                    if (coerce && parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0) {
                        if (nullValueAdjusted != null) {
                            accumulator.add(nullValueAdjusted);
                        }
                    }

                    try {
                        double value = parser.doubleValue(coerce);
                        // Convert to doc_values format
                        var converted = adjustSourceValue(value, scalingFactor);
                        accumulator.add(converted);
                    } catch (Exception e) {
                        // Malformed value, skip it
                    }
                }

                @Override
                public void writeToBlock(List<Double> values, BlockLoader.Builder blockBuilder) {
                    var longBuilder = (BlockLoader.DoubleBuilder) blockBuilder;

                    for (var value : values) {
                        longBuilder.appendDouble(value);
                    }
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            ValuesSourceType valuesSourceType = indexMode == IndexMode.TIME_SERIES && metricType == TimeSeriesParams.MetricType.COUNTER
                ? TimeSeriesValuesSourceType.COUNTER
                : IndexNumericFieldData.NumericType.LONG.getValuesSourceType();
            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return (cache, breakerService) -> {
                    final IndexNumericFieldData scaledValues = new SortedNumericIndexFieldData.Builder(
                        name(),
                        IndexNumericFieldData.NumericType.LONG,
                        valuesSourceType,
                        (dv, n) -> {
                            throw new UnsupportedOperationException();
                        },
                        isIndexed()
                    ).build(cache, breakerService);
                    return new ScaledFloatIndexFieldData(scaledValues, scalingFactor, ScaledFloatDocValuesField::new);
                };
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherSortedDoubleIndexFieldData.Builder(
                    name(),
                    valuesSourceType,
                    sourceValueFetcher(sourcePaths),
                    searchLookup,
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
                        doubleValue = NumberFieldMapper.NumberType.objectToDouble(value);
                    }

                    return adjustSourceValue(doubleValue, getScalingFactor());
                }
            };
        }

        // Adjusts precision of a double value so that it looks like it came from doc_values.
        private static Double adjustSourceValue(double value, double scalingFactor) {
            return Math.round(value * scalingFactor) / scalingFactor;
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
            return new BigDecimal(Double.toString(NumberFieldMapper.NumberType.objectToDouble(input))).multiply(
                BigDecimal.valueOf(scalingFactor)
            ).doubleValue();
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
    private final boolean isSourceSynthetic;

    private final boolean ignoreMalformedByDefault;
    private final boolean coerceByDefault;
    private final TimeSeriesParams.MetricType metricType;
    private final IndexMode indexMode;

    private final IndexVersion indexCreatedVersion;
    private final String offsetsFieldName;
    private final SourceKeepMode indexSourceKeepMode;

    private ScaledFloatFieldMapper(
        String simpleName,
        ScaledFloatFieldType mappedFieldType,
        BuilderParams builderParams,
        boolean isSourceSynthetic,
        Builder builder,
        String offsetsFieldName
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.isSourceSynthetic = isSourceSynthetic;
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
        this.indexMode = builder.indexMode;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.offsetsFieldName = offsetsFieldName;
        this.indexSourceKeepMode = builder.indexSourceKeepMode;
    }

    boolean coerce() {
        return coerce.value();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
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
        return new Builder(leafName(), ignoreMalformedByDefault, coerceByDefault, indexMode, indexCreatedVersion, indexSourceKeepMode)
            .metric(metricType)
            .init(this);
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
                numericValue = parser.doubleValue(coerce.value());
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    context.addIgnoredField(mappedFieldType.name());
                    if (isSourceSynthetic) {
                        // Save a copy of the field so synthetic source can load it
                        context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), context.parser()));
                    }
                    return;
                } else {
                    throw e;
                }
            }
            value = numericValue;
        }

        boolean shouldStoreOffsets = offsetsFieldName != null && context.isImmediateParentAnArray() && context.canAddIgnoredField();

        if (value == null) {
            value = nullValue;
        }

        if (value == null) {
            if (shouldStoreOffsets) {
                context.getOffSetContext().recordNull(offsetsFieldName);
            }
            return;
        }

        if (numericValue == null) {
            numericValue = NumberFieldMapper.NumberType.objectToDouble(value);
        }

        double doubleValue = numericValue.doubleValue();
        if (Double.isFinite(doubleValue) == false) {
            if (ignoreMalformed.value()) {
                context.addIgnoredField(mappedFieldType.name());
                if (isSourceSynthetic) {
                    // Save a copy of the field so synthetic source can load it
                    context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), context.parser()));
                }
                return;
            } else {
                // since we encode to a long, we have no way to carry NaNs and infinities
                throw new IllegalArgumentException("[scaled_float] only supports finite values, but got [" + doubleValue + "]");
            }
        }
        long scaledValue = encode(doubleValue, scalingFactor);

        NumberFieldMapper.NumberType.LONG.addFields(context.doc(), fieldType().name(), scaledValue, indexed, hasDocValues, stored);

        if (shouldStoreOffsets) {
            context.getOffSetContext().recordOffset(offsetsFieldName, scaledValue);
        }

        if (hasDocValues == false && (indexed || stored)) {
            context.addToFieldNames(fieldType().name());
        }
    }

    static long encode(double value, double scalingFactor) {
        return Math.round(value * scalingFactor);
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
        protected boolean isIndexed() {
            return false; // We don't know how to take advantage of the index with half floats anyway
        }

        @Override
        public NumericType getNumericType() {
            /*
             * {@link ScaledFloatLeafFieldData#getDoubleValues()} transforms the raw long values in `scaled` floats.
             */
            return NumericType.DOUBLE;
        }

    }

    private static class ScaledFloatLeafFieldData extends LeafDoubleFieldData {

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
        public long ramBytesUsed() {
            return scaledFieldData.ramBytesUsed();
        }

        @Override
        public void close() {
            scaledFieldData.close();
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
    }

    private SourceLoader.SyntheticFieldLoader docValuesSyntheticFieldLoader() {
        if (offsetsFieldName != null) {
            var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
            layers.add(
                new SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer(
                    fullPath(),
                    offsetsFieldName,
                    (b, value) -> b.value(decodeForSyntheticSource(value, scalingFactor))
                )
            );
            if (ignoreMalformed.value()) {
                layers.add(new CompositeSyntheticFieldLoader.MalformedValuesLayer(fullPath()));
            }
            return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
        } else {
            return new SortedNumericDocValuesSyntheticFieldLoader(fullPath(), leafName(), ignoreMalformed.value()) {
                @Override
                protected void writeValue(XContentBuilder b, long value) throws IOException {
                    b.value(decodeForSyntheticSource(value, scalingFactor));
                }
            };
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (hasDocValues) {
            return new SyntheticSourceSupport.Native(this::docValuesSyntheticFieldLoader);
        }

        return super.syntheticSourceSupport();
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

        // If original double value is close to MAX_VALUE
        // and rounding is performed in the direction of the same infinity
        // it is possible to "overshoot" infinity during reconstruction.
        // E.g. for a value close to Double.MAX_VALUE "true" scaled value is 10.5
        // and with rounding it becomes 11.
        // Now, because of that rounding difference, 11 divided by scaling factor goes into infinity.
        // There is nothing we can do about it so we'll return the closest finite value to infinity
        // which is MAX_VALUE.
        if (Double.isInfinite(v)) {
            var sign = v == Double.POSITIVE_INFINITY ? 1 : -1;
            return sign * Double.MAX_VALUE;
        }

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
