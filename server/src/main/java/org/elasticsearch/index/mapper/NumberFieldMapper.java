/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedDoubleIndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxDoublesFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxIntsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinDoublesFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinIntsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.RoundToLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.SortedNumericDocValuesRangeQuery;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.ByteDocValuesField;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.FloatDocValuesField;
import org.elasticsearch.script.field.HalfFloatDocValuesField;
import org.elasticsearch.script.field.IntegerDocValuesField;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.script.field.ShortDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.TimeSeriesValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;
import static org.elasticsearch.index.mapper.FieldMapper.Parameter.useTimeSeriesDocValuesSkippers;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    public static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    private static NumberFieldMapper toType(FieldMapper in) {
        return (NumberFieldMapper) in;
    }

    private static DocValuesParameter.Values defaultDocValuesParameters(IndexSettings indexSettings) {
        if (indexSettings.getMode().isStrictColumnar() == false) {
            return new DocValuesParameter.Values(
                true,
                DocValuesParameter.Values.Cardinality.LOW,
                true,
                true,
                DocValuesParameter.Values.OnFailure.FAIL
            );
        }

        boolean multiValue = FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.get(indexSettings.getSettings());
        boolean nullability = FieldMapper.DOC_VALUES_NULLABILITY_SETTING.get(indexSettings.getSettings());
        var onFailure = FieldMapper.resolveOnFailureSetting(indexSettings.getSettings());
        return new DocValuesParameter.Values(true, DocValuesParameter.Values.Cardinality.LOW, multiValue, nullability, onFailure);
    }

    /**
     * Encodes an integer into the term used by the {@code index_terms} inverted index, such that
     * unsigned byte-wise (lexicographic) term order matches numeric order.
     */
    static BytesRef encodeIndexTerm(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, bytes, 0);
        return new BytesRef(bytes);
    }

    /**
     * Lucene field type combining a sortable-bytes inverted index (for {@code index_terms})
     * with sorted numeric doc values (for the original integer value).
     */
    private static final FieldType INDEX_TERMS_FIELD_TYPE;

    /**
     * Lucene field type with a sortable-bytes inverted index only (for {@code index_terms}, no doc values).
     */
    private static final FieldType INDEX_TERMS_FIELD_TYPE_INDEX_ONLY;

    static {
        INDEX_TERMS_FIELD_TYPE = new FieldType();
        INDEX_TERMS_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        INDEX_TERMS_FIELD_TYPE.setOmitNorms(true);
        INDEX_TERMS_FIELD_TYPE.setTokenized(false);
        INDEX_TERMS_FIELD_TYPE.freeze();

        INDEX_TERMS_FIELD_TYPE_INDEX_ONLY = new FieldType();
        INDEX_TERMS_FIELD_TYPE_INDEX_ONLY.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_FIELD_TYPE_INDEX_ONLY.setOmitNorms(true);
        INDEX_TERMS_FIELD_TYPE_INDEX_ONLY.setTokenized(false);
        INDEX_TERMS_FIELD_TYPE_INDEX_ONLY.freeze();
    }

    /**
     * A Lucene field that stores a sortable-bytes term in the inverted index and the original
     * integer value in sorted numeric doc values, using a single consistent field type.
     */
    private static class IndexTermsIntegerField extends Field {
        private final int numericVal;

        IndexTermsIntegerField(String name, int value, BytesRef term) {
            super(name, term, INDEX_TERMS_FIELD_TYPE);
            this.numericVal = value;
        }

        @Override
        public Number numericValue() {
            return numericVal;
        }
    }

    public static final class Builder extends FieldMapper.DimensionBuilder {

        private final Parameter<Boolean> indexed;
        private final DocValuesParameter docValuesParameters;
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Explicit<Boolean>> coerce;

        private final Parameter<Number> nullValue;

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<OnScriptError> onScriptErrorParam = Parameter.onScriptErrorParam(
            m -> toType(m).builderParams.onScriptError(),
            script
        );

        /**
         * Parameter that marks this field as a time series dimension.
         */
        private final Parameter<Boolean> dimension;

        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * For the numeric fields gauge and counter metric types are
         * supported
         */
        private final Parameter<MetricType> metric;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<Boolean> indexTerms;

        private final ScriptCompiler scriptCompiler;
        private final NumberType type;

        public NumberType type() {
            return type;
        }

        private boolean allowMultipleValues = true;
        private final IndexSettings indexSettings;

        public static Builder docValuesOnly(String name, NumberType type, IndexSettings indexSettings) {
            Builder builder = new Builder(name, type, ScriptCompiler.NONE, indexSettings);
            builder.indexed.setValue(false);
            builder.dimension.setValue(false);
            return builder;
        }

        public Builder(String name, NumberType type, ScriptCompiler compiler, IndexSettings indexSettings) {
            super(name);
            this.type = type;
            this.scriptCompiler = Objects.requireNonNull(compiler);
            this.indexSettings = Objects.requireNonNull(indexSettings);
            this.docValuesParameters = DocValuesParameter.of(
                defaultDocValuesParameters(indexSettings),
                m -> toType(m).docValuesParameters(),
                indexSettings.getMode().isStrictColumnar()
            );

            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                IGNORE_MALFORMED_SETTING.get(indexSettings.getSettings())
            );
            this.coerce = Parameter.explicitBoolParam(
                "coerce",
                true,
                m -> toType(m).coerce,
                COERCE_SETTING.get(indexSettings.getSettings())
            );
            this.nullValue = new Parameter<>(
                "null_value",
                false,
                () -> null,
                (n, c, o) -> o == null ? null : type.parse(o, false),
                m -> toType(m).nullValue,
                XContentBuilder::field,
                Objects::toString
            ).acceptsNull();
            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).dimension, () -> docValuesParameters.get().enabled());
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, () -> {
                if (indexSettings.isIndexDisabledByDefault()) {
                    return false;
                }

                if (useTimeSeriesDocValuesSkippers(indexSettings, dimension.get())) {
                    return false;
                }
                if (indexSettings.getMode().isTsdb()) {
                    var metricType = getMetric().getValue();
                    return metricType != MetricType.COUNTER && metricType != MetricType.GAUGE;
                } else {
                    return true;
                }
            });

            this.metric = TimeSeriesParams.metricParam(m -> toType(m).metricType, MetricType.GAUGE, MetricType.COUNTER).addValidator(v -> {
                if (v != null && docValuesParameters.getValue().enabled() == false) {
                    throw new IllegalArgumentException(
                        "Field [" + TimeSeriesParams.TIME_SERIES_METRIC_PARAM + "] requires that [" + docValuesParameters.name + "] is true"
                    );
                }
            }).precludesParameters(dimension);

            this.indexTerms = Parameter.boolParam("index_terms", false, m -> toType(m).indexTerms, false).addValidator(v -> {
                if (v) {
                    if (type != NumberType.INTEGER) {
                        throw new IllegalArgumentException("[index_terms] is only supported on [integer] fields");
                    }
                    if (indexed.getValue() == false) {
                        throw new IllegalArgumentException("[index_terms] requires that [index] is true");
                    }
                    if (indexSettings.getIndexVersionCreated().isLegacyIndexVersion()) {
                        // Legacy/archive indices fall back to IndexType.archivedPoints() regardless of
                        // index_terms, and their Lucene segments predate this feature and are never
                        // reindexed, so they can never actually contain the sortable-bytes terms field.
                        throw new IllegalArgumentException("[index_terms] is not supported on legacy indices");
                    }
                }
            });

            this.script.precludesParameters(ignoreMalformed, coerce, nullValue);
            addScriptValidation(script, indexed, () -> docValuesParameters.getValue().enabled());
        }

        Builder nullValue(Number number) {
            this.nullValue.setValue(number);
            return this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed.setValue(new Explicit<>(ignoreMalformed, true));
            return this;
        }

        public Builder coerce(boolean coerce) {
            this.coerce.setValue(new Explicit<>(coerce, true));
            return this;
        }

        public Builder index(boolean index) {
            this.indexed.setValue(index);
            return this;
        }

        @Deprecated
        public Builder docValues(boolean hasDocValues) {
            this.docValuesParameters.setValue(
                hasDocValues ? defaultDocValuesParameters(indexSettings) : DocValuesParameter.Values.DISABLED
            );
            return this;
        }

        private IndexType indexType() {
            if (indexSettings.getIndexVersionCreated().isLegacyIndexVersion()) {
                return IndexType.archivedPoints();
            }
            if (indexTerms.getValue()) {
                assert indexed.get() : "[index_terms] requires [index] to be true";
                return IndexType.terms(indexed.get(), docValuesParameters.get().enabled());
            }
            if (indexed.get() == false && docValuesParameters.get().enabled()) {
                if (useTimeSeriesDocValuesSkippers(indexSettings, dimension.get())) {
                    return IndexType.skippers();
                }
                if (indexSettings.useDocValuesSkipper()
                    && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS)) {
                    return IndexType.skippers();
                }
            }
            return IndexType.points(indexed.get(), docValuesParameters.get().enabled());
        }

        private FieldValues<Number> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            return type.compile(leafName(), script.get(), scriptCompiler);
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        public Builder metric(MetricType metric) {
            this.metric.setValue(metric);
            return this;
        }

        private Parameter<MetricType> getMetric() {
            return metric;
        }

        public Builder allowMultipleValues(boolean allowMultipleValues) {
            this.allowMultipleValues = allowMultipleValues;
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                indexed,
                docValuesParameters,
                stored,
                ignoreMalformed,
                coerce,
                nullValue,
                script,
                onScriptErrorParam,
                meta,
                dimension,
                metric,
                indexTerms };
        }

        @Override
        public String contentType() {
            return type.typeName();
        }

        private String offsetsFieldName;

        @Override
        public NumberFieldMapper build(MapperBuilderContext context) {
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension.setValue(true);
            }

            hasScript = script.get() != null;
            onScriptError = onScriptErrorParam.getValue();
            this.offsetsFieldName = getOffsetsFieldName(
                context,
                indexSettings.sourceKeepMode(),
                docValuesParameters.getValue().enabled(),
                stored.getValue(),
                this,
                indexSettings.getIndexVersionCreated(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_NUMBER,
                indexSettings.getMode().isStrictColumnar(),
                docValuesParameters.getValue().multiValue()
            );
            MappedFieldType ft = new NumberFieldType(context.buildFullName(leafName()), this, context.isSourceSynthetic());

            return new NumberFieldMapper(leafName(), ft, builderParams(this, context), context.isSourceSynthetic(), this, offsetsFieldName);
        }
    }

    public enum NumberType {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result = parseToFloat(value);
                validateFiniteValue(result);
                // Reduce the precision to what we actually index
                return HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(result));
            }

            @Override
            public double reduceToStoredPrecision(double value) {
                return parse(value, false).doubleValue();
            }

            /**
             * Parse a query parameter or {@code _source} value to a float,
             * keeping float precision. Used by queries which do need to validate
             * against infinite values, but need more precise control over their
             * rounding behavior that {@link #parse(Object, boolean)} provides.
             */
            private static float parseToFloat(Object value) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                return result;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return HalfFloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateFiniteValue(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                float v = parseToFloat(value);
                if (Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(v))) == false) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] is out of range");
                }

                if (indexType.hasPoints()) {
                    if (indexType.hasDocValues()) {
                        long sv = HalfFloatPoint.halfFloatToSortableShort(v);
                        return new IndexOrDocValuesQuery(
                            HalfFloatPoint.newExactQuery(field, v),
                            SortedNumericDocValuesRangeQuery.newRangeQuery(field, sv, sv)
                        );
                    }
                    return HalfFloatPoint.newExactQuery(field, v);
                } else {
                    long sv = HalfFloatPoint.halfFloatToSortableShort(v);
                    return SortedNumericDocValuesRangeQuery.newRangeQuery(field, sv, sv);
                }
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                float[] v = new float[values.size()];
                int pos = 0;
                for (Object value : values) {
                    float float_value = parseToFloat(value);
                    validateFiniteValue(float_value);
                    v[pos++] = float_value;
                }
                return HalfFloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parseToFloat(lowerTerm);
                    if (includeLower) {
                        l = HalfFloatPoint.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parseToFloat(upperTerm);
                    if (includeUpper) {
                        u = HalfFloatPoint.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                Query query;
                if (hasPoints) {
                    query = HalfFloatPoint.newRangeQuery(field, l, u);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(
                            field,
                            HalfFloatPoint.halfFloatToSortableShort(l),
                            HalfFloatPoint.halfFloatToSortableShort(u)
                        );
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                } else {
                    query = SortedNumericDocValuesRangeQuery.newRangeQuery(
                        field,
                        HalfFloatPoint.halfFloatToSortableShort(l),
                        HalfFloatPoint.halfFloatToSortableShort(u)
                    );
                }
                return query;
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                final float f = value.floatValue();
                if (indexType.hasPoints()) {
                    document.add(new HalfFloatPoint(name, f));
                }
                if (indexType.hasDocValues()) {
                    dvFactory.addNumericField(document, name, HalfFloatPoint.halfFloatToSortableShort(f));
                }
                if (stored) {
                    document.add(new StoredField(name, f));
                }
            }

            @Override
            public long toSortableLong(Number value) {
                return HalfFloatPoint.halfFloatToSortableShort(value.floatValue());
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedDoublesIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    HalfFloatDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedDoubleIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    HalfFloatDocValuesField::new
                );
            }

            private static void validateFiniteValue(float value) {
                if (Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value))) == false) {
                    throw new IllegalArgumentException("[half_float] supports only finite values, but got [" + value + "]");
                }
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(HalfFloatPoint.sortableShortToHalfFloat((short) value));
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new DoublesBlockLoader(fieldName, l -> HalfFloatPoint.sortableShortToHalfFloat((short) l), readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.DoublesBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return floatingPointBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinDoublesFromDocValuesBlockLoader(fieldName, l -> HalfFloatPoint.sortableShortToHalfFloat((short) l));
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxDoublesFromDocValuesBlockLoader(fieldName, l -> HalfFloatPoint.sortableShortToHalfFloat((short) l));
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for half_float");
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result = parseToFloat(value);
                validateFiniteValue(result);
                return result;
            }

            /**
             * Parse a query parameter or {@code _source} value to a float,
             * keeping float precision. Used by queries which do need validate
             * against infinite values like {@link #parse(Object, boolean)} does.
             */
            private static float parseToFloat(Object value) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                return result;
            }

            @Override
            public double reduceToStoredPrecision(double value) {
                return parse(value, false).doubleValue();
            }

            @Override
            public Number parsePoint(byte[] value) {
                return FloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateFiniteValue(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                float v = parseToFloat(value);
                if (Float.isFinite(v) == false) {
                    return new MatchNoDocsQuery("Value [" + value + "] is out of range");
                }
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    return FloatField.newExactQuery(field, v);
                } else if (indexType.hasPoints()) {
                    return FloatPoint.newExactQuery(field, v);
                } else {
                    long sv = NumericUtils.floatToSortableInt(v);
                    return SortedNumericDocValuesRangeQuery.newRangeQuery(field, sv, sv);
                }
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                float[] v = new float[values.size()];
                int pos = 0;
                for (Object value : values) {
                    v[pos++] = parse(value, false);
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parseToFloat(lowerTerm);
                    if (includeLower) {
                        l = FloatPoint.nextDown(l);
                    }
                    l = FloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parseToFloat(upperTerm);
                    if (includeUpper) {
                        u = FloatPoint.nextUp(u);
                    }
                    u = FloatPoint.nextDown(u);
                }
                Query query;
                if (hasPoints) {
                    query = FloatPoint.newRangeQuery(field, l, u);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(
                            field,
                            NumericUtils.floatToSortableInt(l),
                            NumericUtils.floatToSortableInt(u)
                        );
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                } else {
                    query = SortedNumericDocValuesRangeQuery.newRangeQuery(
                        field,
                        NumericUtils.floatToSortableInt(l),
                        NumericUtils.floatToSortableInt(u)
                    );
                }
                return query;
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                final float f = value.floatValue();
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    document.add(new FloatField(name, f, Field.Store.NO));
                } else if (indexType.hasDocValues()) {
                    dvFactory.addNumericField(document, name, NumericUtils.floatToSortableInt(f));
                } else if (indexType.hasPoints()) {
                    document.add(new FloatPoint(name, f));
                }
                if (stored) {
                    document.add(new StoredField(name, f));
                }
            }

            @Override
            public long toSortableLong(Number value) {
                return NumericUtils.floatToSortableInt(value.floatValue());
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedDoublesIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    FloatDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedDoubleIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    FloatDocValuesField::new
                );
            }

            private static void validateFiniteValue(float value) {
                if (Float.isFinite(value) == false) {
                    throw new IllegalArgumentException("[float] supports only finite values, but got [" + value + "]");
                }
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(NumericUtils.sortableIntToFloat((int) value));
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new DoublesBlockLoader(fieldName, l -> NumericUtils.sortableIntToFloat((int) l), readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.DoublesBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return floatingPointBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinDoublesFromDocValuesBlockLoader(fieldName, l -> NumericUtils.sortableIntToFloat((int) l));
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxDoublesFromDocValuesBlockLoader(fieldName, l -> NumericUtils.sortableIntToFloat((int) l));
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for float");
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            public Double parse(Object value, boolean coerce) {
                double parsed = objectToDouble(value);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return DoublePoint.decodeDimension(value, 0);
            }

            @Override
            public Double parse(XContentParser parser, boolean coerce) throws IOException {
                double parsed = parser.doubleValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
                DoubleFieldScript.Factory scriptFactory = compiler.compile(script, DoubleFieldScript.CONTEXT);
                return (lookup, ctx, doc, consumer) -> scriptFactory.newFactory(fieldName, script.getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer::accept);
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                double v = objectToDouble(value);
                if (Double.isFinite(v) == false) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    return DoubleField.newExactQuery(field, v);
                } else if (indexType.hasPoints()) {
                    return DoublePoint.newExactQuery(field, v);
                } else {
                    long sv = NumericUtils.doubleToSortableLong(v);
                    return SortedNumericDocValuesRangeQuery.newRangeQuery(field, sv, sv);
                }
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                double[] v = values.stream().mapToDouble(value -> parse(value, false)).toArray();
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                return doubleRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    Query query;
                    if (hasPoints) {
                        query = DoublePoint.newRangeQuery(field, l, u);
                        if (hasDocValues) {
                            Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(
                                field,
                                NumericUtils.doubleToSortableLong(l),
                                NumericUtils.doubleToSortableLong(u)
                            );
                            query = new IndexOrDocValuesQuery(query, dvQuery);
                        }
                    } else {
                        query = SortedNumericDocValuesRangeQuery.newRangeQuery(
                            field,
                            NumericUtils.doubleToSortableLong(l),
                            NumericUtils.doubleToSortableLong(u)
                        );
                    }
                    return query;
                });
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                final double d = value.doubleValue();
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    document.add(new DoubleField(name, d, Field.Store.NO));
                } else if (indexType.hasDocValues()) {
                    dvFactory.addNumericField(document, name, NumericUtils.doubleToSortableLong(d));
                } else if (indexType.hasPoints()) {
                    document.add(new DoublePoint(name, d));
                }
                if (stored) {
                    document.add(new StoredField(name, d));
                }
            }

            @Override
            public long toSortableLong(Number value) {
                return NumericUtils.doubleToSortableLong(value.doubleValue());
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedDoublesIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    DoubleDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedDoubleIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    DoubleDocValuesField::new
                );
            }

            private static void validateParsed(double value) {
                if (Double.isFinite(value) == false) {
                    throw new IllegalArgumentException("[double] supports only finite values, but got [" + value + "]");
                }
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(NumericUtils.sortableLongToDouble(value));
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new DoublesBlockLoader(fieldName, NumericUtils::sortableLongToDouble, readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.DoublesBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return floatingPointBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinDoublesFromDocValuesBlockLoader(fieldName, NumericUtils::sortableLongToDouble);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxDoublesFromDocValuesBlockLoader(fieldName, NumericUtils::sortableLongToDouble);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for double");
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            public Byte parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }

                return (byte) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).byteValue();
            }

            @Override
            public Byte parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                return (byte) value;
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                if (isOutOfRange(value)) {
                    return new MatchNoDocsQuery("Value [" + value + "] is out of range");
                }

                return INTEGER.termQuery(field, value, indexType);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                return INTEGER.rangeQuery(
                    field,
                    lowerTerm,
                    upperTerm,
                    includeLower,
                    includeUpper,
                    hasDocValues,
                    context,
                    hasPoints,
                    hasTerms
                );
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                INTEGER.addFields(document, name, value, indexType, stored, dvFactory);
            }

            @Override
            public long toSortableLong(Number value) {
                return INTEGER.toSortableLong(value);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedNumericIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    ByteDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedNumericIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    ByteDocValuesField::new
                );
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(value);
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new IntsBlockLoader(fieldName, readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.IntsBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return integerBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for byte");
            }

            private boolean isOutOfRange(Object value) {
                double doubleValue = objectToDouble(value);
                return doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE;
            }
        },
        SHORT("short", NumericType.SHORT) {
            @Override
            public Short parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }

                return (short) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).shortValue();
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.shortValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                if (isOutOfRange(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] is out of range");
                }
                return INTEGER.termQuery(field, value, indexType);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                return INTEGER.rangeQuery(
                    field,
                    lowerTerm,
                    upperTerm,
                    includeLower,
                    includeUpper,
                    hasDocValues,
                    context,
                    hasPoints,
                    hasTerms
                );
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                INTEGER.addFields(document, name, value, indexType, stored, dvFactory);
            }

            @Override
            public long toSortableLong(Number value) {
                return INTEGER.toSortableLong(value);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedNumericIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    ShortDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedNumericIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    ShortDocValuesField::new
                );
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(value);
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new IntsBlockLoader(fieldName, readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.IntsBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return integerBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for short");
            }

            private boolean isOutOfRange(Object value) {
                double doubleValue = objectToDouble(value);
                return doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE;
            }
        },
        INTEGER("integer", NumericType.INT) {
            @Override
            public Integer parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (isOutOfRange(doubleValue)) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return (int) doubleValue;
            }

            private boolean isOutOfRange(double value) {
                return value < Integer.MIN_VALUE || value > Integer.MAX_VALUE;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return IntPoint.decodeDimension(value, 0);
            }

            @Override
            public Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                double doubleValue = objectToDouble(value);

                if (isOutOfRange(doubleValue)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] is out of range");
                }
                int v = parse(value, true);
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    return IntField.newExactQuery(field, v);
                } else if (indexType.hasPoints()) {
                    return IntPoint.newExactQuery(field, v);
                } else {
                    return SortedNumericDocValuesRangeQuery.newRangeQuery(field, v, v);
                }
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                int[] v = new int[values.size()];
                int upTo = 0;

                for (Object value : values) {
                    if (hasDecimalPart(value) == false) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return IntPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                int l = Integer.MIN_VALUE;
                int u = Integer.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Integer.MAX_VALUE) {
                            return Queries.NO_DOCS_INSTANCE;
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Integer.MIN_VALUE) {
                            return Queries.NO_DOCS_INSTANCE;
                        }
                        --u;
                    }
                }
                Query query;
                if (hasTerms) {
                    // index_terms has no BKD points, so range queries go through the terms
                    // dictionary directly instead of falling back to doc values -- this also
                    // means range queries work even when doc_values is disabled.
                    query = new TermRangeQuery(field, encodeIndexTerm(l), encodeIndexTerm(u), true, true);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(field, l, u);
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                } else if (hasPoints) {
                    query = IntPoint.newRangeQuery(field, l, u);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(field, l, u);
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                } else {
                    query = SortedNumericDocValuesRangeQuery.newRangeQuery(field, l, u);
                }
                if (hasDocValues && context.indexSortedOnField(field)) {
                    query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                }
                return query;
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                final int i = value.intValue();
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    document.add(new IntField(name, i, Field.Store.NO));
                } else if (indexType.hasDocValues()) {
                    dvFactory.addNumericField(document, name, i);
                } else if (indexType.hasPoints()) {
                    document.add(new IntPoint(name, i));
                }
                if (stored) {
                    document.add(new StoredField(name, i));
                }
            }

            @Override
            public long toSortableLong(Number value) {
                return value.intValue();
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedNumericIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    IntegerDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedNumericIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    IntegerDocValuesField::new
                );
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(value);
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new IntsBlockLoader(fieldName, readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.IntsBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                return integerBlockLoaderFromFallbackSyntheticSource(this, fieldName, nullValue, coerce, blContext);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxIntsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                throw new UnsupportedOperationException("ROUND_TO not supported for integer");
            }
        },
        LONG("long", NumericType.LONG) {
            @Override
            public Long parse(Object value, boolean coerce) {
                return objectToLong(value, coerce);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            public Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
                final LongFieldScript.Factory scriptFactory = compiler.compile(script, LongFieldScript.CONTEXT);
                return (lookup, ctx, doc, consumer) -> scriptFactory.newFactory(fieldName, script.getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer::accept);
            }

            @Override
            public Query termQuery(String field, Object value, IndexType indexType) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                if (isOutOfRange(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] is out of range");
                }

                long v = parse(value, true);
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    return LongField.newExactQuery(field, v);
                } else if (indexType.hasPoints()) {
                    return LongPoint.newExactQuery(field, v);
                } else {
                    return SortedNumericDocValuesRangeQuery.newRangeQuery(field, v, v);
                }
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                long[] v = new long[values.size()];
                int upTo = 0;

                for (Object value : values) {
                    if (hasDecimalPart(value) == false) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return LongPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                SearchExecutionContext context,
                boolean hasPoints,
                boolean hasTerms
            ) {
                return longRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    Query query;
                    if (hasPoints) {
                        query = LongPoint.newRangeQuery(field, l, u);
                        if (hasDocValues) {
                            Query dvQuery = SortedNumericDocValuesRangeQuery.newRangeQuery(field, l, u);
                            query = new IndexOrDocValuesQuery(query, dvQuery);
                        }
                    } else {
                        query = SortedNumericDocValuesRangeQuery.newRangeQuery(field, l, u);
                    }
                    if (hasDocValues && context.indexSortedOnField(field)) {
                        query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                    }
                    return query;
                });
            }

            @Override
            public void addFields(
                LuceneDocument document,
                String name,
                Number value,
                IndexType indexType,
                boolean stored,
                DocValuesFieldFactory dvFactory
            ) {
                final long l = value.longValue();
                if (indexType.hasPoints() && indexType.hasDocValues()) {
                    document.add(new LongField(name, l, Field.Store.NO));
                } else if (indexType.hasDocValues()) {
                    dvFactory.addNumericField(document, name, l);
                } else if (indexType.hasPoints()) {
                    document.add(new LongPoint(name, l));
                }
                if (stored) {
                    document.add(new StoredField(name, l));
                }
            }

            @Override
            public long toSortableLong(Number value) {
                return value.longValue();
            }

            @Override
            public IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
                return new SortedNumericIndexFieldData.Builder(
                    ft.name(),
                    numericType(),
                    valuesSourceType,
                    LongDocValuesField::new,
                    ft.indexType
                );
            }

            @Override
            public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
                String name,
                ValuesSourceType valuesSourceType,
                SourceProvider sourceProvider,
                ValueFetcher valueFetcher
            ) {
                return new SourceValueFetcherSortedNumericIndexFieldData.Builder(
                    name,
                    valuesSourceType,
                    valueFetcher,
                    sourceProvider,
                    LongDocValuesField::new
                );
            }

            @Override
            public void writeValue(XContentBuilder b, long value) throws IOException {
                b.value(value);
            }

            @Override
            BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder) {
                return new LongsBlockLoader(fieldName, readInArrayOrder);
            }

            @Override
            BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup) {
                return new BlockSourceReader.LongsBlockLoader(sourceValueFetcher, lookup);
            }

            @Override
            BlockLoader blockLoaderFromFallbackSyntheticSource(
                String fieldName,
                Number nullValue,
                boolean coerce,
                MappedFieldType.BlockLoaderContext blContext
            ) {
                var reader = new NumberFallbackSyntheticSourceReader(this, nullValue, coerce) {
                    @Override
                    public void writeToBlock(List<Number> values, BlockLoader.Builder blockBuilder) {
                        var builder = (BlockLoader.LongBuilder) blockBuilder;
                        for (var value : values) {
                            builder.appendLong(value.longValue());
                        }
                    }
                };

                return new FallbackSyntheticSourceBlockLoader(
                    reader,
                    fieldName,
                    IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings())
                ) {
                    @Override
                    public Builder builder(BlockFactory factory, int expectedCount) {
                        return factory.longs(expectedCount);
                    }
                };
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMin(String fieldName) {
                return new MvMinLongsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesMvMax(String fieldName) {
                return new MvMaxLongsFromDocValuesBlockLoader(fieldName);
            }

            @Override
            BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points) {
                return new RoundToLongsFromDocValuesBlockLoader(fieldName, points);
            }

            private boolean isOutOfRange(Object value) {
                if (value instanceof Long) {
                    return false;
                }
                String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
                BigDecimal bigDecimalValue = new BigDecimal(stringValue);
                return bigDecimalValue.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0
                    || bigDecimalValue.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0;
            }
        };

        private final String name;
        private final NumericType numericType;
        private final TypeParser parser;

        NumberType(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
            this.parser = createTypeParserWithLegacySupport((n, c) -> new Builder(n, this, c.scriptCompiler(), c.getIndexSettings()));
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }

        /** Get the associated numeric type */
        public final NumericType numericType() {
            return numericType;
        }

        public final TypeParser parser() {
            return parser;
        }

        public abstract Query termQuery(String field, Object value, IndexType indexType);

        public abstract Query termsQuery(String field, Collection<?> values);

        public abstract Query rangeQuery(
            String field,
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            boolean hasDocValues,
            SearchExecutionContext context,
            boolean hasPoints,
            boolean hasTerms
        );

        public abstract Number parse(XContentParser parser, boolean coerce) throws IOException;

        public abstract Number parse(Object value, boolean coerce);

        public abstract Number parsePoint(byte[] value);

        /**
         * Maps the given {@code value} to one or more Lucene field values ands them to the given {@code document} under the given
         * {@code name}. Delegates to {@link #addFields(LuceneDocument, String, Number, IndexType, boolean, DocValuesFieldFactory)}
         * with a multi-valued factory — kept so test callers and other non-mapper callers don't need to thread a
         * {@code DocValuesFieldFactory} through when they only care about the default multi-valued behavior.
         */
        public final void addFields(LuceneDocument document, String name, Number value, IndexType indexType, boolean stored) {
            addFields(
                document,
                name,
                value,
                indexType,
                stored,
                new DocValuesFieldFactory(true, indexType.hasDocValuesSkipper(), IndexVersion.current())
            );
        }

        /**
         * @param document document to add fields to
         * @param name field name
         * @param value value to map
         * @param indexType an IndexType describing the index structures to be added
         * @param stored whether or not the field is stored
         * @param dvFactory factory responsible for creating doc values fields
         */
        public abstract void addFields(
            LuceneDocument document,
            String name,
            Number value,
            IndexType indexType,
            boolean stored,
            DocValuesFieldFactory dvFactory
        );

        /**
         * For a given {@code Number}, returns the sortable long representation that will be stored in the doc values.
         * @param value number to convert
         * @return sortable long representation
         */
        public abstract long toSortableLong(Number value);

        public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
            // only implemented for long and double fields
            throw new IllegalArgumentException("Unknown parameter [script] for mapper [" + fieldName + "]");
        }

        Number valueForSearch(Number value) {
            return value;
        }

        /**
         * Returns true if the object is a number and has a decimal part
         */
        public static boolean hasDecimalPart(Object number) {
            if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
                return false;
            }
            if (number instanceof Number) {
                double doubleValue = ((Number) number).doubleValue();
                return doubleValue % 1 != 0;
            }
            if (number instanceof BytesRef) {
                number = ((BytesRef) number).utf8ToString();
            }
            if (number instanceof String) {
                return Double.parseDouble((String) number) % 1 != 0;
            }
            return false;
        }

        /**
         * Returns -1, 0, or 1 if the value is lower than, equal to, or greater than 0
         */
        static double signum(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return Math.signum(doubleValue);
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Math.signum(Double.parseDouble(value.toString()));
        }

        /**
         * Converts an Object to a double by checking it against known types first
         */
        public static double objectToDouble(Object value) {
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

        /**
         * Converts an Object to a {@code long} by checking it against known
         * types and checking its range.
         */
        public static long objectToLong(Object value, boolean coerce) {
            if (value instanceof Long) {
                return (Long) value;
            }

            double doubleValue = objectToDouble(value);
            // this check does not guarantee that value is inside MIN_VALUE/MAX_VALUE because values up to 9223372036854776832 will
            // be equal to Long.MAX_VALUE after conversion to double. More checks ahead.
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
            }
            if (coerce == false && doubleValue % 1 != 0) {
                throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
            }

            // longs need special handling so we don't lose precision while parsing
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            return Numbers.toLong(stringValue, coerce);
        }

        public static Query doubleRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Double, Double, Query> builder
        ) {
            double l = Double.NEGATIVE_INFINITY;
            double u = Double.POSITIVE_INFINITY;
            if (lowerTerm != null) {
                l = objectToDouble(lowerTerm);
                if (includeLower == false) {
                    l = DoublePoint.nextUp(l);
                }
            }
            if (upperTerm != null) {
                u = objectToDouble(upperTerm);
                if (includeUpper == false) {
                    u = DoublePoint.nextDown(u);
                }
            }
            return builder.apply(l, u);
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query longRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Long, Long, Query> builder
        ) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = objectToLong(lowerTerm, true);
                // if the lower bound is decimal:
                // - if the bound is positive then we increment it:
                // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                // - if the bound is negative then we leave it as is:
                // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                    if (l == Long.MAX_VALUE) {
                        return Queries.NO_DOCS_INSTANCE;
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = objectToLong(upperTerm, true);
                boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                    if (u == Long.MIN_VALUE) {
                        return Queries.NO_DOCS_INSTANCE;
                    }
                    --u;
                }
            }
            return builder.apply(l, u);
        }

        public abstract IndexFieldData.Builder getFieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType);

        public IndexFieldData.Builder getValueFetcherFieldDataBuilder(
            String name,
            ValuesSourceType valuesSourceType,
            SourceProvider sourceProvider,
            ValueFetcher valueFetcher
        ) {
            throw new UnsupportedOperationException("not supported for source fallback");
        }

        /**
         * Adjusts a value to the value it would have been had it been parsed by that mapper
         * and then cast up to a double. This is meant to be an entry point to manipulate values
         * before the actual value is parsed.
         *
         * @param value the value to reduce to the field stored value
         * @return the double value
         */
        public double reduceToStoredPrecision(double value) {
            return ((Number) value).doubleValue();
        }

        abstract void writeValue(XContentBuilder builder, long longValue) throws IOException;

        SourceLoader.SyntheticFieldLoader syntheticFieldLoader(
            String fieldName,
            String fieldSimpleName,
            boolean ignoreMalformed,
            IndexVersion indexVersion
        ) {
            var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
            layers.add(new SortedNumericDocValuesSyntheticFieldLoaderLayer(fieldName, NumberType.this::writeValue));
            if (ignoreMalformed) {
                layers.add(CompositeSyntheticFieldLoader.malformedValuesLayer(fieldName, indexVersion));
            }
            return new CompositeSyntheticFieldLoader(fieldSimpleName, fieldName, layers);
        }

        abstract BlockLoader blockLoaderFromDocValues(String fieldName, boolean readInArrayOrder);

        abstract BlockLoader blockLoaderFromSource(SourceValueFetcher sourceValueFetcher, BlockSourceReader.LeafIteratorLookup lookup);

        abstract BlockLoader blockLoaderFromFallbackSyntheticSource(
            String fieldName,
            Number nullValue,
            boolean coerce,
            MappedFieldType.BlockLoaderContext blContext
        );

        abstract BlockLoader blockLoaderFromDocValuesMvMin(String fieldName);

        abstract BlockLoader blockLoaderFromDocValuesMvMax(String fieldName);

        abstract BlockLoader blockLoaderFromDocValuesRoundTo(String fieldName, long[] points);

        // All values that fit into integer are returned as integers
        private static BlockLoader integerBlockLoaderFromFallbackSyntheticSource(
            NumberType type,
            String fieldName,
            Number nullValue,
            boolean coerce,
            MappedFieldType.BlockLoaderContext blContext
        ) {
            var reader = new NumberFallbackSyntheticSourceReader(type, nullValue, coerce) {
                @Override
                public void writeToBlock(List<Number> values, BlockLoader.Builder blockBuilder) {
                    var builder = (BlockLoader.IntBuilder) blockBuilder;
                    for (var value : values) {
                        builder.appendInt(value.intValue());
                    }
                }
            };

            return new FallbackSyntheticSourceBlockLoader(
                reader,
                fieldName,
                IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings())
            ) {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.ints(expectedCount);
                }
            };
        }

        // All floating point values are returned as doubles
        private static BlockLoader floatingPointBlockLoaderFromFallbackSyntheticSource(
            NumberType type,
            String fieldName,
            Number nullValue,
            boolean coerce,
            MappedFieldType.BlockLoaderContext blContext
        ) {
            var reader = new NumberFallbackSyntheticSourceReader(type, nullValue, coerce) {
                @Override
                public void writeToBlock(List<Number> values, BlockLoader.Builder blockBuilder) {
                    var builder = (BlockLoader.DoubleBuilder) blockBuilder;
                    for (var value : values) {
                        builder.appendDouble(value.doubleValue());
                    }
                }
            };

            return new FallbackSyntheticSourceBlockLoader(
                reader,
                fieldName,
                IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings())
            ) {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.doubles(expectedCount);
                }
            };
        }

        abstract static class NumberFallbackSyntheticSourceReader extends FallbackSyntheticSourceBlockLoader.SingleValueReader<Number> {
            private final NumberType type;
            private final Number nullValue;
            private final boolean coerce;

            NumberFallbackSyntheticSourceReader(NumberType type, Number nullValue, boolean coerce) {
                super(nullValue);
                this.type = type;
                this.nullValue = nullValue;
                this.coerce = coerce;
            }

            @Override
            public void convertValue(Object value, List<Number> accumulator) {
                if (coerce && value.equals("")) {
                    if (nullValue != null) {
                        accumulator.add(nullValue);
                    }
                }

                try {
                    var converted = type.parse(value, coerce);
                    accumulator.add(converted);
                } catch (Exception e) {
                    // Malformed value, skip it
                }
            }

            @Override
            public void parseNonNullValue(XContentParser parser, List<Number> accumulator) throws IOException {
                // Aligned with implementation of `value(XContentParser)`
                if (coerce && parser.currentToken() == Token.VALUE_STRING && parser.textLength() == 0) {
                    if (nullValue != null) {
                        accumulator.add(nullValue);
                    }
                }

                try {
                    Number rawValue = type.parse(parser, coerce);
                    // Transform number to correct type (e.g. reduce precision)
                    accumulator.add(type.parse(rawValue, coerce));
                } catch (Exception e) {
                    // Malformed value, skip it
                }
            }
        }
    }

    public static class NumberFieldType extends SimpleMappedFieldType {

        private final NumberType type;
        private final boolean coerce;
        private final Number nullValue;
        private final FieldValues<Number> scriptValues;
        private final boolean isDimension;
        private final MetricType metricType;
        private final IndexMode indexMode;
        private final boolean isSyntheticSource;
        private final boolean readInArrayOrder;
        private final boolean indexTerms;

        public NumberFieldType(
            String name,
            NumberType type,
            IndexType indexType,
            boolean isStored,
            boolean coerce,
            Number nullValue,
            Map<String, String> meta,
            FieldValues<Number> script,
            boolean isDimension,
            MetricType metricType,
            IndexMode indexMode,
            boolean isSyntheticSource,
            boolean readInArrayOrder,
            boolean indexTerms
        ) {
            super(name, indexType, isStored, meta);
            this.type = Objects.requireNonNull(type);
            this.coerce = coerce;
            this.nullValue = nullValue;
            this.scriptValues = script;
            this.isDimension = isDimension;
            this.metricType = metricType;
            this.indexMode = indexMode;
            this.isSyntheticSource = isSyntheticSource;
            this.readInArrayOrder = readInArrayOrder;
            this.indexTerms = indexTerms;
        }

        NumberFieldType(String name, Builder builder, boolean isSyntheticSource) {
            this(
                name,
                builder.type,
                builder.indexType(),
                builder.stored.getValue(),
                builder.coerce.getValue().value(),
                builder.nullValue.getValue(),
                builder.meta.getValue(),
                builder.scriptValues(),
                builder.dimension.getValue(),
                builder.metric.getValue(),
                builder.indexSettings.getMode(),
                isSyntheticSource,
                builder.offsetsFieldName != null
                    && builder.docValuesParameters.getValue().multiValue()
                    && builder.indexSettings.getMode().isStrictColumnar(),
                builder.indexTerms.getValue()
            );
        }

        public NumberFieldType(String name, NumberType type) {
            this(name, type, true);
        }

        public NumberFieldType(String name, NumberType type, boolean isIndexed) {
            this(
                name,
                type,
                IndexType.points(isIndexed, true),
                false,
                true,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null,
                false,
                false,
                false
            );
        }

        public NumberFieldType(String name, NumberType type, boolean isIndexed, boolean hasDocValues) {
            this(
                name,
                type,
                IndexType.points(isIndexed, hasDocValues),
                false,
                true,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null,
                false,
                false,
                false
            );
        }

        @Override
        public String typeName() {
            return type.name;
        }

        @Override
        public TextSearchInfo getTextSearchInfo() {
            return TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS;
        }

        /**
         * This method reinterprets a double precision value based on the maximum precision of the stored number field.  Mostly this
         * corrects for unrepresentable values which have different approximations when cast from floats than when parsed as doubles.
         * It may seem strange to convert a double to a double, and it is.  This function's goal is to reduce the precision
         * on the double in the case that the backing number type would have parsed the value differently.  This is to address
         * the problem where (e.g.) 0.04F &lt; 0.04D, which causes problems for range aggregations.
         */
        public double reduceToStoredPrecision(double value) {
            if (Double.isInfinite(value)) {
                // Trying to parse infinite values into ints/longs throws. Understandably.
                return value;
            }
            return type.reduceToStoredPrecision(value);
        }

        public NumericType numericType() {
            return type.numericType();
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return context.fieldExistsInIndex(this.name());
        }

        public boolean isSearchable() {
            return indexType.hasPoints() || indexType.hasTerms() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexTerms) {
                assert indexType.hasTerms() : "[index_terms] requires a terms index";
                BytesRef term = indexTermOrNull(value);
                if (term == null) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] cannot match an [index_terms] integer field");
                }
                return new TermQuery(new Term(name(), term));
            }
            return type.termQuery(name(), value, indexType);
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexTerms) {
                assert indexType.hasTerms() : "[index_terms] requires a terms index";
                List<BytesRef> terms = new ArrayList<>(values.size());
                for (Object value : values) {
                    BytesRef term = indexTermOrNull(value);
                    if (term != null) {
                        terms.add(term);
                    }
                }
                if (terms.isEmpty()) {
                    return Queries.newMatchNoDocsQuery("No values can match an [index_terms] integer field");
                }
                return new TermInSetQuery(name(), terms);
            }
            if (indexType.hasPoints()) {
                return type.termsQuery(name(), values);
            } else {
                return super.termsQuery(values, context);
            }
        }

        /** Returns the numeric type of this field. */
        public NumberType numberType() {
            return type;
        }

        /** Returns {@code true} if this field is indexed with BKD points. */
        public boolean isIndexedWithPoints() {
            return indexType.hasPoints();
        }

        /** Returns {@code true} if this field uses an inverted index for terms ({@code index_terms: true}). */
        public boolean isIndexedWithTerms() {
            return indexType.hasTerms();
        }

        /**
         * Maps a query value to the sortable-bytes term used by an {@code index_terms} integer
         * field, or returns {@code null} if the value cannot match any document. Byte, Short,
         * Integer and Long values are handled directly, since none of them can carry a decimal
         * part; other types (Double, Float, BigDecimal, String, BytesRef, ...) are checked for a
         * decimal part and range, since a value with a fractional part or outside the integer
         * range can never match a document and is excluded rather than raising an error.
         */
        private BytesRef indexTermOrNull(Object value) {
            if (value instanceof Integer i) {
                return encodeIndexTerm(i);
            }
            if (value instanceof Byte b) {
                return encodeIndexTerm(b);
            }
            if (value instanceof Short s) {
                return encodeIndexTerm(s);
            }
            if (value instanceof Long l) {
                return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? encodeIndexTerm(l.intValue()) : null;
            }
            double doubleValue = NumberType.objectToDouble(value);
            if (doubleValue % 1 != 0 || doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                return null;
            }
            return encodeIndexTerm((int) doubleValue);
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
            return type.rangeQuery(
                name(),
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                hasDocValues(),
                context,
                indexType.hasPoints(),
                indexType.hasTerms()
            );
        }

        @Override
        public Function<byte[], Number> pointReaderIfPossible() {
            if (indexType.hasPoints()) {
                return this::parsePoint;
            }
            return null;
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (hasDocValues() && (blContext.fieldExtractPreference() != FieldExtractPreference.STORED || isSyntheticSource)) {
                BlockLoaderFunctionConfig cfg = blContext.blockLoaderFunctionConfig();
                if (cfg == null) {
                    return type.blockLoaderFromDocValues(name(), readInArrayOrder);
                }
                return switch (cfg.function()) {
                    case MV_MAX -> type.blockLoaderFromDocValuesMvMax(name());
                    case MV_MIN -> type.blockLoaderFromDocValuesMvMin(name());
                    case AMD_COUNT, AMD_DEFAULT, AMD_MAX, AMD_MIN, AMD_SUM -> type.blockLoaderFromDocValues(name(), false);
                    case ROUND_TO -> type.blockLoaderFromDocValuesRoundTo(name(), ((BlockLoaderFunctionConfig.RoundToLongs) cfg).points());
                    default -> throw new UnsupportedOperationException("unknown fusion config [" + cfg.function() + "]");
                };
            }
            if (blContext.blockLoaderFunctionConfig() != null) {
                throw new UnsupportedOperationException("function fusing only supported for doc values");
            }
            // columnar_stored pre-builds _source as a single blob; skip the per-field fallback loader.
            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSource && blContext.mappingLookup().isSourceColumnarStored() == false && blContext.parentField(name()) == null) {
                return type.blockLoaderFromFallbackSyntheticSource(name(), nullValue, coerce, blContext);
            }

            BlockSourceReader.LeafIteratorLookup lookup = hasDocValues() == false && (isStored() || indexType.hasPoints())
                // We only write the field names field if there aren't doc values or norms
                ? BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name())
                : BlockSourceReader.lookupMatchingAll();
            return type.blockLoaderFromSource(sourceValueFetcher(blContext.sourcePaths(name()), blContext.indexSettings()), lookup);
        }

        @Override
        public boolean supportsBlockLoaderConfig(BlockLoaderFunctionConfig config, FieldExtractPreference preference) {
            if (hasDocValues() && (preference != FieldExtractPreference.STORED || isSyntheticSource)) {
                return switch (config.function()) {
                    case AMD_MIN, AMD_MAX, AMD_SUM, AMD_COUNT, AMD_DEFAULT, MV_MAX, MV_MIN -> true;
                    case ROUND_TO -> type == NumberType.LONG && indexType.hasDocValuesSkipper();
                    default -> false;
                };
            }
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (fieldDataContext.fielddataOperation() == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            ValuesSourceType valuesSourceType = IndexMode.isTsdb(indexMode) && metricType == TimeSeriesParams.MetricType.COUNTER
                ? TimeSeriesValuesSourceType.COUNTER
                : type.numericType.getValuesSourceType();

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return type.getFieldDataBuilder(this, valuesSourceType);
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());
                return type.getValueFetcherFieldDataBuilder(
                    name(),
                    valuesSourceType,
                    searchLookup,
                    sourceValueFetcher(sourcePaths, fieldDataContext.indexSettings())
                );
            }

            throw new IllegalStateException("unknown field data type [" + operation.name() + "]");
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return type.valueForSearch((Number) value);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (this.scriptValues != null) {
                return FieldValues.valueFetcher(this.scriptValues, context);
            }
            return sourceValueFetcher(
                context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet(),
                context.getIndexSettings()
            );
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths, IndexSettings indexSettings) {
            return new SourceValueFetcher(sourcePaths, nullValue, indexSettings.getIgnoredSourceFormat()) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if (value.equals("")) {
                        return nullValue;
                    }
                    return type.parse(value, coerce);
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            checkNoTimeZone(timeZone);
            if (format == null) {
                return DocValueFormat.RAW;
            }
            return new DocValueFormat.Decimal(format);
        }

        public Number parsePoint(byte[] value) {
            return type.parsePoint(value);
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.NUMERIC;
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public boolean hasScriptValues() {
            return scriptValues != null;
        }

        /**
         * If field is a time series metric field, returns its metric type
         * @return the metric type or null
         */
        public MetricType getMetricType() {
            return metricType;
        }
    }

    private final NumberType type;
    private final boolean indexed;
    private final DocValuesParameter.Values docValuesParameters;
    private final boolean stored;
    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> coerce;
    private final Number nullValue;
    private final FieldValues<Number> scriptValues;
    private final boolean dimension;
    private final boolean writeDimensionRouting;
    private final ScriptCompiler scriptCompiler;
    private final Script script;
    private final MetricType metricType;
    private boolean allowMultipleValues;
    private final boolean isSyntheticSource;
    private final String offsetsFieldName;
    private final boolean indexTerms;

    private final IndexSettings indexSettings;

    // cached to avoid lookup overhead
    private final NumberFieldType fieldType;

    private final DocValuesFieldFactory dvFactory;

    private NumberFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        boolean isSyntheticSource,
        Builder builder,
        String offsetsFieldName
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.type = builder.type;
        this.indexed = builder.indexed.getValue();
        this.docValuesParameters = builder.docValuesParameters.getValue();
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.coerce = builder.coerce.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.scriptValues = builder.scriptValues();
        this.dimension = builder.dimension.getValue();
        this.writeDimensionRouting = this.dimension
            && builder.indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource efs
            && efs.extractDimensionsWhileMapping();
        this.scriptCompiler = builder.scriptCompiler;
        this.script = builder.script.getValue();
        this.metricType = builder.metric.getValue();
        this.allowMultipleValues = builder.allowMultipleValues;
        this.isSyntheticSource = isSyntheticSource;
        this.offsetsFieldName = offsetsFieldName;
        this.indexSettings = builder.indexSettings;
        this.indexTerms = builder.indexTerms.getValue();

        // this is basically just an inlined version of `(NumberFieldType) super.fieldType()`
        this.fieldType = (NumberFieldType) mappedFieldType;
        this.dvFactory = new DocValuesFieldFactory(
            docValuesParameters.multiValue(),
            fieldType.indexType.hasDocValuesSkipper(),
            builder.indexSettings.getIndexVersionCreated()
        );
    }

    boolean coerce() {
        return coerce.value();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public DocValuesParameter.Values docValuesParameters() {
        return docValuesParameters;
    }

    @Override
    protected boolean isSingleValueEnforced() {
        return allowMultipleValues == false || docValuesParameters.multiValue() == false;
    }

    @Override
    protected DocValuesParameter.Values.OnFailure onFailureBehavior() {
        // allowMultipleValues==false is a structural invariant of the field type (ie. aggregate_metric_double's metric sub-fields, which
        // must always be single-valued), not the user-configurable doc_values.multi_value constraint, so it must always fail regardless
        // of on_failure.
        if (allowMultipleValues == false) {
            return DocValuesParameter.Values.OnFailure.FAIL;
        }
        return docValuesParameters.onFailure();
    }

    @Override
    public boolean isNullable() {
        return docValuesParameters.nullability() || nullValue != null;
    }

    @Override
    public NumberFieldType fieldType() {
        return fieldType;
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
    }

    public NumberType type() {
        return type;
    }

    @Override
    protected String contentType() {
        return fieldType.type.typeName();
    }

    @Override
    public boolean supportsBatchIndexing() {
        // Plain number mappers can be driven through parseCreateField by the bulk batch path.
        // ignore_malformed is allowed — parseCreateField handles it and only needs
        // addIgnoredField on the context. Dimensions, copy_to, multi-fields, and scripts pull
        // in behavior that the v1 batch path does not support.
        return hasScript() == false
            && copyTo().copyToFields().isEmpty()
            && multiFields().iterator().hasNext() == false
            && dimension == false;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        Number value;
        try {
            value = value(context.parser());
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed.value() && context.parser().currentToken().isValue()) {
                context.addIgnoredField(mappedFieldType.name());
                if (isSyntheticSource) {
                    // Save a copy of the field so synthetic source can load it
                    IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fullPath(), context.parser());
                }
                return;
            } else {
                throw e;
            }
        }
        if (value != null) {
            indexValue(context, value);
        } else {
            value = fieldType.nullValue;
        }
        if (FieldArrayContext.shouldRecordOffsets(context, offsetsFieldName, docValuesParameters.multiValue())) {
            if (value != null) {
                // We cannot simply cast value to Comparable<> because we need to also capture the potential loss of precision that occurs
                // when the value is stored into the doc values.
                long sortableLongValue = type.toSortableLong(value);
                context.getOffSetContext().recordOffset(offsetsFieldName, sortableLongValue);
            } else {
                context.getOffSetContext().recordNull(offsetsFieldName);
            }
        }

    }

    /**
     * Read the value at the current position of the parser. For numeric fields
     * this is called by {@link #parseCreateField} but it is public so it can
     * be used by other fields that want to share the behavior of numeric fields.
     * @throws IllegalArgumentException if there was an error parsing the value from the json
     * @throws IOException if there was any other IO error
     */
    public Number value(XContentParser parser) throws IllegalArgumentException, IOException {
        final Token currentToken = parser.currentToken();
        if (currentToken == Token.VALUE_NULL) {
            return nullValue;
        }
        final boolean coerce = coerce();
        if (coerce && currentToken == Token.VALUE_STRING && parser.textLength() == 0) {
            return nullValue;
        }
        if (currentToken == Token.START_OBJECT) {
            throw new IllegalArgumentException("Cannot parse object as number");
        }
        // Switch avoids megamorphic virtual dispatch on the NumberType enum (visible in flamegraphs for bulk indexing).
        return switch (type) {
            case BYTE -> {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                yield (byte) value;
            }
            case SHORT -> parser.shortValue(coerce);
            case INTEGER -> parser.intValue(coerce);
            case LONG -> parser.longValue(coerce);
            default -> type.parse(parser, coerce);
        };
    }

    /**
     * Index a value for this field. For numeric fields this is called by
     * {@link #parseCreateField} but it is public so it can be used by other
     * fields that want to share the behavior of numeric fields.
     */
    public void indexValue(DocumentParserContext context, Number numericValue) {
        final String name = fieldType.name();
        final LuceneDocument doc = context.doc();

        if (writeDimensionRouting) {
            context.getRoutingFields().addLong(name, numericValue.longValue());
        }

        // Switch avoids megamorphic virtual dispatch on the NumberType enum (visible in flamegraphs for bulk indexing).
        switch (type) {
            case BYTE, SHORT, INTEGER -> addIntFields(doc, name, numericValue.intValue());
            case LONG -> addLongFields(doc, name, numericValue.longValue());
            default -> type.addFields(doc, name, numericValue, fieldType.indexType, stored, dvFactory);
        }

        if (false == allowMultipleValues && (indexed || docValuesParameters.enabled() || stored)) {
            // the last field is the current field, Add to the key map, so that we can validate if it has been added
            List<IndexableField> fields = doc.getFields();
            final IndexableField last = fields.getLast();
            assert last.name().equals(name) : "last field name [" + last.name() + "] mis match field name [" + name + "]";
            doc.onlyAddKey(name, last);
        }

        if (docValuesParameters.enabled() == false && (stored || indexed)) {
            context.addToFieldNames(name);
        }
    }

    private void addIntFields(LuceneDocument document, String name, int i) {
        final var indexType = fieldType.indexType();
        if (indexTerms) {
            if (indexType.hasDocValues()) {
                document.add(new IndexTermsIntegerField(name, i, encodeIndexTerm(i)));
            } else {
                document.add(new Field(name, encodeIndexTerm(i), INDEX_TERMS_FIELD_TYPE_INDEX_ONLY));
            }
        } else {
            if (indexType.hasPoints() && indexType.hasDocValues()) {
                document.add(new IntField(name, i, Field.Store.NO));
            } else if (indexType.hasDocValues()) {
                dvFactory.addNumericField(document, name, i);
            } else if (indexType.hasPoints()) {
                document.add(new IntPoint(name, i));
            }
        }
        if (stored) {
            document.add(new StoredField(name, i));
        }
    }

    private void addLongFields(LuceneDocument document, String name, long l) {
        final var indexType = fieldType.indexType();
        if (indexType.hasPoints() && indexType.hasDocValues()) {
            document.add(new LongField(name, l, Field.Store.NO));
        } else if (indexType.hasDocValues()) {
            dvFactory.addNumericField(document, name, l);
        } else if (indexType.hasPoints()) {
            document.add(new LongPoint(name, l));
        }
        if (stored) {
            document.add(new StoredField(name, l));
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), type, scriptCompiler, indexSettings).dimension(dimension)
            .metric(metricType)
            .allowMultipleValues(allowMultipleValues)
            .init(this);
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (dimension && null != lookup.nestedLookup().getNestedParent(fullPath())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + fullPath() + "]"
            );
        }
        if (dimension && metricType != null) {
            throw new IllegalArgumentException(
                "["
                    + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM
                    + "] and ["
                    + TimeSeriesParams.TIME_SERIES_METRIC_PARAM
                    + "] cannot be set in conjunction with each other ["
                    + fullPath()
                    + "]"
            );
        }
    }

    private SourceLoader.SyntheticFieldLoader docValuesSyntheticFieldLoader() {
        if (offsetsFieldName != null) {
            var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
            layers.add(new SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer(fullPath(), offsetsFieldName, type::writeValue));
            if (ignoreMalformed.value()) {
                layers.add(CompositeSyntheticFieldLoader.malformedValuesLayer(fullPath(), indexSettings.getIndexVersionCreated()));
            }
            return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
        } else {
            return type.syntheticFieldLoader(fullPath(), leafName(), ignoreMalformed.value(), indexSettings.getIndexVersionCreated());
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (docValuesParameters.enabled()) {
            return new SyntheticSourceSupport.Native(this::docValuesSyntheticFieldLoader);
        }

        return super.syntheticSourceSupport();
    }

    // For testing only:
    void setAllowMultipleValues(boolean allowMultipleValues) {
        this.allowMultipleValues = allowMultipleValues;
    }
}
