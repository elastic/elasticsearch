/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton.AUTOMATON_TYPE;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.escf.LuceneBinaryColumn;
import org.elasticsearch.escf.LuceneLongColumn;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader.ArrayOrderSource;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.ByteLengthFromBytesRefDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader;
import org.elasticsearch.index.query.AutomatonQueryWithDescription;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesPrefixQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRangeQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRegexpQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermInSetQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesWildcardQuery;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.SortedBinaryDocValuesStringFieldScript;
import org.elasticsearch.script.SortedSetDocValuesStringFieldScript;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.StringScriptFieldFuzzyQuery;
import org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery;
import org.elasticsearch.search.runtime.StringScriptFieldTermQuery;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_SETTING;
import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;
import static org.elasticsearch.index.mapper.FieldMapper.Parameter.useTimeSeriesDocValuesSkippers;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    private static final Logger logger = LogManager.getLogger(KeywordFieldMapper.class);

    public static final String CONTENT_TYPE = "keyword";
    private static final String HOST_NAME = "host.name";

    public static class Defaults {
        public static final FieldType FIELD_TYPE;
        public static final FieldType FIELD_TYPE_WITH_SKIP_DOC_VALUES;

        /**
         * The field type produced by {@link NumericDocValuesField#indexedField} — used for the
         * {@code <name>.counts} columnar output column.
         */
        static final IndexableFieldType COUNTS_FIELD_TYPE = MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_TYPE;

        static {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            ft.setDocValuesType(DocValuesType.SORTED_SET);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

        static {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.NONE);
            ft.setDocValuesType(DocValuesType.SORTED_SET);
            ft.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
            FIELD_TYPE_WITH_SKIP_DOC_VALUES = freezeAndDeduplicateFieldType(ft);
        }

        public static final TextSearchInfo TEXT_SEARCH_INFO = new TextSearchInfo(
            FIELD_TYPE,
            null,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER
        );
    }

    public static class KeywordField extends Field {

        public KeywordField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }
    }

    private static TextSearchInfo textSearchInfo(
        FieldType fieldType,
        @Nullable SimilarityProvider similarity,
        NamedAnalyzer searchAnalyzer,
        NamedAnalyzer searchQuoteAnalyzer
    ) {
        final TextSearchInfo textSearchInfo = new TextSearchInfo(fieldType, similarity, searchAnalyzer, searchQuoteAnalyzer);
        if (textSearchInfo.equals(Defaults.TEXT_SEARCH_INFO)) {
            return Defaults.TEXT_SEARCH_INFO;
        }
        return textSearchInfo;
    }

    private static KeywordFieldMapper toType(FieldMapper in) {
        return (KeywordFieldMapper) in;
    }

    public static final class Builder extends FieldMapper.DimensionBuilder {

        private final Parameter<Boolean> indexed;
        private final DocValuesParameter docValuesParameters;
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).fieldType.stored(), false);

        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).fieldType().nullValue, null)
            .acceptsNull();

        private final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> toType(m).fieldType().eagerGlobalOrdinals(),
            false
        );
        private final Parameter<Integer> ignoreAbove;
        private final Parameter<String> indexOptions = TextParams.keywordIndexOptions(m -> toType(m).indexOptions);
        private final Parameter<Boolean> hasNorms = Parameter.normsParam(m -> toType(m).fieldType.omitNorms() == false, false);
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(
            m -> toType(m).fieldType().getTextSearchInfo().similarity()
        );

        private final Parameter<String> normalizer;
        private final Parameter<Boolean> normalizerSkipStoreOriginalValue;

        private final Parameter<Boolean> splitQueriesOnWhitespace = Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> toType(m).splitQueriesOnWhitespace,
            false
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<OnScriptError> onScriptError = Parameter.onScriptErrorParam(
            m -> toType(m).builderParams.onScriptError(),
            script
        );
        private final Parameter<Boolean> dimension;

        private final IndexAnalyzers indexAnalyzers;
        private final ScriptCompiler scriptCompiler;
        private final IndexVersion indexCreatedVersion;
        private final boolean forceDocValuesSkipper;
        private final boolean isWithinMultiField;
        private final IndexSettings indexSettings;
        private final boolean storeIgnoredFieldsInBinaryDocValues;

        private String offsetsFieldName;
        private boolean arrayOrderBinaryDocValues;

        public Builder(final String name, final MappingParserContext mappingParserContext) {
            this(
                name,
                mappingParserContext.getIndexAnalyzers(),
                mappingParserContext.scriptCompiler(),
                mappingParserContext.getIndexSettings(),
                false,
                mappingParserContext.isWithinMultiField()
            );
        }

        public Builder(
            String name,
            IndexAnalyzers indexAnalyzers,
            ScriptCompiler scriptCompiler,
            IndexSettings indexSettings,
            boolean forceDocValuesSkipper,
            boolean isWithinMultiField
        ) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.indexCreatedVersion = indexSettings.getIndexVersionCreated();
            this.normalizer = Parameter.stringParam(
                "normalizer",
                indexCreatedVersion.isLegacyIndexVersion(),
                m -> toType(m).normalizerName,
                null
            ).acceptsNull();
            this.normalizerSkipStoreOriginalValue = Parameter.boolParam(
                "normalizer_skip_store_original_value",
                false,
                m -> ((KeywordFieldMapper) m).isNormalizerSkipStoreOriginalValue(),
                () -> "lowercase".equals(normalizer.getValue())
                    && indexAnalyzers.getNormalizer(normalizer.getValue()).analyzer() instanceof LowercaseNormalizer
            );

            this.script.precludesParameters(nullValue);

            this.docValuesParameters = DocValuesParameter.of(
                DocValuesParameter.defaultValues(
                    indexSettings,
                    DocValuesParameter.Values.ENABLED_LOW_CARDINALITY,
                    DocValuesParameter.Values.Cardinality.HIGH
                ),
                m -> toType(m).docValuesParameters(),
                indexSettings.getMode().isStrictColumnar()
            ).withUpdatableSupport();

            this.dimension = TimeSeriesParams.dimensionParam(
                m -> toType(m).fieldType().isDimension(),
                () -> docValuesParameters.getValue().enabled()
            ).precludesParameters(normalizer);
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, indexSettings, dimension);
            addScriptValidation(script, indexed, () -> docValuesParameters.getValue().enabled());

            this.ignoreAbove = Parameter.ignoreAboveParam(
                m -> toType(m).fieldType().ignoreAbove().get(),
                IGNORE_ABOVE_SETTING.get(indexSettings.getSettings())
            );
            this.forceDocValuesSkipper = forceDocValuesSkipper;
            this.isWithinMultiField = isWithinMultiField;
            this.indexSettings = indexSettings;
            if (indexCreatedVersion.onOrAfter(IndexVersions.STORE_IGNORED_WILDCARD_FIELDS_IN_BINARY_DOC_VALUES)) {
                // from this version, we check whether TSDB doc values format is enabled
                this.storeIgnoredFieldsInBinaryDocValues = indexSettings.useTimeSeriesDocValuesFormat();
            } else {
                // older indices stored ignored keyword fields in binary doc values regardless of the doc values format
                this.storeIgnoredFieldsInBinaryDocValues = indexCreatedVersion.onOrAfter(
                    IndexVersions.STORE_IGNORED_KEYWORDS_IN_BINARY_DOC_VALUES
                );
            }
        }

        public Builder(String name, IndexSettings indexSettings) {
            this(name, null, ScriptCompiler.NONE, indexSettings, false, false);
        }

        public Builder(String name, IndexSettings indexSettings, boolean isWithinMultiField) {
            this(name, null, ScriptCompiler.NONE, indexSettings, false, isWithinMultiField);
        }

        public static Builder buildWithDocValuesSkipper(String name, IndexSettings indexSettings, boolean isWithinMultiField) {
            return new Builder(name, null, ScriptCompiler.NONE, indexSettings, true, isWithinMultiField);
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove.setValue(ignoreAbove);
            return this;
        }

        Builder normalizer(String normalizerName) {
            this.normalizer.setValue(normalizerName);
            return this;
        }

        public boolean hasNormalizer() {
            return this.normalizer.get() != null;
        }

        public boolean isNormalizerSkipStoreOriginalValue() {
            return this.normalizerSkipStoreOriginalValue.getValue();
        }

        // Returns true when an effective ignore_above limit applies (field-level or index-level), so the doc values omit longer values.
        public boolean hasIgnoreAbove() {
            return this.ignoreAbove.getValue() != Integer.MAX_VALUE;
        }

        // Returns true when a null_value is configured, so the doc values substitute it for nulls rather than mirroring the raw values.
        public boolean hasNullValue() {
            return this.nullValue.getValue() != null;
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        @Deprecated()
        public Builder docValues(boolean hasDocValues) {
            this.docValuesParameters.setValue(
                hasDocValues
                    ? DocValuesParameter.defaultValues(
                        indexSettings,
                        DocValuesParameter.Values.ENABLED_LOW_CARDINALITY,
                        DocValuesParameter.Values.Cardinality.HIGH
                    )
                    : DocValuesParameter.Values.DISABLED_LOW_CARDINALITY
            );
            return this;
        }

        public Builder docValues(DocValuesParameter.Values.Cardinality cardinality) {
            var defaultDocValues = DocValuesParameter.defaultValues(
                indexSettings,
                DocValuesParameter.Values.ENABLED_LOW_CARDINALITY,
                DocValuesParameter.Values.Cardinality.HIGH
            );
            this.docValuesParameters.setValue(
                new DocValuesParameter.Values(
                    true,
                    cardinality,
                    defaultDocValues.multiValue(),
                    defaultDocValues.nullability(),
                    defaultDocValues.onFailure()
                )
            );
            return this;
        }

        public DocValuesParameter.Values docValuesParameters() {
            return docValuesParameters.getValue();
        }

        boolean usesBinaryDocValues() {
            return docValuesParameters().enabled() && docValuesParameters().cardinality() == DocValuesParameter.Values.Cardinality.HIGH;
        }

        public SimilarityProvider similarity() {
            return this.similarity.get();
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        public Builder indexed(boolean indexed) {
            this.indexed.setValue(indexed);
            return this;
        }

        public Builder stored(boolean stored) {
            this.stored.setValue(stored);
            return this;
        }

        public boolean isStored() {
            return this.stored.get();
        }

        private FieldValues<String> scriptValues() {
            if (script.get() == null) {
                return null;
            }
            StringFieldScript.Factory scriptFactory = scriptCompiler.compile(script.get(), StringFieldScript.CONTEXT);
            return scriptFactory == null
                ? null
                : (lookup, ctx, doc, consumer) -> scriptFactory.newFactory(leafName(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                indexed,
                docValuesParameters,
                stored,
                nullValue,
                eagerGlobalOrdinals,
                ignoreAbove,
                indexOptions,
                hasNorms,
                similarity,
                normalizer,
                normalizerSkipStoreOriginalValue,
                splitQueriesOnWhitespace,
                script,
                onScriptError,
                meta,
                dimension };
        }

        private IndexType buildIndexType(FieldType fieldType) {
            var docValuesParameters = docValuesParameters();
            if (docValuesParameters.enabled() && docValuesParameters.cardinality() == DocValuesParameter.Values.Cardinality.HIGH) {
                // Binary doc values are not reflected on the KeywordField's FieldType (see resolveFieldType); still advertise doc values
                // on the mapped field so queries, fielddata, and aggregations use the docvalues path
                return IndexType.terms(fieldType.indexOptions() != IndexOptions.NONE, true);
            }

            return IndexType.terms(fieldType);
        }

        private KeywordFieldType buildFieldType(MapperBuilderContext context, FieldType fieldType) {
            NamedAnalyzer normalizer = Lucene.KEYWORD_ANALYZER;
            NamedAnalyzer searchAnalyzer = Lucene.KEYWORD_ANALYZER;
            NamedAnalyzer quoteAnalyzer = Lucene.KEYWORD_ANALYZER;
            String normalizerName = this.normalizer.getValue();
            if (normalizerName != null) {
                assert indexAnalyzers != null;
                normalizer = indexAnalyzers.getNormalizer(normalizerName);
                if (normalizer == null) {
                    if (indexCreatedVersion.isLegacyIndexVersion()) {
                        logger.warn(
                            () -> format("Could not find normalizer [%s] of legacy index, falling back to default", normalizerName)
                        );
                        normalizer = Lucene.KEYWORD_ANALYZER;
                    } else {
                        throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + leafName() + "]");
                    }
                }
                searchAnalyzer = quoteAnalyzer = normalizer;
                if (splitQueriesOnWhitespace.getValue()) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                }
            } else if (splitQueriesOnWhitespace.getValue()) {
                searchAnalyzer = Lucene.WHITESPACE_ANALYZER;
            }
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension(true);
            }
            return new KeywordFieldType(
                context.buildFullName(leafName()),
                buildIndexType(fieldType),
                new TextSearchInfo(fieldType, similarity.get(), searchAnalyzer, quoteAnalyzer),
                normalizer,
                this,
                context.isSourceSynthetic()
            );
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public KeywordFieldMapper build(MapperBuilderContext context) {
            FieldType fieldtype = resolveFieldType(forceDocValuesSkipper, context.buildFullName(leafName()));
            super.hasScript = script.get() != null;
            super.onScriptError = onScriptError.getValue();

            this.offsetsFieldName = getOffsetsFieldName(
                context,
                indexSettings.sourceKeepMode(),
                docValuesParameters().enabled(),
                stored.getValue(),
                this,
                indexCreatedVersion,
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD,
                indexSettings.getMode().isStrictColumnar(),
                docValuesParameters().multiValue()
            );
            // High-cardinality (binary doc values) fields in strict columnar mode store their values in document order directly in the
            // binary doc values (ArrayOrderInlineNull) instead of recording a sidecar .offsets field; low-cardinality (sorted-set) fields
            // keep using offsets. This applies to index sort fields too: both MultiValuedBinaryDocValuesSortField (index sorting) and
            // AbstractBinaryDocValuesQuery (term/prefix/wildcard/range queries against fields with no inverted index, e.g. the
            // host.name skip-index sort field - see shouldUseHostnameSkipper) decode both the ArrayOrderInlineNull and SeparateCount
            // binary formats.
            if (offsetsFieldName != null && usesBinaryDocValues() && indexSettings.getMode().isStrictColumnar()) {
                this.arrayOrderBinaryDocValues = true;
                this.offsetsFieldName = null;
            }
            String fullName = context.buildFullName(leafName());
            FieldMapper.validateUpdatableDocValues(
                fullName,
                docValuesParameters(),
                indexed.getValue(),
                fieldtype.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE,
                indexSettings
            );
            if (docValuesParameters().updatable() && usesBinaryDocValues() == false) {
                // Lucene cannot update SORTED_SET doc values, which is what low-cardinality keywords are written as. High-cardinality
                // keywords go to a binary column instead, which it can update.
                throw new IllegalArgumentException(
                    "[doc_values.updatable] is not supported for low cardinality keyword field [" + fullName + "]"
                );
            }
            return new KeywordFieldMapper(
                leafName(),
                fieldtype,
                buildFieldType(context, fieldtype),
                builderParams(this, context),
                this,
                offsetsFieldName
            );
        }

        private FieldType resolveFieldType(final boolean forceDocValuesSkipper, final String fullFieldName) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            if (forceDocValuesSkipper
                || shouldUseHostnameSkipper(fullFieldName)
                || shouldUseTimeSeriesSkipper()
                || shouldUseStandardSkipper()) {
                fieldtype = new FieldType(Defaults.FIELD_TYPE_WITH_SKIP_DOC_VALUES);
            }
            fieldtype.setOmitNorms(this.hasNorms.getValue() == false);
            fieldtype.setStored(this.stored.getValue());

            DocValuesParameter.Values docValuesParameters = this.docValuesParameters.get();
            if (docValuesParameters.enabled() && docValuesParameters.cardinality() == DocValuesParameter.Values.Cardinality.LOW) {
                // Always use SORTED_SET so index-sort (SortedSetSortField) works at segment-merge time
                // even for multi_value=false fields. Single-valuedness is enforced at parse time instead.
                fieldtype.setDocValuesType(DocValuesType.SORTED_SET);
            } else {
                // NOTE: we still set DocValuesType.NONE on the fieldtype even when using binary doc values (cardinality == HIGH).
                // Values are written to a separate MultiValuedBinaryDocValuesField, so we must set this fieldtype to DocValuesType.NONE
                // to prevent the field constructed in KeywordFieldMapper#buildKeywordField (which uses this fieldType) from conflicting
                // with the separate MultiValuedBinaryDocValuesField.
                fieldtype.setDocValuesType(DocValuesType.NONE);
                fieldtype.setDocValuesSkipIndexType(DocValuesSkipIndexType.NONE);
            }

            if (fieldtype.equals(Defaults.FIELD_TYPE_WITH_SKIP_DOC_VALUES) == false) {
                // NOTE: override index options only if we are not using a sparse doc values index (and we use an inverted index)
                fieldtype.setIndexOptions(TextParams.toIndexOptions(this.indexed.getValue(), this.indexOptions.getValue()));
            }
            if (fieldtype.equals(Defaults.FIELD_TYPE)) {
                // deduplicate in the common default case to save some memory
                fieldtype = Defaults.FIELD_TYPE;
            }
            if (fieldtype.equals(Defaults.FIELD_TYPE_WITH_SKIP_DOC_VALUES)) {
                fieldtype = Defaults.FIELD_TYPE_WITH_SKIP_DOC_VALUES;
            }
            return fieldtype;
        }

        private boolean shouldUseTimeSeriesSkipper() {
            return docValuesParameters.getValue().enabled()
                && indexed.get() == false
                && useTimeSeriesDocValuesSkippers(indexSettings, dimension.get());
        }

        // TODO: for columnar the default should be based on the soon the built skipper mapping attribute.
        private boolean shouldUseHostnameSkipper(final String fullFieldName) {
            IndexMode mode = indexSettings.getMode();
            return docValuesParameters.getValue().enabled()
                && indexSettings.useDocValuesSkipperForHostName()
                && (IndexMode.LOGSDB.equals(mode) || IndexMode.LOGSDB_COLUMNAR.equals(mode))
                && HOST_NAME.equals(fullFieldName)
                && indexSortConfigByHostName(indexSettings.getIndexSortConfig());
        }

        private boolean shouldUseStandardSkipper() {
            return docValuesParameters().enabled()
                && indexed.get() == false
                && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS)
                && indexSettings.useDocValuesSkipper();
        }

        private static boolean indexSortConfigByHostName(final IndexSortConfig indexSortConfig) {
            return indexSortConfig != null && indexSortConfig.hasIndexSort() && indexSortConfig.hasSortOnField(HOST_NAME);
        }
    }

    public static final TypeParser PARSER = createTypeParserWithLegacySupport(Builder::new);

    public static final class KeywordFieldType extends TextFamilyFieldType {

        private static final IgnoreAbove IGNORE_ABOVE_DEFAULT = new IgnoreAbove(null, IndexMode.STANDARD);

        private final IgnoreAbove ignoreAbove;
        private final String nullValue;
        private final BytesRef nullUtf8Value;
        private final NamedAnalyzer normalizer;
        private final boolean eagerGlobalOrdinals;
        private final FieldValues<String> scriptValues;
        private final boolean isDimension;
        private final boolean usesBinaryDocValues;
        private final boolean usesBinaryDocValuesForIgnoredFields;
        private final DocValuesParameter.Values docValuesParams;
        private final IndexVersion indexVersion;
        private final boolean readInArrayOrder;
        private final boolean useArrayOrderBinaryDocValues;

        public KeywordFieldType(
            String name,
            IndexType indexType,
            TextSearchInfo textSearchInfo,
            NamedAnalyzer normalizer,
            Builder builder,
            boolean isSyntheticSource
        ) {
            super(
                name,
                indexType,
                builder.stored.get(),
                textSearchInfo,
                builder.meta.getValue(),
                isSyntheticSource,
                builder.isWithinMultiField
            );
            this.eagerGlobalOrdinals = builder.eagerGlobalOrdinals.getValue();
            this.normalizer = normalizer;
            this.ignoreAbove = new IgnoreAbove(
                builder.ignoreAbove.getValue(),
                builder.indexSettings.getMode(),
                builder.indexSettings.getIndexVersionCreated()
            );
            this.nullValue = builder.nullValue.getValue();
            this.nullUtf8Value = this.nullValue == null ? null : new BytesRef(this.nullValue);
            this.scriptValues = builder.scriptValues();
            this.isDimension = builder.dimension.getValue();
            this.usesBinaryDocValues = builder.usesBinaryDocValues();
            this.usesBinaryDocValuesForIgnoredFields = builder.storeIgnoredFieldsInBinaryDocValues;
            this.docValuesParams = builder.docValuesParameters();
            this.indexVersion = builder.indexSettings.getIndexVersionCreated();
            this.readInArrayOrder = builder.offsetsFieldName != null
                && builder.docValuesParameters().multiValue()
                && builder.indexSettings.getMode().isStrictColumnar();
            this.useArrayOrderBinaryDocValues = builder.arrayOrderBinaryDocValues;
        }

        public KeywordFieldType(String name) {
            this(name, true, true, false, Collections.emptyMap());
        }

        public KeywordFieldType(String name, boolean isIndexed, boolean hasDocValues, Map<String, String> meta) {
            this(name, isIndexed, hasDocValues, false, meta);
        }

        public KeywordFieldType(
            String name,
            boolean isIndexed,
            boolean hasDocValues,
            boolean usesBinaryDocValues,
            Map<String, String> meta
        ) {
            super(name, IndexType.terms(isIndexed, hasDocValues), false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta, false, false);
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = IGNORE_ABOVE_DEFAULT;
            this.nullValue = null;
            this.nullUtf8Value = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
            this.usesBinaryDocValues = usesBinaryDocValues;
            this.usesBinaryDocValuesForIgnoredFields = false;
            this.docValuesParams = null;
            this.indexVersion = IndexVersion.current();
            this.readInArrayOrder = false;
            this.useArrayOrderBinaryDocValues = false;
        }

        public KeywordFieldType(String name, FieldType fieldType, boolean isSyntheticSource) {
            super(
                name,
                IndexType.terms(fieldType),
                fieldType.stored(),
                textSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap(),
                isSyntheticSource,
                false
            );
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = IGNORE_ABOVE_DEFAULT;
            this.nullValue = null;
            this.nullUtf8Value = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
            this.usesBinaryDocValues = false;
            this.usesBinaryDocValuesForIgnoredFields = false;
            this.docValuesParams = null;
            this.indexVersion = IndexVersion.current();
            this.readInArrayOrder = false;
            this.useArrayOrderBinaryDocValues = false;
        }

        public KeywordFieldType(String name, NamedAnalyzer analyzer) {
            super(
                name,
                IndexType.terms(true, true),
                false,
                textSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer),
                Collections.emptyMap(),
                false,
                false
            );
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = IGNORE_ABOVE_DEFAULT;
            this.nullValue = null;
            this.nullUtf8Value = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
            this.usesBinaryDocValues = false;
            this.usesBinaryDocValuesForIgnoredFields = false;
            this.docValuesParams = null;
            this.indexVersion = IndexVersion.current();
            this.readInArrayOrder = false;
            this.useArrayOrderBinaryDocValues = false;
        }

        public boolean usesBinaryDocValues() {
            return usesBinaryDocValues;
        }

        /**
         * Returns true when this field stores keyword values through binary doc values and can store
         * more than one value for a document. Lucene term statistics do not describe value counts
         * for this representation, so callers must load the field to count values.
         */
        public boolean usesMultivaluedBinaryDocValues() {
            return usesBinaryDocValues && docValuesParams != null && docValuesParams.multiValue();
        }

        /**
         * Whether this field stores its (high-cardinality) binary doc values in document order with inline nulls
         * ({@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}) rather than via a sidecar offsets field.
         */
        public boolean usesArrayOrderBinaryDocValues() {
            return useArrayOrderBinaryDocValues;
        }

        public boolean usesBinaryDocValuesForIgnoredFields() {
            return usesBinaryDocValuesForIgnoredFields;
        }

        @Override
        public boolean isSearchable() {
            return indexType.hasTerms() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termQuery(value, context);
            } else if (usesBinaryDocValues) {
                return new ScanningBinaryDocValuesTermQuery(name(), indexedValueForSearch(value), useArrayOrderBinaryDocValues);
            } else {
                return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termsQuery(values, context);
            } else if (usesBinaryDocValues) {
                List<BytesRef> bytesRefs = values.stream().map(this::indexedValueForSearch).toList();
                return new ScanningBinaryDocValuesTermInSetQuery(name(), bytesRefs, useArrayOrderBinaryDocValues);
            } else {
                Collection<BytesRef> bytesRefs = values.stream().map(this::indexedValueForSearch).toList();
                return SortedSetDocValuesField.newSlowSetQuery(name(), bytesRefs);
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
            if (indexType.hasTerms()) {
                return super.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
            } else if (usesBinaryDocValues) {
                return new ScanningBinaryDocValuesRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                    upperTerm == null ? null : indexedValueForSearch(upperTerm),
                    includeLower,
                    includeUpper,
                    useArrayOrderBinaryDocValues
                );
            } else {
                return SortedSetDocValuesField.newSlowRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                    upperTerm == null ? null : indexedValueForSearch(upperTerm),
                    includeLower,
                    includeUpper
                );
            }
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context,
            @Nullable MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context, rewriteMethod);
            } else if (usesBinaryDocValues) {
                return StringScriptFieldFuzzyQuery.build(
                    new Script(""),
                    ctx -> new SortedBinaryDocValuesStringFieldScript(name(), context.lookup(), ctx, indexVersion),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    fuzziness.asDistance(BytesRefs.toString(value)),
                    prefixLength,
                    transpositions,
                    context
                );
            } else {
                return FuzzyQueries.create(
                    new Term(name(), indexedValueForSearch(value)),
                    fuzziness.asDistance(BytesRefs.toString(value)),
                    prefixLength,
                    maxExpansions,
                    transpositions,
                    MultiTermQuery.DOC_VALUES_REWRITE,
                    context,
                    name()
                );
            }
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            } else if (usesBinaryDocValues) {
                return new ScanningBinaryDocValuesPrefixQuery(
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    caseInsensitive,
                    useArrayOrderBinaryDocValues
                );
            } else {
                if (caseInsensitive == false) {
                    Term prefix = new Term(name(), indexedValueForSearch(value));
                    return new PrefixQuery(prefix, MultiTermQuery.DOC_VALUES_REWRITE);
                }
                return new StringScriptFieldPrefixQuery(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    caseInsensitive
                );
            }
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termQueryCaseInsensitive(value, context);
            } else if (usesBinaryDocValues) {
                return new StringScriptFieldTermQuery(
                    new Script(""),
                    ctx -> new SortedBinaryDocValuesStringFieldScript(name(), context.lookup(), ctx, indexVersion),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    true
                );
            } else {
                return new StringScriptFieldTermQuery(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    true
                );
            }
        }

        @Override
        public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) throws IOException {
            Terms terms = null;
            if (indexType.hasTerms()) {
                terms = MultiTerms.getTerms(reader, name());
            } else if (hasDocValues()) {
                if (usesBinaryDocValues) {
                    // Not possible to support terms enum api as underlying doc values lacks the capabilities to support it.
                    throw new IllegalArgumentException("terms enum is unsupported for field [" + name() + "]");
                } else {
                    terms = SortedSetDocValuesTerms.getTerms(reader, name());
                }
            }
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }
            Automaton a = caseInsensitive
                ? AutomatonQueries.caseInsensitivePrefix(prefix)
                : Operations.concatenate(Automata.makeString(prefix), Automata.makeAnyString());
            assert a.isDeterministic();

            CompiledAutomaton automaton = new CompiledAutomaton(a, true, true);

            BytesRef searchBytes = searchAfter == null ? null : new BytesRef(searchAfter);

            if (automaton.type == AUTOMATON_TYPE.ALL) {
                TermsEnum result = terms.iterator();
                if (searchAfter != null) {
                    result = new SearchAfterTermsEnum(result, searchBytes);
                }
                return result;
            }
            return terms.intersect(automaton, searchBytes);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean eagerGlobalOrdinals() {
            return eagerGlobalOrdinals;
        }

        NamedAnalyzer normalizer() {
            return normalizer;
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (hasDocValues() && (blContext.fieldExtractPreference() != FieldExtractPreference.STORED || isSyntheticSourceEnabled())) {
                BlockLoaderFunctionConfig cfg = blContext.blockLoaderFunctionConfig();
                if (cfg == null) {
                    if (usesBinaryDocValues) {
                        if (docValuesParams != null && docValuesParams.multiValue() == false) {
                            return new BytesRefsFromBinaryBlockLoader(name());
                        } else {
                            return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(
                                name(),
                                useArrayOrderBinaryDocValues ? ArrayOrderSource.INLINE : ArrayOrderSource.NONE
                            );
                        }
                    } else {
                        return new BytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize(), readInArrayOrder);
                    }
                }
                ArrayOrderSource arrayOrderSource = useArrayOrderBinaryDocValues ? ArrayOrderSource.INLINE : ArrayOrderSource.NONE;
                return switch (cfg.function()) {
                    case BYTE_LENGTH -> new ByteLengthFromBytesRefDocValuesBlockLoader(blContext.warnings(), name(), arrayOrderSource);
                    case LENGTH -> new Utf8CodePointsFromOrdsBlockLoader(
                        blContext.warnings(),
                        name(),
                        blContext.ordinalsByteSize(),
                        arrayOrderSource
                    );
                    case MV_MAX -> usesBinaryDocValues
                        ? new MvMaxBytesRefsFromBinaryBlockLoader(name(), arrayOrderSource)
                        : new MvMaxBytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize());
                    case MV_MIN -> usesBinaryDocValues
                        ? new MvMinBytesRefsFromBinaryBlockLoader(name(), arrayOrderSource)
                        : new MvMinBytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize());
                    default -> throw new UnsupportedOperationException("unknown fusion config [" + cfg.function() + "]");
                };
            }
            if (blContext.blockLoaderFunctionConfig() != null) {
                throw new UnsupportedOperationException("function fusing only supported for doc values");
            }
            if (isStored()) {
                return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(name());
            }

            // columnar_stored pre-builds _source as a single blob; skip the per-field fallback loader.
            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSourceEnabled()
                && blContext.mappingLookup().isSourceColumnarStored() == false
                && blContext.parentField(name()) == null) {
                return new FallbackSyntheticSourceBlockLoader(
                    fallbackSyntheticSourceBlockLoaderReader(),
                    name(),
                    IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings())
                ) {
                    @Override
                    public Builder builder(BlockFactory factory, int expectedCount) {
                        return factory.bytesRefs(expectedCount);
                    }
                };
            }

            SourceValueFetcher fetcher = sourceValueFetcher(blContext.sourcePaths(name()), blContext.indexSettings());
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, sourceBlockLoaderLookup(blContext));
        }

        @Override
        public boolean supportsBlockLoaderConfig(BlockLoaderFunctionConfig config, FieldExtractPreference preference) {
            if (hasDocValues() && (preference != FieldExtractPreference.STORED || isSyntheticSourceEnabled())) {
                return switch (config.function()) {
                    // Only push BYTE_LENGTH to load if using doc values
                    case BYTE_LENGTH -> usesBinaryDocValues;
                    case LENGTH, MV_MAX, MV_MIN -> true;
                    default -> false;
                };
            }
            return false;
        }

        private FallbackSyntheticSourceBlockLoader.Reader<?> fallbackSyntheticSourceBlockLoaderReader() {
            var nullValueBytes = nullValue != null ? new BytesRef(nullValue) : null;
            return new FallbackSyntheticSourceBlockLoader.SingleValueReader<BytesRef>(nullValueBytes) {
                @Override
                public void convertValue(Object value, List<BytesRef> accumulator) {
                    // When _source is synthetic, unmapped numeric fields are provided as their native Java types (Long, Double, etc.)
                    // rather than BytesRef. Since we treat all unmapped fields as keyword, we fall back to toString().
                    String stringValue = value instanceof BytesRef br ? br.utf8ToString() : value.toString();
                    String adjusted = applyIgnoreAboveAndNormalizer(stringValue);
                    if (adjusted != null) {
                        // TODO what if the value didn't change?
                        accumulator.add(new BytesRef(adjusted));
                    }
                }

                @Override
                public void parseNonNullValue(XContentParser parser, List<BytesRef> accumulator) throws IOException {
                    assert parser.currentToken() == XContentParser.Token.VALUE_STRING : "Unexpected token " + parser.currentToken();

                    var value = applyIgnoreAboveAndNormalizer(parser.text());
                    if (value != null) {
                        accumulator.add(new BytesRef(value));
                    }
                }

                @Override
                public void writeToBlock(List<BytesRef> values, BlockLoader.Builder blockBuilder) {
                    var bytesRefBuilder = (BlockLoader.BytesRefBuilder) blockBuilder;

                    for (var value : values) {
                        bytesRefBuilder.appendBytesRef(value);
                    }
                }
            };
        }

        private BlockSourceReader.LeafIteratorLookup sourceBlockLoaderLookup(BlockLoaderContext blContext) {
            if (getTextSearchInfo().hasNorms()) {
                return BlockSourceReader.lookupFromNorms(name());
            }
            if (hasDocValues() == false && (indexType.hasTerms() || isStored())) {
                // We only write the field names field if there aren't doc values or norms
                return BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name());
            }
            return BlockSourceReader.lookupMatchingAll();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
                return fieldDataFromDocValues();
            }
            if (operation != FielddataOperation.SCRIPT) {
                throw new IllegalStateException("unknown operation [" + operation.name() + "]");
            }

            if (hasDocValues()) {
                return fieldDataFromDocValues();
            }
            if (isSyntheticSourceEnabled()) {
                if (false == isStored()) {
                    throw new IllegalStateException(
                        "keyword field ["
                            + name()
                            + "] is only supported in synthetic _source index if it creates doc values or stored fields"
                    );
                }
                return (cache, breaker) -> new StoredFieldSortedBinaryIndexFieldData(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    KeywordDocValuesField::new
                ) {
                    @Override
                    protected BytesRef storedToBytesRef(Object stored) {
                        return (BytesRef) stored;
                    }
                };
            }

            Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());
            return new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                sourceValueFetcher(sourcePaths, fieldDataContext.indexSettings()),
                fieldDataContext.lookupSupplier().get(),
                KeywordDocValuesField::new
            );
        }

        private IndexFieldData.Builder fieldDataFromDocValues() {
            if (usesBinaryDocValues) {
                return new BytesBinaryIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    KeywordDocValuesField::new,
                    indexVersion,
                    useArrayOrderBinaryDocValues
                );
            } else {
                return new SortedSetOrdinalsIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
                );
            }
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
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    return applyIgnoreAboveAndNormalizer(keywordValue);
                }
            };
        }

        private String applyIgnoreAboveAndNormalizer(String value) {
            if (ignoreAbove.isIgnored(value)) return null;
            return normalizeValue(normalizer(), name(), value);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().searchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // keyword analyzer with the default attribute source which encodes terms using UTF8
                // in that case we skip normalization, which may be slow if there many terms need to
                // parse (eg. large terms query) since Analyzer.normalize involves things like creating
                // attributes through reflection
                // This if statement will be used whenever a normalizer is NOT configured
                return super.indexedValueForSearch(value);
            }

            if (value == null) {
                return null;
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return getTextSearchInfo().searchAnalyzer().normalize(name(), value.toString());
        }

        /**
         * Wildcard queries on keyword fields use the normalizer of the underlying field, regardless of their case sensitivity option
         */
        @Override
        public Query wildcardQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.wildcardQuery(value, method, caseInsensitive, true, context);
            } else {
                if (getTextSearchInfo().searchAnalyzer() != null) {
                    value = normalizeWildcardPattern(name(), value, getTextSearchInfo().searchAnalyzer());
                } else {
                    value = indexedValueForSearch(value).utf8ToString();
                }

                if (usesBinaryDocValues) {
                    return new ScanningBinaryDocValuesWildcardQuery(name(), value, caseInsensitive, useArrayOrderBinaryDocValues);
                }

                if (caseInsensitive == false) {
                    Term term = new Term(name(), value);
                    if (context.getCircuitBreaker() != null) {
                        Automaton dfa = AutomatonQueries.toWildcardAutomaton(term, context.getCircuitBreaker());
                        return new AutomatonQuery(term, dfa, false, MultiTermQuery.DOC_VALUES_REWRITE);
                    }
                    return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE);
                }

                StringFieldScript.LeafFactory leafFactory = ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx);
                return new StringScriptFieldWildcardQuery(new Script(""), leafFactory, name(), value, caseInsensitive);
            }
        }

        @Override
        public Query normalizedWildcardQuery(String value, MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.normalizedWildcardQuery(value, method, context);
            } else {
                if (getTextSearchInfo().searchAnalyzer() != null) {
                    value = normalizeWildcardPattern(name(), value, getTextSearchInfo().searchAnalyzer());
                } else {
                    value = indexedValueForSearch(value).utf8ToString();
                }

                if (usesBinaryDocValues) {
                    return new StringScriptFieldWildcardQuery(
                        new Script(""),
                        ctx -> new SortedBinaryDocValuesStringFieldScript(name(), context.lookup(), ctx, indexVersion),
                        name(),
                        value,
                        false
                    );
                } else {
                    Term term = new Term(name(), value);
                    if (context.getCircuitBreaker() != null) {
                        Automaton dfa = AutomatonQueries.toWildcardAutomaton(term, context.getCircuitBreaker());
                        return new AutomatonQuery(term, dfa, false, MultiTermQuery.DOC_VALUES_REWRITE);
                    }
                    return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE);
                }
            }
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
            } else {
                value = AutomatonQueries.collapseConsecutiveQuantifiers(value);
                if (usesBinaryDocValues) {
                    return new ScanningBinaryDocValuesRegexpQuery(
                        name(),
                        indexedValueForSearch(value).utf8ToString(),
                        syntaxFlags,
                        matchFlags,
                        maxDeterminizedStates,
                        useArrayOrderBinaryDocValues,
                        context.getCircuitBreaker()
                    );
                } else {
                    if (context.getCircuitBreaker() != null) {
                        Term term = new Term(name(), indexedValueForSearch(value));
                        Automaton dfa = AutomatonQueries.toRegexpAutomaton(
                            term,
                            syntaxFlags,
                            matchFlags,
                            maxDeterminizedStates,
                            context.getCircuitBreaker()
                        );
                        return new AutomatonQuery(term, dfa, false, MultiTermQuery.DOC_VALUES_REWRITE);
                    }
                    return new RegexpQuery(
                        new Term(name(), indexedValueForSearch(value)),
                        syntaxFlags,
                        matchFlags,
                        RegexpQuery.DEFAULT_PROVIDER,
                        maxDeterminizedStates,
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                }
            }
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.KEYWORD;
        }

        /** Values that have more chars than the return value of this method will
         *  be skipped at parsing time. */
        public IgnoreAbove ignoreAbove() {
            return ignoreAbove;
        }

        // True when a null_value is configured; such a substitution mutates the doc values away from the raw indexed values.
        public boolean hasNullValue() {
            return nullValue != null;
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public boolean hasScriptValues() {
            return scriptValues != null;
        }

        public boolean hasNormalizer() {
            return normalizer != Lucene.KEYWORD_ANALYZER;
        }

        @Override
        public Query automatonQuery(
            Supplier<Automaton> automatonSupplier,
            Supplier<CharacterRunAutomaton> characterRunAutomatonSupplier,
            @Nullable MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context,
            String description
        ) {
            return new AutomatonQueryWithDescription(new Term(name()), automatonSupplier.get(), description);
        }
    }

    private final boolean indexed;
    private final DocValuesParameter.Values docValuesParameters;
    private final DocValuesFieldFactory dvFactory;
    private final String indexOptions;
    private final FieldType fieldType;
    private final String normalizerName;
    private final boolean normalizerSkipStoreOriginalValue;
    private final boolean splitQueriesOnWhitespace;
    private final Script script;
    private final ScriptCompiler scriptCompiler;
    private final SourceKeepMode sourceKeepMode;

    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final boolean writeDimensionRouting;
    private final boolean forceDocValuesSkipper;
    private final boolean storeIgnoredFieldsInBinaryDocValues;
    private final String offsetsFieldName;

    private final IndexVersion indexCreatedVersion;

    private KeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        KeywordFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        String offsetsFieldName
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.indexed = builder.indexed.getValue();
        this.docValuesParameters = builder.docValuesParameters.getValue();
        this.dvFactory = new DocValuesFieldFactory(
            docValuesParameters.multiValue(),
            fieldType().indexType.hasDocValuesSkipper(),
            builder.indexCreatedVersion
        );
        this.indexOptions = builder.indexOptions.getValue();
        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.normalizerName = builder.normalizer.getValue();
        this.normalizerSkipStoreOriginalValue = builder.normalizerSkipStoreOriginalValue.getValue();
        this.splitQueriesOnWhitespace = builder.splitQueriesOnWhitespace.getValue();
        this.script = builder.script.get();
        this.indexAnalyzers = builder.indexAnalyzers;
        this.scriptCompiler = builder.scriptCompiler;
        this.indexSettings = builder.indexSettings;
        this.writeDimensionRouting = builder.dimension.getValue()
            && builder.indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource efs
            && efs.extractDimensionsWhileMapping();
        this.forceDocValuesSkipper = builder.forceDocValuesSkipper;
        this.storeIgnoredFieldsInBinaryDocValues = builder.storeIgnoredFieldsInBinaryDocValues;
        this.offsetsFieldName = offsetsFieldName;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        sourceKeepMode = builder.sourceKeepMode.orElse(indexSettings.sourceKeepMode());
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    public boolean storesArrayValuesInOrder() {
        return fieldType().usesArrayOrderBinaryDocValues();
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
    }

    public boolean isNormalizerSkipStoreOriginalValue() {
        return normalizerSkipStoreOriginalValue;
    }

    public DocValuesParameter.Values docValuesParameters() {
        return docValuesParameters;
    }

    @Override
    public boolean isDocValuesUpdatable() {
        return docValuesParameters.updatable();
    }

    @Override
    public void encodeDocValuesUpdate(Object value, DocValuesUpdateSink sink) {
        // Mirrors the binary doc-values value written by indexValue for a high-cardinality (binary) keyword: the normalized UTF-8 bytes.
        String normalized = normalizeValue(fieldType().normalizer(), fullPath(), value.toString());
        sink.binary(fullPath(), new BytesRef(normalized));
    }

    @Override
    public DocValuesUpdateSourceReader docValuesUpdateSourceReader(LeafReader reader) throws IOException {
        // An updatable keyword is single-valued binary doc values (see DocValuesFieldFactory); the stored bytes are the normalized value.
        BinaryDocValues docValues = DocValues.getBinary(reader, fullPath());
        return doc -> docValues.advanceExact(doc) ? docValues.binaryValue().utf8ToString() : null;
    }

    @Override
    protected boolean shouldEnforceSingleValue(XContentParser.Token token) {
        return docValuesParameters.multiValue() == false && (token != XContentParser.Token.VALUE_NULL || fieldType().nullValue != null);
    }

    @Override
    protected DocValuesParameter.Values.OnFailure onFailureBehavior() {
        return docValuesParameters.onFailure();
    }

    @Override
    public boolean isNullable() {
        return docValuesParameters.nullability() || fieldType().nullValue != null;
    }

    @Override
    public boolean supportsColumnarParse(IndexSettings indexSettings) {
        return indexSettings.getMode().isStrictColumnar()
            && supportsColumnarDocValues()
            && hasScript() == false
            && copyTo().copyToFields().isEmpty()
            && multiFields().iterator().hasNext() == false
            && normalizerName == null
            && fieldType().isDimension() == false;
    }

    /**
     * Returns true when this keyword field's doc-values encoding is supported on the columnar batch
     * path. Accepts both the array-order (multi_value=true, offsetsFieldName set) and single-valued
     * binary (multi_value=false) encoding. Other combinations fall back to the row path.
     */
    private boolean supportsColumnarDocValues() {
        if (fieldType().usesBinaryDocValues() == false) {
            return false;
        }

        if (fieldType().usesArrayOrderBinaryDocValues()) {
            return true;
        }

        // Only support single valued when not ArrayOrderBinaryDocValues
        return docValuesParameters().multiValue() == false;
    }

    // TODO: make the batch supply a recycler to wire up recycling instead of NON_RECYCLING_INSTANCE.
    private static EscfColumnBuilder mergeStringColumn() {
        EscfColumnBuilder b = new EscfColumnBuilder(CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        b.lockScalar(EscfColumnKind.STRING);
        return b;
    }

    private static EscfColumnBuilder mergeLongColumn() {
        EscfColumnBuilder b = new EscfColumnBuilder(CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        b.lockScalar(EscfColumnKind.LONG);
        return b;
    }

    @Override
    public void mapColumnBatch(BatchMappingContext ctx, EscfColumn source) {
        final boolean emitTerms = fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored();
        final boolean emitFallback = storeIgnoredValuesForSyntheticSource();
        final boolean emitDvs = fieldType().hasDocValues();
        if (emitTerms == false && emitDvs == false && emitFallback == false) {
            return;
        }

        // These paths build a scan cursor that converts all ESCF column kinds to BytesRef strings:
        // longs/doubles via canonical toString, booleans as "true"/"false", strings as-is, arrays
        // element-by-element. BINARY and KEY_VALUE columns are unsupported and throw when the cursor is
        // iterated.
        // NOTE: numbers are converted from their parsed values (Long/Double), so non-canonical source
        // literals (e.g. "1.50", "1e3") will produce the canonical toString form rather than the original
        // source characters (which the row path preserves via parser.getText()).
        // In order to support the original string representations we would need to keep the columns as
        // strings. This is possible as an eventual user option.

        if (fieldType().usesArrayOrderBinaryDocValues()) {
            mapColumnBatchArrayOrder(ctx, source, emitTerms, emitDvs, emitFallback);
        } else {
            mapColumnBatchSingleValue(ctx, source, emitTerms, emitDvs, emitFallback);
        }
    }

    private void mapColumnBatchArrayOrder(
        BatchMappingContext ctx,
        EscfColumn source,
        boolean emitTerms,
        boolean emitDvs,
        boolean emitFallback
    ) {
        final int docCount = ctx.docCount();

        // retainValues=false: each value is appended to the document blob before the cursor advances, so no
        // value has to outlive the nextDoc() that moves past it.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);
        // TODO: make the batch return these column builders to wire up recycling
        final EscfColumnBuilder terms = emitTerms ? mergeStringColumn() : null;
        final EscfColumnBuilder binaryDvs = emitDvs ? mergeStringColumn() : null;
        final EscfColumnBuilder dvCounts = emitDvs ? mergeLongColumn() : null;
        final EscfColumnBuilder fallback = emitFallback ? mergeStringColumn() : null;
        final EscfColumnBuilder fallbackCounts = emitFallback ? mergeLongColumn() : null;
        final BytesRef nullValueBytes = fieldType().nullUtf8Value;

        int currentDoc = -1;
        boolean ignoredThisDoc = false;
        // Buffer null when not emitted. Each document's slots are appended as they are read and
        // the finished blob is handed to binaryDvs.setString, which copies it out immediately, so the
        // buffer is free to be rewritten.
        final BytesRefBuilder docBlob = emitDvs ? new BytesRefBuilder() : null;
        int pos = 0;
        int docSlotCount = 0;
        int lastValueLength = 0;
        // True when the current doc has at least one non-null slot; gates binary dv blob emission.
        boolean hasNonNull = false;

        while (true) {
            final int nextDoc = cursor.nextDoc();
            if (nextDoc != currentDoc) {
                // Flush the completed doc's elements.
                // All-null docs write counts (matching ArrayOrderInlineNull.recordNull) but no blob.
                if (binaryDvs != null && docSlotCount > 0) {
                    dvCounts.setLong(currentDoc, docSlotCount);
                    if (hasNonNull) {
                        // TODO: considering appending slots straight into the column builder's stream.
                        // A single non-null slot is stored raw, so drop its length prefix; both cases end at pos.
                        final int length = docSlotCount == 1 ? lastValueLength : pos;
                        binaryDvs.setString(currentDoc, docBlob.bytes(), pos - length, length);
                    }
                    pos = 0;
                    docSlotCount = 0;
                    hasNonNull = false;
                }
                if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                currentDoc = nextDoc;
                ignoredThisDoc = false;
            }

            BytesRef binaryValue = cursor.value();

            // Explicit JSON null: apply null_value substitution if configured; otherwise record a
            // null doc-values slot (no term, no ignore_above check), mirroring the row-path's
            // ArrayOrderInlineNull.recordNull for an absent value with no null_value.
            if (binaryValue == null) {
                if (nullValueBytes != null) {
                    binaryValue = nullValueBytes;
                    // Fall through to normal value processing below.
                } else {
                    if (binaryDvs != null) {
                        pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(docBlob, pos, null);
                        docSlotCount++;
                        // hasNonNull stays false: null slots do not produce a binary dv blob.
                    }
                    continue;
                }
            }

            // ignore_above: record _ignored once per doc; defer the synthetic-source value fallback.
            if (fieldType().ignoreAbove().isIgnored(binaryValue)) {
                if (ignoredThisDoc == false) {
                    ctx.addIgnoredFieldColumnar(currentDoc, fullPath());
                    if (fallback != null) {
                        fallback.setString(currentDoc, binaryValue);
                        fallbackCounts.setLong(currentDoc, 1L);
                    }
                    ignoredThisDoc = true;
                } else if (fallback != null) {
                    // TODO: support multiple ignore_above-exceeded values per doc (multi-valued
                    // fallback requires SeparateCount vint-length encoding across multiple values).
                    throw new UnsupportedOperationException(
                        "mapColumnBatch: more than one ignore_above-exceeded value in field ["
                            + fullPath()
                            + "] for doc ["
                            + currentDoc
                            + "]; multi-valued synthetic-source fallback is not yet supported"
                    );
                }
                continue;
            }

            if (binaryValue.length > MAX_TERM_LENGTH) {
                throw largeTermException(binaryValue);
            }

            if (terms != null) {
                terms.setString(currentDoc, binaryValue);
            }
            if (binaryDvs != null) {
                pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(docBlob, pos, binaryValue);
                lastValueLength = binaryValue.length;
                docSlotCount++;
                hasNonNull = true;
            }
        }

        // Attach output columns. Terms, binary-dv blob, and counts are each emitted independently.
        // All-null docs emit counts but no binary blob, so binaryDvs and dvCounts are decoupled.
        if (terms != null && terms.isEmpty() == false) {
            ctx.addColumn(LuceneBinaryColumn.of(terms.finish(docCount), fieldType().name(), fieldType));
        }
        if (binaryDvs != null && binaryDvs.isEmpty() == false) {
            ctx.addColumn(LuceneBinaryColumn.of(binaryDvs.finish(docCount), fieldType().name(), CustomDocValuesField.TYPE));
        }
        if (dvCounts != null && dvCounts.isEmpty() == false) {
            ctx.addColumn(
                LuceneLongColumn.of(
                    dvCounts.finish(docCount),
                    fieldType().name() + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX,
                    Defaults.COUNTS_FIELD_TYPE,
                    LongColumn.NumericKind.LONG
                )
            );
        }
        if (emitFallback && fallback != null && fallback.isEmpty() == false) {
            final String fallbackFieldName = fieldType().syntheticSourceFallbackFieldName();
            ctx.addColumn(LuceneBinaryColumn.of(fallback.finish(docCount), fallbackFieldName, CustomDocValuesField.TYPE));
            ctx.addColumn(
                LuceneLongColumn.of(
                    fallbackCounts.finish(docCount),
                    fallbackFieldName + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX,
                    Defaults.COUNTS_FIELD_TYPE,
                    LongColumn.NumericKind.LONG
                )
            );
        }
    }

    private void mapColumnBatchSingleValue(
        BatchMappingContext ctx,
        EscfColumn source,
        boolean emitTerms,
        boolean emitDvs,
        boolean emitFallback
    ) {
        final int docCount = ctx.docCount();
        boolean valuesProduced = false;

        // retainValues=false: every value is consumed within one loop iteration, before the cursor advances.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);
        EscfColumnBuilder values = source.leafValueKind() != EscfColumnKind.STRING && (emitTerms || emitDvs) ? mergeStringColumn() : null;
        final EscfColumnBuilder fallback = emitFallback ? mergeStringColumn() : null;
        final BytesRef nullValueBytes = fieldType().nullUtf8Value;

        int currentDoc = -1;
        boolean valueSeenThisDoc = false;
        while (true) {
            final int nextDoc = cursor.nextDoc();
            if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            if (nextDoc != currentDoc) {
                currentDoc = nextDoc;
                valueSeenThisDoc = false;
            }
            BytesRef binaryValue = cursor.value();
            if (binaryValue == null) {
                if (nullValueBytes != null) {
                    binaryValue = nullValueBytes;  // substitute, fall through to normal processing
                } else {
                    continue;  // null without null_value -> absent (row-path parity)
                }
            }

            // TODO: Can move this validation earlier based on array type
            if (valueSeenThisDoc) {
                // multi_value=false violation: bail so ShardBatchMapper falls back to the row path,
                // which raises the correct per-doc error (on_failure=FAIL).
                throw new UnsupportedOperationException(
                    "mapColumnBatch: multi_value=false field [" + fullPath() + "] has more than one value for doc [" + currentDoc + "]"
                );
            }
            valueSeenThisDoc = true;

            if (fieldType().ignoreAbove().isIgnored(binaryValue)) {
                ctx.addIgnoredFieldColumnar(currentDoc, fullPath());
                // Deoptimize: we were planning to zero-copy the source column, but now we must
                // exclude this doc's value from the output. Lazily create the builder and backfill
                // all accepted values from before this doc, then continue building per-value.
                if (values == null && (emitTerms || emitDvs)) {
                    values = mergeStringColumn();
                    EscfColumnTransforms.backfillUtf8Before(values, source, currentDoc);
                }
                if (fallback != null) {
                    fallback.setString(currentDoc, binaryValue);
                }
                continue;
            }
            if (binaryValue.length > MAX_TERM_LENGTH) {
                throw largeTermException(binaryValue);
            }

            valuesProduced = true;
            if (values != null) {
                values.setString(currentDoc, binaryValue);
            }
        }

        // Emit one term column (frozen fieldType, DocValuesType=NONE for HIGH cardinality) and one
        // plain binary DV column (BinaryDocValuesField.TYPE, omitNorms=false) — no .counts sidecar.
        // Both columns share the same finished EscfColumnData (one serialization, two field-type wrappers).
        if (valuesProduced) {
            final EscfColumnData data = values != null ? values.finish(docCount) : source.columnData();
            if (emitTerms) {
                ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), fieldType));
            }
            if (emitDvs) {
                ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), BinaryDocValuesField.TYPE));
            }
        }
        // Synthetic-source fallback for ignore_above values: single BinaryDocValuesField (no counts),
        // mirroring the row-path's addBinaryFieldLegacyEncodingAware isSingleValued() branch.
        if (fallback != null && fallback.isEmpty() == false) {
            ctx.addColumn(
                LuceneBinaryColumn.of(fallback.finish(docCount), fieldType().syntheticSourceFallbackFieldName(), BinaryDocValuesField.TYPE)
            );
        }
    }

    private IllegalArgumentException largeTermException(BytesRef value) {
        byte[] prefix = new byte[30];
        System.arraycopy(value.bytes, value.offset, prefix, 0, 30);
        return new IllegalArgumentException(
            "Document contains at least one immense term in field=\""
                + fieldType().name()
                + "\" (whose UTF8 encoding is longer than the max length "
                + MAX_TERM_LENGTH
                + "), all of which were skipped. Please correct the analyzer to not produce such terms."
                + " The prefix of the first immense term is: '"
                + Arrays.toString(prefix)
                + "...'"
        );
    }

    protected void parseCreateField(DocumentParserContext context) throws IOException {
        var value = context.parser().optimizedTextOrNull();

        if (value == null && fieldType().nullValue != null) {
            value = new Text(fieldType().nullValue);
        }

        boolean indexed = indexValue(context, value);
        if (fieldType().usesArrayOrderBinaryDocValues()) {
            // In-order path: non-null values are recorded in indexValue (in document order); here we record null slots so their position
            // is preserved. Values that tripped ignore_above (indexed == false, value != null) record no slot, matching the offsets path.
            if (indexed == false && value == null) {
                MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordNull(context.doc(), fieldType().name());
            }
        } else if (FieldArrayContext.shouldRecordOffsets(context, offsetsFieldName, docValuesParameters.multiValue())) {
            if (indexed) {
                context.getOffSetContext().recordOffset(offsetsFieldName, value.bytes());
            } else if (value == null) {
                context.getOffSetContext().recordNull(offsetsFieldName);
            }
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.fieldType().scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    private boolean indexValue(DocumentParserContext context, String value) {
        return indexValue(context, new Text(value));
    }

    /**
     * Returns whether this field should be stored separately as a {@link StoredField} for supporting synthetic source.
     */
    private boolean storeIgnoredValuesForSyntheticSource() {
        // skip all fields that are multi-fields
        return fieldType().isSyntheticSourceEnabled() && fieldType().isWithinMultiField() == false;
    }

    private boolean indexValue(DocumentParserContext context, XContentString value) {
        // nothing to index
        if (value == null) {
            return false;
        }

        // if field is disabled, skip indexing
        if ((fieldType.indexOptions() == IndexOptions.NONE) && (fieldType.stored() == false) && (fieldType().hasDocValues() == false)) {
            return false;
        }

        // if the value's length exceeds ignore_above, then don't index it
        if (fieldType().ignoreAbove().isIgnored(value)) {
            context.addIgnoredField(fullPath());

            // if synthetic source is enabled, then store a copy of the value so that synthetic source be load it
            if (storeIgnoredValuesForSyntheticSource()) {
                var utfBytes = value.bytes();
                var bytesRef = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
                final String fieldName = fieldType().syntheticSourceFallbackFieldName();

                if (storeIgnoredFieldsInBinaryDocValues) {
                    dvFactory.addBinaryFieldLegacyEncodingAware(
                        context.doc(),
                        fieldName,
                        bytesRef,
                        keepDuplicatesInBinaryDocValues()
                            ? MultiValuedBinaryDocValuesField.ValueOrdering.SORTED
                            : MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                    );
                } else {
                    // otherwise for bwc, store the value in a stored fields like we used to
                    context.doc().add(new StoredField(fieldName, bytesRef));
                }
            }

            return false;
        }

        if (fieldType().normalizer() != Lucene.KEYWORD_ANALYZER) {
            String normalizedString = normalizeValue(fieldType().normalizer(), fullPath(), value.string());
            value = new Text(normalizedString);
        }

        var utfBytes = value.bytes();
        var binaryValue = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
        if (writeDimensionRouting) {
            context.getRoutingFields().addString(fieldType().name(), binaryValue);
        }

        // If the UTF8 encoding of the field value is bigger than the max length 32766, Lucene will fail the indexing request and, to
        // roll back the changes, will mark the (possibly partially indexed) document as deleted. This results in deletes, even in an
        // append-only workload, which in turn leads to slower merges, as these will potentially have to fall back to MergeStrategy.DOC
        // instead of MergeStrategy.BULK. To avoid this, we do a preflight check here before indexing the document into Lucene.
        if (binaryValue.length > MAX_TERM_LENGTH) {
            throw largeTermException(binaryValue);
        }

        if (fieldType().usesBinaryDocValues()) {
            // KeywordField is built with a FieldType that omits Lucene doc values; binary values are accumulated on a parallel field.
            assert fieldType.docValuesType() == DocValuesType.NONE;
            if (fieldType().usesArrayOrderBinaryDocValues()) {
                // In-order path: write the value into the field's own binary doc-values column directly, in document order with nulls.
                if (context.isPartOfArray() == false) {
                    MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordSingleValue(context.doc(), fieldType().name(), binaryValue);
                } else {
                    MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(context.doc(), fieldType().name(), binaryValue);
                }
            } else {
                dvFactory.addBinaryField(
                    context.doc(),
                    fieldType().name(),
                    binaryValue,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
            }
        }

        // If we're using binary doc values, then the values are stored in a separate MultiValuedBinaryDocValuesField (see above)
        // and this fieldType has docValuesType=NONE. Then, when there is no index defined and the field is not stored, this field
        // is a no-op and we can skip adding it to the document.
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.docValuesType() != DocValuesType.NONE || fieldType.stored()) {
            Field field = buildKeywordField(binaryValue);
            context.doc().add(field);
        }

        if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
            context.addToFieldNames(fieldType().name());
        }

        return true;
    }

    /**
     * To be as true as possible to the provided source, we should NOT be deduplicating any values. As a result, in new indices, we will
     * keep duplicates.
     *
     * This mirrors how text and match only text fields support synthetic source.
     */
    private boolean keepDuplicatesInBinaryDocValues() {
        return indexCreatedVersion.onOrAfter(IndexVersions.KEYWORD_FIELDS_KEEP_DUPLICATES_IN_BINARY_DOC_VALUES);
    }

    private static String normalizeValue(NamedAnalyzer normalizer, String field, String value) {
        if (normalizer == Lucene.KEYWORD_ANALYZER) {
            return value;
        }
        try (TokenStream ts = normalizer.tokenStream(field, value)) {
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            if (ts.incrementToken() == false) {
                throw new IllegalStateException(String.format(Locale.ROOT, """
                    The normalization token stream is expected to produce exactly 1 token, \
                    but got 0 for analyzer %s and input "%s"
                    """, normalizer, value));
            }
            final String newValue = termAtt.toString();
            if (ts.incrementToken()) {
                throw new IllegalStateException(String.format(Locale.ROOT, """
                    The normalization token stream is expected to produce exactly 1 token, \
                    but got 2+ for analyzer %s and input "%s"
                    """, normalizer, value));
            }
            ts.end();
            return newValue;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), fieldType().normalizer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            leafName(),
            indexAnalyzers,
            scriptCompiler,
            indexSettings,
            forceDocValuesSkipper,
            fieldType().isWithinMultiField()
        ).dimension(fieldType().isDimension()).init(this);
    }

    // Uses this mapper's frozen FieldType; for high-cardinality doc values that type has DocValuesType.NONE because binary doc values
    // are indexed via MultiValuedBinaryDocValuesField in indexValue, not on this Lucene Field instance.
    public Field buildKeywordField(BytesRef binaryValue) {
        return new KeywordField(fieldType().name(), binaryValue, fieldType);
    }

    public FieldType luceneFieldType() {
        return fieldType;
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (fieldType().isDimension() && null != lookup.nestedLookup().getNestedParent(fullPath())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + fullPath() + "]"
            );
        }
    }

    boolean hasNormalizer() {
        return normalizerName != null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (hasNormalizer() && normalizerSkipStoreOriginalValue == false) {
            // NOTE: we use fallback synthetic source to store the original value since the doc values would be altered by the normalizer
            return SyntheticSourceSupport.FALLBACK;
        }

        if (fieldType.stored() || docValuesParameters.enabled()) {
            return new SyntheticSourceSupport.Native(() -> syntheticFieldLoader(fullPath(), leafName()));
        }

        return super.syntheticSourceSupport();
    }

    /**
     * Returns the layers for loading synthetic source values for this keyword field.
     * These can be used by parent fields to combine layers from multiple sources.
     */
    public List<CompositeSyntheticFieldLoader.Layer> syntheticFieldLoaderLayers() {
        assert fieldType.stored() || docValuesParameters.enabled();

        var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
        if (fieldType.stored()) {
            layers.add(new CompositeSyntheticFieldLoader.StoredFieldLayer(fullPath()) {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    BytesRef ref = (BytesRef) value;
                    b.utf8Value(ref.bytes, ref.offset, ref.length);
                }
            });
        } else if (docValuesParameters.enabled()) {
            if (fieldType().usesBinaryDocValues() == false) {
                if (offsetsFieldName != null) {
                    layers.add(new SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer(fullPath(), offsetsFieldName));
                } else {
                    layers.add(new SortedSetDocValuesSyntheticFieldLoaderLayer(fullPath()) {

                        @Override
                        protected BytesRef convert(BytesRef value) {
                            return value;
                        }

                        @Override
                        protected BytesRef preserve(BytesRef value) {
                            // Preserve must make a deep copy because convert gets a shallow copy from the iterator
                            return BytesRef.deepCopyOf(value);
                        }
                    });
                }
            } else {
                if (fieldType().usesArrayOrderBinaryDocValues()) {
                    layers.add(new ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer(fieldType().name()));
                } else {
                    layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fieldType().name(), indexCreatedVersion));
                }
            }
        }

        // if ignore_above is set, then there is a chance that this field will be ignored. In such cases, we save an
        // extra copy of the field for supporting synthetic source. This layer will check that copy.
        if (fieldType().ignoreAbove.valuesPotentiallyIgnored()) {
            final String fieldName = fieldType().syntheticSourceFallbackFieldName();

            if (storeIgnoredFieldsInBinaryDocValues) {
                layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fieldName, indexCreatedVersion));
            } else {
                // old indices, stored ignored values in stored fields
                layers.add(new CompositeSyntheticFieldLoader.StoredFieldLayer(fieldName) {
                    @Override
                    protected void writeValue(Object value, XContentBuilder b) throws IOException {
                        BytesRef ref = (BytesRef) value;
                        b.utf8Value(ref.bytes, ref.offset, ref.length);
                    }
                });
            }
        }

        return layers;
    }

    public CompositeSyntheticFieldLoader syntheticFieldLoader(String fullFieldName, String leafFieldName) {
        var layers = syntheticFieldLoaderLayers();
        if (onFailureColumnEnabled()) {
            layers.add(CompositeSyntheticFieldLoader.onFailureValuesLayer(fullPath(), indexCreatedVersion));
        }
        return new CompositeSyntheticFieldLoader(leafFieldName, fullFieldName, layers);
    }
}
