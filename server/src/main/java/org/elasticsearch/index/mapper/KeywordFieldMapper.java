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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton.AUTOMATON_TYPE;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader;
import org.elasticsearch.index.query.AutomatonQueryWithDescription;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.SortedSetDocValuesStringFieldScript;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery;
import org.elasticsearch.search.runtime.StringScriptFieldTermQuery;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;
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
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
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

            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).fieldType().isDimension(), hasDocValues::get)
                .precludesParameters(normalizer);
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, indexSettings, dimension);
            addScriptValidation(script, indexed, hasDocValues);

            this.ignoreAbove = Parameter.ignoreAboveParam(
                m -> toType(m).fieldType().ignoreAbove().get(),
                IGNORE_ABOVE_SETTING.get(indexSettings.getSettings())
            );
            this.forceDocValuesSkipper = forceDocValuesSkipper;
            this.isWithinMultiField = isWithinMultiField;
            this.indexSettings = indexSettings;
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

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        public boolean hasDocValues() {
            return this.hasDocValues.get();
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
                hasDocValues,
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
                IndexType.terms(fieldType),
                new TextSearchInfo(fieldType, similarity.get(), searchAnalyzer, quoteAnalyzer),
                normalizer,
                this,
                context.isSourceSynthetic()
            );
        }

        @Override
        public KeywordFieldMapper build(MapperBuilderContext context) {
            FieldType fieldtype = resolveFieldType(forceDocValuesSkipper, context.buildFullName(leafName()));
            super.hasScript = script.get() != null;
            super.onScriptError = onScriptError.getValue();

            String offsetsFieldName = getOffsetsFieldName(
                context,
                indexSettings.sourceKeepMode(),
                hasDocValues.getValue(),
                stored.getValue(),
                this,
                indexCreatedVersion,
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            );
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
            if (forceDocValuesSkipper || shouldUseHostnameSkipper(fullFieldName) || shouldUseTimeSeriesSkipper()) {
                fieldtype = new FieldType(Defaults.FIELD_TYPE_WITH_SKIP_DOC_VALUES);
            }
            fieldtype.setOmitNorms(this.hasNorms.getValue() == false);
            fieldtype.setStored(this.stored.getValue());
            fieldtype.setDocValuesType(this.hasDocValues.getValue() ? DocValuesType.SORTED_SET : DocValuesType.NONE);
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
            return hasDocValues.get() && indexed.get() == false && useTimeSeriesDocValuesSkippers(indexSettings, dimension.get());
        }

        private boolean shouldUseHostnameSkipper(final String fullFieldName) {
            return hasDocValues.get()
                && IndexSettings.USE_DOC_VALUES_SKIPPER.get(indexSettings.getSettings())
                && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.SKIPPERS_ENABLED_BY_DEFAULT)
                && IndexMode.LOGSDB.equals(indexSettings.getMode())
                && HOST_NAME.equals(fullFieldName)
                && indexSortConfigByHostName(indexSettings.getIndexSortConfig());
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
        private final NamedAnalyzer normalizer;
        private final boolean eagerGlobalOrdinals;
        private final FieldValues<String> scriptValues;
        private final boolean isDimension;

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
            this.scriptValues = builder.scriptValues();
            this.isDimension = builder.dimension.getValue();
        }

        public KeywordFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        public KeywordFieldType(String name, boolean isIndexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, IndexType.terms(isIndexed, hasDocValues), false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta, false, false);
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = IGNORE_ABOVE_DEFAULT;
            this.nullValue = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
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
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
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
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
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
            } else {
                return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termsQuery(values, context);
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
            } else {
                return new FuzzyQuery(
                    new Term(name(), indexedValueForSearch(value)),
                    fuzziness.asDistance(BytesRefs.toString(value)),
                    prefixLength,
                    maxExpansions,
                    transpositions,
                    MultiTermQuery.DOC_VALUES_REWRITE
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
                terms = SortedSetDocValuesTerms.getTerms(reader, name());
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
                    return new BytesRefsFromOrdsBlockLoader(name());
                }
                if (cfg.function() == BlockLoaderFunctionConfig.Function.LENGTH) {
                    return new Utf8CodePointsFromOrdsBlockLoader(((BlockLoaderFunctionConfig.JustWarnings) cfg).warnings(), name());
                }
                throw new UnsupportedOperationException("unknown fusion config [" + cfg.function() + "]");
            }
            if (blContext.blockLoaderFunctionConfig() != null) {
                throw new UnsupportedOperationException("function fusing only supported for doc values");
            }
            if (isStored()) {
                return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(name());
            }

            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSourceEnabled() && blContext.parentField(name()) == null) {
                return new FallbackSyntheticSourceBlockLoader(
                    fallbackSyntheticSourceBlockLoaderReader(),
                    name(),
                    IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings().getIndexVersionCreated())
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
                return config.function() == BlockLoaderFunctionConfig.Function.LENGTH;
            }
            return false;
        }

        private FallbackSyntheticSourceBlockLoader.Reader<?> fallbackSyntheticSourceBlockLoaderReader() {
            var nullValueBytes = nullValue != null ? new BytesRef(nullValue) : null;
            return new FallbackSyntheticSourceBlockLoader.SingleValueReader<BytesRef>(nullValueBytes) {
                @Override
                public void convertValue(Object value, List<BytesRef> accumulator) {
                    String stringValue = ((BytesRef) value).utf8ToString();
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

        private SortedSetOrdinalsIndexFieldData.Builder fieldDataFromDocValues() {
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
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
                if (caseInsensitive == false) {
                    Term term = new Term(name(), value);
                    return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE);
                }
                return new StringScriptFieldWildcardQuery(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    value,
                    caseInsensitive
                );
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
                Term term = new Term(name(), value);
                return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE);
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
                if (matchFlags != 0) {
                    throw new IllegalArgumentException("Match flags not yet implemented [" + matchFlags + "]");
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

        @Override
        public CollapseType collapseType() {
            return CollapseType.KEYWORD;
        }

        /** Values that have more chars than the return value of this method will
         *  be skipped at parsing time. */
        public IgnoreAbove ignoreAbove() {
            return ignoreAbove;
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
    private final boolean hasDocValues;
    private final String indexOptions;
    private final FieldType fieldType;
    private final String normalizerName;
    private final boolean normalizerSkipStoreOriginalValue;
    private final boolean splitQueriesOnWhitespace;
    private final Script script;
    private final ScriptCompiler scriptCompiler;

    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final boolean forceDocValuesSkipper;
    private final String offsetsFieldName;

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
        this.hasDocValues = builder.hasDocValues.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.normalizerName = builder.normalizer.getValue();
        this.normalizerSkipStoreOriginalValue = builder.normalizerSkipStoreOriginalValue.getValue();
        this.splitQueriesOnWhitespace = builder.splitQueriesOnWhitespace.getValue();
        this.script = builder.script.get();
        this.indexAnalyzers = builder.indexAnalyzers;
        this.scriptCompiler = builder.scriptCompiler;
        this.indexSettings = builder.indexSettings;
        this.forceDocValuesSkipper = builder.forceDocValuesSkipper;
        this.offsetsFieldName = offsetsFieldName;
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
    }

    public boolean isNormalizerSkipStoreOriginalValue() {
        return normalizerSkipStoreOriginalValue;
    }

    protected void parseCreateField(DocumentParserContext context) throws IOException {
        var value = context.parser().optimizedTextOrNull();

        if (value == null && fieldType().nullValue != null) {
            value = new Text(fieldType().nullValue);
        }

        boolean indexed = indexValue(context, value);
        if (offsetsFieldName != null && context.isImmediateParentAnArray() && context.canAddIgnoredField()) {
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
                context.doc().add(new StoredField(fieldName, bytesRef));
            }

            return false;
        }

        if (fieldType().normalizer() != Lucene.KEYWORD_ANALYZER) {
            String normalizedString = normalizeValue(fieldType().normalizer(), fullPath(), value.string());
            value = new Text(normalizedString);
        }

        var utfBytes = value.bytes();
        var binaryValue = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
        if (fieldType().isDimension()) {
            context.getRoutingFields().addString(fieldType().name(), binaryValue);
        }

        // If the UTF8 encoding of the field value is bigger than the max length 32766, Lucene fill fail the indexing request and, to
        // roll back the changes, will mark the (possibly partially indexed) document as deleted. This results in deletes, even in an
        // append-only workload, which in turn leads to slower merges, as these will potentially have to fall back to MergeStrategy.DOC
        // instead of MergeStrategy.BULK. To avoid this, we do a preflight check here before indexing the document into Lucene.
        if (binaryValue.length > MAX_TERM_LENGTH) {
            byte[] prefix = new byte[30];
            System.arraycopy(binaryValue.bytes, binaryValue.offset, prefix, 0, 30);
            String msg = "Document contains at least one immense term in field=\""
                + fieldType().name()
                + "\" (whose "
                + "UTF8 encoding is longer than the max length "
                + MAX_TERM_LENGTH
                + "), all of which were "
                + "skipped. Please correct the analyzer to not produce such terms. The prefix of the first immense "
                + "term is: '"
                + Arrays.toString(prefix)
                + "...'";
            throw new IllegalArgumentException(msg);
        }

        Field field = buildKeywordField(binaryValue);
        context.doc().add(field);

        if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
            context.addToFieldNames(fieldType().name());
        }

        return true;
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

    public Field buildKeywordField(BytesRef binaryValue) {
        return new KeywordField(fieldType().name(), binaryValue, fieldType);
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

        if (fieldType.stored() || hasDocValues) {
            return new SyntheticSourceSupport.Native(() -> syntheticFieldLoader(fullPath(), leafName()));
        }

        return super.syntheticSourceSupport();
    }

    public CompositeSyntheticFieldLoader syntheticFieldLoader(String fullFieldName, String leafFieldName) {
        assert fieldType.stored() || hasDocValues;

        var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
        if (fieldType.stored()) {
            layers.add(new CompositeSyntheticFieldLoader.StoredFieldLayer(fullPath()) {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    BytesRef ref = (BytesRef) value;
                    b.utf8Value(ref.bytes, ref.offset, ref.length);
                }
            });
        } else if (hasDocValues) {
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
        }

        // if ignore_above is set, then there is a chance that this field will be ignored. In such cases, we save an
        // extra copy of the field for supporting synthetic source. This layer will check that copy.
        if (fieldType().ignoreAbove.valuesPotentiallyIgnored()) {
            final String fieldName = fieldType().syntheticSourceFallbackFieldName();
            layers.add(new CompositeSyntheticFieldLoader.StoredFieldLayer(fieldName) {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    BytesRef ref = (BytesRef) value;
                    b.utf8Value(ref.bytes, ref.offset, ref.length);
                }
            });
        }

        return new CompositeSyntheticFieldLoader(leafFieldName, fullFieldName, layers);
    }
}
