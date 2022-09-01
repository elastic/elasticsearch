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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.search.DocValuesTermsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton.AUTOMATON_TYPE;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
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
import org.elasticsearch.search.runtime.StringScriptFieldFuzzyQuery;
import org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery;
import org.elasticsearch.search.runtime.StringScriptFieldRegexpQuery;
import org.elasticsearch.search.runtime.StringScriptFieldTermQuery;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;

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

import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;
import static org.elasticsearch.core.Strings.format;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    private static final Logger logger = LogManager.getLogger(KeywordFieldMapper.class);

    public static final String CONTENT_TYPE = "keyword";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static TextSearchInfo TEXT_SEARCH_INFO = new TextSearchInfo(
            FIELD_TYPE,
            null,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER
        );

        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class KeywordField extends Field {

        public KeywordField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
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

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
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
        private final Parameter<Integer> ignoreAbove = Parameter.intParam(
            "ignore_above",
            true,
            m -> toType(m).fieldType().ignoreAbove(),
            Defaults.IGNORE_ABOVE
        );

        private final Parameter<String> indexOptions = TextParams.keywordIndexOptions(m -> toType(m).indexOptions);
        private final Parameter<Boolean> hasNorms = TextParams.norms(false, m -> toType(m).fieldType.omitNorms() == false);
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> toType(m).similarity);

        private final Parameter<String> normalizer;

        private final Parameter<Boolean> splitQueriesOnWhitespace = Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> toType(m).splitQueriesOnWhitespace,
            false
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);
        private final Parameter<Boolean> dimension;

        private final IndexAnalyzers indexAnalyzers;
        private final ScriptCompiler scriptCompiler;
        private final Version indexCreatedVersion;

        public Builder(String name, IndexAnalyzers indexAnalyzers, ScriptCompiler scriptCompiler, Version indexCreatedVersion) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.indexCreatedVersion = Objects.requireNonNull(indexCreatedVersion);
            this.normalizer = Parameter.stringParam(
                "normalizer",
                indexCreatedVersion.isLegacyIndexVersion(),
                m -> toType(m).normalizerName,
                null
            ).acceptsNull();
            this.script.precludesParameters(nullValue);
            addScriptValidation(script, indexed, hasDocValues);

            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).fieldType().isDimension()).addValidator(v -> {
                if (v && (indexed.getValue() == false || hasDocValues.getValue() == false)) {
                    throw new IllegalArgumentException(
                        "Field ["
                            + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM
                            + "] requires that ["
                            + indexed.name
                            + "] and ["
                            + hasDocValues.name
                            + "] are true"
                    );
                }
            }).precludesParameters(normalizer, ignoreAbove);
        }

        public Builder(String name, Version indexCreatedVersion) {
            this(name, null, ScriptCompiler.NONE, indexCreatedVersion);
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove.setValue(ignoreAbove);
            return this;
        }

        Builder normalizer(String normalizerName) {
            this.normalizer.setValue(normalizerName);
            return this;
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        private FieldValues<String> scriptValues() {
            if (script.get() == null) {
                return null;
            }
            StringFieldScript.Factory scriptFactory = scriptCompiler.compile(script.get(), StringFieldScript.CONTEXT);
            return scriptFactory == null
                ? null
                : (lookup, ctx, doc, consumer) -> scriptFactory.newFactory(name, script.get().getParams(), lookup)
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
                        throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + name + "]");
                    }
                }
                searchAnalyzer = quoteAnalyzer = normalizer;
                if (splitQueriesOnWhitespace.getValue()) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                }
            } else if (splitQueriesOnWhitespace.getValue()) {
                searchAnalyzer = Lucene.WHITESPACE_ANALYZER;
            }
            return new KeywordFieldType(context.buildFullName(name), fieldType, normalizer, searchAnalyzer, quoteAnalyzer, this);
        }

        @Override
        public KeywordFieldMapper build(MapperBuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            fieldtype.setOmitNorms(this.hasNorms.getValue() == false);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(this.indexed.getValue(), this.indexOptions.getValue()));
            fieldtype.setStored(this.stored.getValue());
            if (fieldtype.equals(Defaults.FIELD_TYPE)) {
                // deduplicate in the common default case to save some memory
                fieldtype = Defaults.FIELD_TYPE;
            }
            return new KeywordFieldMapper(
                name,
                fieldtype,
                buildFieldType(context, fieldtype),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    private static final Version MINIMUM_COMPATIBILITY_VERSION = Version.fromString("5.0.0");

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.getIndexAnalyzers(), c.scriptCompiler(), c.indexVersionCreated()),
        MINIMUM_COMPATIBILITY_VERSION
    );

    public static final class KeywordFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;
        private final NamedAnalyzer normalizer;
        private final boolean eagerGlobalOrdinals;
        private final FieldValues<String> scriptValues;
        private final boolean isDimension;

        public KeywordFieldType(
            String name,
            FieldType fieldType,
            NamedAnalyzer normalizer,
            NamedAnalyzer searchAnalyzer,
            NamedAnalyzer quoteAnalyzer,
            Builder builder
        ) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE && builder.indexCreatedVersion.isLegacyIndexVersion() == false,
                fieldType.stored(),
                builder.hasDocValues.getValue(),
                textSearchInfo(fieldType, builder.similarity.getValue(), searchAnalyzer, quoteAnalyzer),
                builder.meta.getValue()
            );
            this.eagerGlobalOrdinals = builder.eagerGlobalOrdinals.getValue();
            this.normalizer = normalizer;
            this.ignoreAbove = builder.ignoreAbove.getValue();
            this.nullValue = builder.nullValue.getValue();
            this.scriptValues = builder.scriptValues();
            this.isDimension = builder.dimension.getValue();
        }

        public KeywordFieldType(String name, boolean isIndexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, isIndexed, false, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
        }

        public KeywordFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        public KeywordFieldType(String name, FieldType fieldType) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                false,
                false,
                textSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
        }

        public KeywordFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, false, true, textSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
            this.normalizer = Lucene.KEYWORD_ANALYZER;
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.eagerGlobalOrdinals = false;
            this.scriptValues = null;
            this.isDimension = false;
        }

        @Override
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (isIndexed()) {
                return super.termQuery(value, context);
            } else {
                return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (isIndexed()) {
                return super.termsQuery(values, context);
            } else {
                BytesRef[] bytesRefs = values.stream().map(this::indexedValueForSearch).toArray(BytesRef[]::new);
                return new DocValuesTermsQuery(name(), bytesRefs);
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
            if (isIndexed()) {
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
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (isIndexed()) {
                return super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context);
            } else {
                return StringScriptFieldFuzzyQuery.build(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    fuzziness.asDistance(BytesRefs.toString(value)),
                    prefixLength,
                    transpositions
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
            if (isIndexed()) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            } else {
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
            if (isIndexed()) {
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
        public TermsEnum getTerms(boolean caseInsensitive, String string, SearchExecutionContext queryShardContext, String searchAfter)
            throws IOException {
            IndexReader reader = queryShardContext.searcher().getTopReaderContext().reader();

            Terms terms = null;
            if (isIndexed()) {
                terms = MultiTerms.getTerms(reader, name());
            } else if (hasDocValues()) {
                terms = SortedSetDocValuesTerms.getTerms(reader, name());
            }
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }
            Automaton a = caseInsensitive ? AutomatonQueries.caseInsensitivePrefix(string) : Automata.makeString(string);
            a = Operations.concatenate(a, Automata.makeAnyString());
            a = MinimizationOperations.minimize(a, Integer.MAX_VALUE);

            CompiledAutomaton automaton = new CompiledAutomaton(a);

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

        // Initialises with a seek to a given term but excludes that term
        // from any results. The problem it addresses is that termsEnum.seekCeil()
        // would work but either leaves us positioned on the seek term (if it exists) or the
        // term after (if the seek term doesn't exist). That complicates any subsequent
        // iteration logic so this class simplifies the pagination use case.
        static final class SearchAfterTermsEnum extends FilteredTermsEnum {
            private final BytesRef afterRef;

            SearchAfterTermsEnum(TermsEnum tenum, BytesRef termText) {
                super(tenum);
                afterRef = termText;
                setInitialSeekTerm(termText);
            }

            @Override
            protected AcceptStatus accept(BytesRef term) {
                return term.equals(afterRef) ? AcceptStatus.NO : AcceptStatus.YES;
            }
        }

        /**
         * A simple terms implementation for SortedSetDocValues that only provides access to {@link TermsEnum} via
         * {@link #iterator} and {@link #intersect(CompiledAutomaton, BytesRef)} methods.
         * We have this custom implementation based on {@link MultiTerms} instead of using
         * {@link org.apache.lucene.index.MultiDocValues#getSortedSetValues(IndexReader, String)}
         * because {@link org.apache.lucene.index.MultiDocValues} builds global ordinals up-front whereas
         * {@link MultiTerms}, which exposes the terms enum via {@link org.apache.lucene.index.MultiTermsEnum},
         * merges terms on the fly.
         */
        static class SortedSetDocValuesTerms extends Terms {

            public static Terms getTerms(IndexReader r, String field) throws IOException {
                final List<LeafReaderContext> leaves = r.leaves();
                if (leaves.size() == 1) {
                    SortedSetDocValues sortedSetDocValues = leaves.get(0).reader().getSortedSetDocValues(field);
                    if (sortedSetDocValues == null) {
                        return null;
                    } else {
                        return new SortedSetDocValuesTerms(sortedSetDocValues);
                    }
                }

                final List<Terms> termsPerLeaf = new ArrayList<>(leaves.size());
                final List<ReaderSlice> slicePerLeaf = new ArrayList<>(leaves.size());

                for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
                    LeafReaderContext ctx = leaves.get(leafIdx);
                    SortedSetDocValues sortedSetDocValues = ctx.reader().getSortedSetDocValues(field);
                    if (sortedSetDocValues != null) {
                        termsPerLeaf.add(new SortedSetDocValuesTerms(sortedSetDocValues));
                        slicePerLeaf.add(new ReaderSlice(ctx.docBase, r.maxDoc(), leafIdx));
                    }
                }

                if (termsPerLeaf.isEmpty()) {
                    return null;
                } else {
                    return new MultiTerms(termsPerLeaf.toArray(EMPTY_ARRAY), slicePerLeaf.toArray(ReaderSlice.EMPTY_ARRAY));
                }
            }

            private final SortedSetDocValues values;

            SortedSetDocValuesTerms(SortedSetDocValues values) {
                this.values = values;
            }

            @Override
            public TermsEnum iterator() throws IOException {
                return values.termsEnum();
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
                if (startTerm == null) {
                    return values.intersect(compiled);
                } else {
                    return super.intersect(compiled, startTerm);
                }
            }

            @Override
            public long size() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getSumDocFreq() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getDocCount() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasFreqs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasOffsets() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasPositions() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasPayloads() {
                throw new UnsupportedOperationException();
            }

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
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return new SortedSetOrdinalsIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
                );
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    sourceValueFetcher(sourcePaths),
                    searchLookup.source(),
                    KeywordDocValuesField::new
                );
            }

            throw new IllegalStateException("unknown field data type [" + operation.name() + "]");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (this.scriptValues != null) {
                return FieldValues.valueFetcher(this.scriptValues, context);
            }
            return sourceValueFetcher(context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet());
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths) {
            return new SourceValueFetcher(sourcePaths, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    if (keywordValue.length() > ignoreAbove) {
                        return null;
                    }

                    return normalizeValue(normalizer(), name(), keywordValue);
                }
            };
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
            if (isIndexed()) {
                return super.wildcardQuery(value, method, caseInsensitive, true, context);
            } else {
                if (getTextSearchInfo().searchAnalyzer() != null) {
                    value = normalizeWildcardPattern(name(), value, getTextSearchInfo().searchAnalyzer());
                } else {
                    value = indexedValueForSearch(value).utf8ToString();
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
            if (isIndexed()) {
                return super.normalizedWildcardQuery(value, method, context);
            } else {
                if (getTextSearchInfo().searchAnalyzer() != null) {
                    value = normalizeWildcardPattern(name(), value, getTextSearchInfo().searchAnalyzer());
                } else {
                    value = indexedValueForSearch(value).utf8ToString();
                }
                return new StringScriptFieldWildcardQuery(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    value,
                    false
                );
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
            if (isIndexed()) {
                return super.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
            } else {
                if (matchFlags != 0) {
                    throw new IllegalArgumentException("Match flags not yet implemented [" + matchFlags + "]");
                }
                return new StringScriptFieldRegexpQuery(
                    new Script(""),
                    ctx -> new SortedSetDocValuesStringFieldScript(name(), context.lookup(), ctx),
                    name(),
                    indexedValueForSearch(value).utf8ToString(),
                    syntaxFlags,
                    matchFlags,
                    maxDeterminizedStates
                );
            }
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.KEYWORD;
        }

        /** Values that have more chars than the return value of this method will
         *  be skipped at parsing time. */
        public int ignoreAbove() {
            return ignoreAbove;
        }

        /**
         * @return true if field has been marked as a dimension field
         */
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public void validateMatchedRoutingPath() {
            if (false == isDimension) {
                throw new IllegalArgumentException(
                    "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                        + "and without the [script] parameter. ["
                        + name()
                        + "] was not [time_series_dimension: true]."
                );
            }
            if (scriptValues != null) {
                throw new IllegalArgumentException(
                    "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                        + "and without the [script] parameter. ["
                        + name()
                        + "] has a [script] parameter."
                );
            }
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final String indexOptions;
    private final FieldType fieldType;
    private final SimilarityProvider similarity;
    private final String normalizerName;
    private final boolean splitQueriesOnWhitespace;
    private final Script script;
    private final ScriptCompiler scriptCompiler;
    private final Version indexCreatedVersion;

    private final IndexAnalyzers indexAnalyzers;

    private KeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        KeywordFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, builder.script.get() != null, builder.onScriptError.getValue());
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.fieldType = fieldType;
        this.similarity = builder.similarity.getValue();
        this.normalizerName = builder.normalizer.getValue();
        this.splitQueriesOnWhitespace = builder.splitQueriesOnWhitespace.getValue();
        this.script = builder.script.get();
        this.indexAnalyzers = builder.indexAnalyzers;
        this.scriptCompiler = builder.scriptCompiler;
        this.indexCreatedVersion = builder.indexCreatedVersion;
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        indexValue(context, value == null ? fieldType().nullValue : value);
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

    private void indexValue(DocumentParserContext context, String value) {
        if (value == null) {
            return;
        }

        if (value.length() > fieldType().ignoreAbove()) {
            context.addIgnoredField(name());
            return;
        }

        value = normalizeValue(fieldType().normalizer(), name(), value);
        if (fieldType().isDimension()) {
            context.getDimensions().addString(fieldType().name(), value);
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);

        // If the UTF8 encoding of the field value is bigger than the max length 32766, Lucene fill fail the indexing request and, to roll
        // back the changes, will mark the (possibly partially indexed) document as deleted. This results in deletes, even in an append-only
        // workload, which in turn leads to slower merges, as these will potentially have to fall back to MergeStrategy.DOC instead of
        // MergeStrategy.BULK. To avoid this, we do a preflight check here before indexing the document into Lucene.
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

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new KeywordField(fieldType().name(), binaryValue, fieldType);
            context.doc().add(field);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                context.addToFieldNames(fieldType().name());
            }
        }

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
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
        return new Builder(simpleName(), indexAnalyzers, scriptCompiler, indexCreatedVersion).dimension(fieldType().isDimension())
            .init(this);
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (fieldType().isDimension() && null != lookup.nestedLookup().getNestedParent(name())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + name() + "]"
            );
        }
    }

    boolean hasNormalizer() {
        return normalizerName != null;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return syntheticFieldLoader(simpleName());
    }

    protected SourceLoader.SyntheticFieldLoader syntheticFieldLoader(String simpleName) {
        if (hasScript()) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }
        if (fieldType().ignoreAbove() != Defaults.IGNORE_ABOVE) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares ignore_above"
            );
        }
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        if (hasNormalizer()) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares a normalizer"
            );
        }
        if (fieldType.stored()) {
            return new StringStoredFieldFieldLoader(name(), simpleName) {
                @Override
                public void load(List<Object> values) {
                    super.load(values.stream().map(fieldType()::valueForDisplay).toList());
                }
            };
        }
        if (hasDocValues == false) {
            throw new IllegalArgumentException(
                "field ["
                    + name()
                    + "] of type ["
                    + typeName()
                    + "] doesn't support synthetic source because it doesn't have doc values and isn't stored"
            );
        }
        return new SortedSetDocValuesSyntheticFieldLoader(name(), simpleName) {
            @Override
            protected BytesRef convert(BytesRef value) {
                return value;
            }

            @Override
            protected BytesRef preserve(BytesRef value) {
                // Preserve must make a deep copy because convert gets a shallow copy from the iterator
                return BytesRef.deepCopyOf(value);
            }
        };
    }

}
