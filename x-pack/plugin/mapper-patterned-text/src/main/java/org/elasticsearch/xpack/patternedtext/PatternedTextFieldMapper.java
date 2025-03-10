/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.extras.SourceIntervalsSource;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DateMillisDocValuesField;
import org.elasticsearch.script.field.KeywordDocValuesField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.support.CoreValuesSourceType.KEYWORD;

/**
 * A {@link FieldMapper} that assigns every document the same value.
 */
public class PatternedTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "patterned_text";
    private static final String TEMPLATE_SUFFIX = ".template";
    private static final String ARGS_SUFFIX = ".args";
    private static final String TIMESTAMP_SUFFIX = ".ts";
    private static final String SORT_SUFFIX = ".sort";
    static final int OPTIMIZED_ARG_COUNT = 0;

    public static class Defaults {
        public static final FieldType FIELD_TYPE;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

    }

    public static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final TextParams.Analyzers analyzers;

        public Builder(String name, IndexVersion indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((PatternedTextFieldMapper) m).indexAnalyzer,
                m -> ((PatternedTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        private Type buildFieldType(MapperBuilderContext context) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            TextSearchInfo tsi = new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchQuoteAnalyzer);
            return new Type(context.buildFullName(leafName()), tsi, indexAnalyzer, context.isSourceSynthetic(), meta.getValue());
        }

        @Override
        public PatternedTextFieldMapper build(MapperBuilderContext context) {
            Type type = buildFieldType(context);
            return new PatternedTextFieldMapper(leafName(), Defaults.FIELD_TYPE, type, builderParams(this, context), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    public static class Type extends StringFieldType {

        private final Analyzer indexAnalyzer;
        private final TextFieldMapper.TextFieldType textFieldType;

        public Type(String name, TextSearchInfo tsi, Analyzer indexAnalyzer, boolean isSyntheticSource, Map<String, String> meta) {
            super(name, true, false, true, tsi, meta);
            this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
            this.textFieldType = new TextFieldMapper.TextFieldType(name, isSyntheticSource);
        }

        public Type(String name) {
            this(
                name,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Lucene.STANDARD_ANALYZER,
                false,
                Collections.emptyMap()
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return TextFieldMapper.CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> getValueFetcherProvider(
            SearchExecutionContext searchExecutionContext
        ) {
            return context -> {
                ValueFetcher valueFetcher = valueFetcher(searchExecutionContext, null);
                valueFetcher.setNextReader(context);
                return docID -> {
                    try {
                        return valueFetcher.fetchValues(null, docID, new ArrayList<>());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };
            };
        }

        private Query toQuery(Query query, SearchExecutionContext context) {
            // Disable scoring
            return new ConstantScoreQuery(query);
        }

        private IntervalsSource toIntervalsSource(
            IntervalsSource source,
            Query approximation,
            SearchExecutionContext searchExecutionContext
        ) {
            return new SourceIntervalsSource(source, approximation, getValueFetcherProvider(searchExecutionContext), indexAnalyzer);
        }

        @Override
        public Query termQuery(Object query, SearchExecutionContext context) {
            // Disable scoring
            return new ConstantScoreQuery(super.termQuery(query, context));
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context,
            MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            // Disable scoring
            return new ConstantScoreQuery(
                super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context, rewriteMethod)
            );
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(templateFieldName());
        }

        @Override
        public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
            return toIntervalsSource(Intervals.term(term), new TermQuery(new Term(name(), term)), context);
        }

        @Override
        public IntervalsSource prefixIntervals(BytesRef term, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.prefix(term, IndexSearcher.getMaxClauseCount()),
                new PrefixQuery(new Term(name(), term)),
                context
            );
        }

        @Override
        public IntervalsSource fuzzyIntervals(
            String term,
            int maxDistance,
            int prefixLength,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            FuzzyQuery fuzzyQuery = new FuzzyQuery(
                new Term(name(), term),
                maxDistance,
                prefixLength,
                IndexSearcher.getMaxClauseCount(),
                transpositions,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
            );
            IntervalsSource fuzzyIntervals = Intervals.multiterm(fuzzyQuery.getAutomata(), IndexSearcher.getMaxClauseCount(), term);
            return toIntervalsSource(fuzzyIntervals, fuzzyQuery, context);
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.wildcard(pattern, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // wildcard queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.regexp(pattern, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // regexp queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public IntervalsSource rangeIntervals(
            BytesRef lowerTerm,
            BytesRef upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            return toIntervalsSource(
                Intervals.range(lowerTerm, upperTerm, includeLower, includeUpper, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // range queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext queryShardContext)
            throws IOException {
            final Query query = textFieldType.phraseQuery(stream, slop, enablePosIncrements, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public Query multiPhraseQuery(
            TokenStream stream,
            int slop,
            boolean enablePositionIncrements,
            SearchExecutionContext queryShardContext
        ) throws IOException {
            final Query query = textFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext queryShardContext)
            throws IOException {
            final Query query = textFieldType.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new PatternedTextBlockLoader(name(), templateFieldName(), timestampFieldName(), argsFieldName());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            var templateDataBuilder = new SortedSetOrdinalsIndexFieldData.Builder(
                templateFieldName(),
                KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
            var argsDataBuilder = new SortedSetOrdinalsIndexFieldData.Builder(
                argsFieldName(),
                KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
            var optimizedArgsDataBuilder = new SortedSetOrdinalsIndexFieldData.Builder[OPTIMIZED_ARG_COUNT];
            for (int i = 0; i < optimizedArgsDataBuilder.length; i++) {
                optimizedArgsDataBuilder[i] = new SortedSetOrdinalsIndexFieldData.Builder(
                    argsFieldName() + "." + i,
                    KEYWORD,
                    (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
                );
            }
            var timestampDataBuilder = new SortedNumericIndexFieldData.Builder(
                timestampFieldName(),
                IndexNumericFieldData.NumericType.LONG,
                DateMillisDocValuesField::new,
                false
            );
            return new PatternedTextIndexFieldData.Builder(
                name(),
                templateDataBuilder,
                argsDataBuilder,
                optimizedArgsDataBuilder,
                timestampDataBuilder
            );
        }

        private String templateFieldName() {
            return name() + TEMPLATE_SUFFIX;
        }

        private String argsFieldName() {
            return name() + ARGS_SUFFIX;
        }

        private String timestampFieldName() {
            return name() + TIMESTAMP_SUFFIX;
        }

        private String sortFieldName() {
            return name() + SORT_SUFFIX;
        }
    }

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final int positionIncrementGap;
    private final FieldType fieldType;

    private PatternedTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        Type mappedFieldType,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues();
        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexAnalyzers).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(value);
        context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

        if (parts.timestamp() != null) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().timestampFieldName(), parts.timestamp()));
        }

        for (int i = 0; i < Integer.min(parts.args().size(), OPTIMIZED_ARG_COUNT); i++) {
            String argFieldname = fieldType().argsFieldName() + "." + i;
            context.doc().add(new SortedSetDocValuesField(argFieldname, new BytesRef(parts.args().get(i))));
        }
        if (parts.args().size() > OPTIMIZED_ARG_COUNT) {
            String remainingArgs = PatternedTextValueProcessor.mergeRemainingArgs(parts, OPTIMIZED_ARG_COUNT);
            context.doc().add(new SortedSetDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
        }

        // Index postings.
        context.doc().add(new Field(fieldType().name(), parts.indexed(), fieldType));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Type fieldType() {
        return (Type) super.fieldType();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(
                leafName(),
                fullPath(),
                new PatternedTextSyntheticFieldLoaderLayer(
                    fieldType().templateFieldName(),
                    fieldType().timestampFieldName(),
                    fieldType().argsFieldName()
                )
            )
        );
    }
}
