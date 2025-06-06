/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.extras.SourceConfirmedTextQuery;
import org.elasticsearch.index.mapper.extras.SourceIntervalsSource;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.support.CoreValuesSourceType.KEYWORD;

public class PatternedTextFieldType extends StringFieldType implements DynamicFieldType {

    private static final String TEMPLATE_SUFFIX = ".template";
    private static final String ARGS_SUFFIX = ".args";

    static final String CONTENT_TYPE = "patterned_text";

    private final Analyzer indexAnalyzer;
    private final TextFieldMapper.TextFieldType textFieldType;
    private final TextFieldMapper.TextFieldType templateFieldType;

    PatternedTextFieldType(String name, TextSearchInfo tsi, Analyzer indexAnalyzer, boolean isSyntheticSource, Map<String, String> meta) {
        super(name, true, false, true, tsi, meta);
        this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
        this.textFieldType = new TextFieldMapper.TextFieldType(name, isSyntheticSource);
        this.templateFieldType = new TextFieldMapper.TextFieldType(name + TEMPLATE_SUFFIX, isSyntheticSource);
        this.templateFieldType.setFielddata(true);  // for aggregations
    }

    PatternedTextFieldType(String name) {
        this(
            name,
            new TextSearchInfo(PatternedTextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            Lucene.STANDARD_ANALYZER,
            false,
            Collections.emptyMap()
        );
    }

    @Override
    public MappedFieldType getChildFieldType(String path) {
        return templateFieldType;
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

    private Query combinedQuery(Query query, Query templateQuery, SearchExecutionContext context) {
        // Disable scoring
        return new ConstantScoreQuery(
            // TODO: skip SourceConfirmedTextQuery when the templateQuery has matches.
            new BooleanQuery.Builder().add(
                new SourceConfirmedTextQuery(query, getValueFetcherProvider(context), indexAnalyzer),
                BooleanClause.Occur.SHOULD
            ).add(templateQuery, BooleanClause.Occur.SHOULD).build()
        );
    }

    private IntervalsSource toIntervalsSource(IntervalsSource source, Query approximation, SearchExecutionContext searchExecutionContext) {
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
        final Query textQuery = textFieldType.phraseQuery(stream, slop, enablePosIncrements, queryShardContext);
        final Query templateQuery = templateFieldType.phraseQuery(stream, slop, enablePosIncrements, queryShardContext);
        return combinedQuery(textQuery, templateQuery, queryShardContext);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext queryShardContext)
        throws IOException {
        final Query textQuery = textFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext);
        final Query templateQuery = templateFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext);
        return combinedQuery(textQuery, templateQuery, queryShardContext);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext queryShardContext)
        throws IOException {
        final Query textQuery = textFieldType.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext);
        final Query templateQuery = templateFieldType.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext);
        return combinedQuery(textQuery, templateQuery, queryShardContext);
    }

    @Override
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        return new PatternedTextBlockLoader(name(), templateFieldName(), argsFieldName());
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
        return new PatternedTextIndexFieldData.Builder(
            name(),
            templateDataBuilder,
            argsDataBuilder
        );
    }

    String templateFieldName() {
        return name() + TEMPLATE_SUFFIX;
    }

    String argsFieldName() {
        return name() + ARGS_SUFFIX;
    }

}
