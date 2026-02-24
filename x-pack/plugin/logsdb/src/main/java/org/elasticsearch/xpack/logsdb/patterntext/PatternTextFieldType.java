/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.TextFamilyFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.extras.SourceConfirmedTextQuery;
import org.elasticsearch.index.mapper.extras.SourceIntervalsSource;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PatternTextFieldType extends TextFamilyFieldType {

    private static final String STORED_SUFFIX = ".stored";
    private static final String TEMPLATE_SUFFIX = ".template";
    private static final String TEMPLATE_ID_SUFFIX = ".template_id";
    private static final String ARGS_SUFFIX = ".args";
    private static final String ARGS_INFO_SUFFIX = ".args_info";

    public static final String CONTENT_TYPE = "pattern_text";

    private final Analyzer indexAnalyzer;
    private final TextFieldMapper.TextFieldType textFieldType;
    private final boolean hasPositions;
    private final boolean disableTemplating;
    private final boolean useBinaryDocValuesArgs;
    private final boolean useBinaryDocValuesRawText;

    PatternTextFieldType(
        String name,
        TextSearchInfo tsi,
        Analyzer indexAnalyzer,
        boolean disableTemplating,
        Map<String, String> meta,
        boolean isSyntheticSource,
        boolean isWithinMultiField,
        boolean useBinaryDocValueArgs,
        boolean useBinaryDocValuesRawText
    ) {
        // Though this type is based on doc_values, hasDocValues is set to false as the pattern_text type is not aggregatable.
        // This does not stop its child .template type from being aggregatable.
        super(name, IndexType.terms(true, false), false, tsi, meta, isSyntheticSource, isWithinMultiField);
        this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
        this.textFieldType = new TextFieldMapper.TextFieldType(name, isSyntheticSource, isWithinMultiField);
        this.hasPositions = tsi.hasPositions();
        this.disableTemplating = disableTemplating;
        this.useBinaryDocValuesArgs = useBinaryDocValueArgs;
        this.useBinaryDocValuesRawText = useBinaryDocValuesRawText;
    }

    // For testing only
    PatternTextFieldType(String name, boolean hasPositions, boolean syntheticSource, boolean useBinaryDocValueArgs) {
        this(
            name,
            new TextSearchInfo(
                hasPositions ? PatternTextFieldMapper.Defaults.FIELD_TYPE_POSITIONS : PatternTextFieldMapper.Defaults.FIELD_TYPE_DOCS,
                null,
                DelimiterAnalyzer.INSTANCE,
                DelimiterAnalyzer.INSTANCE
            ),
            DelimiterAnalyzer.INSTANCE,
            false,
            Collections.emptyMap(),
            syntheticSource,
            false,
            useBinaryDocValueArgs,
            true
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
        return new ValueFetcher() {
            BinaryDocValues docValues;

            @Override
            public void setNextReader(LeafReaderContext context) {
                try {
                    this.docValues = PatternTextFallbackDocValues.from(context.reader(), PatternTextFieldType.this);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                if (false == docValues.advanceExact(doc)) {
                    return List.of();
                }
                return List.of(docValues.binaryValue().utf8ToString());
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                // PatternedTextCompositeValues may require a stored field, but it handles loading this field internally.
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }

    private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> getValueFetcherProvider(
        SearchExecutionContext searchExecutionContext
    ) {
        if (disableTemplating) {
            return useBinaryDocValuesRawText ? binaryDocValuesFetcher(storedNamed()) : storedFieldFetcher(storedNamed());
        }

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

    private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> binaryDocValuesFetcher(String name) {
        return context -> {
            var docValues = context.reader().getBinaryDocValues(name);
            return docId -> {
                if (docValues != null && docValues.advanceExact(docId)) {
                    return List.of(docValues.binaryValue());
                }
                return List.of();
            };
        };
    }

    private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> storedFieldFetcher(String name) {
        var loader = StoredFieldLoader.create(false, Set.of(name));
        return context -> {
            var leafLoader = loader.getLoader(context, null);
            return docId -> {
                leafLoader.advanceTo(docId);
                var storedFields = leafLoader.storedFields();
                var values = storedFields.get(name);
                return values != null ? values : List.of();
            };
        };
    }

    private Query maybeSourceConfirmQuery(Query query, SearchExecutionContext context) {
        // Disable scoring similarly to match_only_text
        if (hasPositions) {
            return new ConstantScoreQuery(query);
        } else {
            return new ConstantScoreQuery(new SourceConfirmedTextQuery(query, getValueFetcherProvider(context), indexAnalyzer));
        }
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
        return new FieldExistsQuery(templateIdFieldName());
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
            Queries.ALL_DOCS_INSTANCE, // wildcard queries can be expensive, what should the approximation be?
            context
        );
    }

    @Override
    public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
        return toIntervalsSource(
            Intervals.regexp(pattern, IndexSearcher.getMaxClauseCount()),
            Queries.ALL_DOCS_INSTANCE, // regexp queries can be expensive, what should the approximation be?
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
            Queries.ALL_DOCS_INSTANCE, // range queries can be expensive, what should the approximation be?
            context
        );
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext queryShardContext)
        throws IOException {
        final Query textQuery = textFieldType.phraseQuery(stream, slop, enablePosIncrements, queryShardContext);
        return maybeSourceConfirmQuery(textQuery, queryShardContext);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext queryShardContext)
        throws IOException {
        final Query textQuery = textFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext);
        return maybeSourceConfirmQuery(textQuery, queryShardContext);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext queryShardContext)
        throws IOException {
        final Query textQuery = textFieldType.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext);
        return maybeSourceConfirmQuery(textQuery, queryShardContext);
    }

    @Override
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        if (disableTemplating) {
            if (useBinaryDocValuesRawText) {
                // for newer indices, raw pattern text values are stored in binary doc values
                return new BytesRefsFromBinaryBlockLoader(storedNamed());
            } else {
                // for older indices (bwc), raw pattern text values are stored in stored fields
                return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(storedNamed());
            }
        }

        return new BytesRefsFromBinaryBlockLoader(leafReader -> PatternTextFallbackDocValues.from(leafReader, this));
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        if (fieldDataContext.fielddataOperation() != FielddataOperation.SCRIPT) {
            throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support sorting and aggregations");
        }
        return new PatternTextIndexFieldData.Builder(this);
    }

    String templateFieldName() {
        return name() + TEMPLATE_SUFFIX;
    }

    String templateIdFieldName() {
        return name() + TEMPLATE_ID_SUFFIX;
    }

    String templateIdFieldName(String leafName) {
        return leafName + TEMPLATE_ID_SUFFIX;
    }

    String argsFieldName() {
        return name() + ARGS_SUFFIX;
    }

    String argsInfoFieldName() {
        return name() + ARGS_INFO_SUFFIX;
    }

    String storedNamed() {
        return name() + STORED_SUFFIX;
    }

    boolean disableTemplating() {
        return disableTemplating;
    }

    boolean useBinaryDocValuesArgs() {
        return useBinaryDocValuesArgs;
    }

}
