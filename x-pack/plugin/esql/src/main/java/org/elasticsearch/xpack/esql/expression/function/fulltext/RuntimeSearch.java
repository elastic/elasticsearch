/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TokenStreamMatching;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;

/**
 * Runtime (per-row) evaluation of full-text functions on {@code text} expressions that are not index-mapped fields,
 * where there is no Lucene index to query. Instead, each value is analyzed on the fly and the resulting token stream
 * is matched directly against the analyzed query terms.
 * <p>
 * The block-level walking (multivalue any-value semantics, null handling, per-thread scratch) is shared here through
 * a single {@code Text} evaluator. How one value's tokens are matched is Elasticsearch's to say, and comes from
 * {@link TokenStreamMatching}: {@link TokenStreamMatching.AnyTerm} for {@code match},
 * {@link TokenStreamMatching.Phrase} for {@code match_phrase}.
 */
public final class RuntimeSearch {

    public static final String CONTENT_FIELD = "content_field";

    private static final Similarity BOOLEAN_SIMILARITY = new BooleanSimilarity();

    private RuntimeSearch() {}

    /**
     * Exact (unanalyzed) value equality, shared by the runtime full-text functions for types that a pushed-down
     * query matches as a single term: {@code keyword} for {@code match} and {@code match_phrase} (and {@code ip},
     * {@code version} etc. for {@code match}, whose query value is converted up front).
     */
    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean processBytesRef(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed BytesRef queryStringBytesRef,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        if (fieldBlock == null) {
            return false;
        }

        return fieldBlock.hasValue(position, queryStringBytesRef, scratch);
    }

    @Evaluator(extraName = "Text", warnExceptions = { IOException.class }, allNullsIsNull = false)
    static boolean processText(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed TokenStreamMatching.Matcher matcher,
        @Fixed Analyzer analyzer,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) throws IOException {
        if (fieldBlock == null) {
            return false;
        }

        final var valueCount = fieldBlock.getValueCount(position);
        final var startIndex = fieldBlock.getFirstValueIndex(position);

        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            boolean foundMatch;
            scratch = fieldBlock.getBytesRef(valueIndex, scratch);
            // The analyzed token stream is matched directly; queries with options (or phrase queries under a
            // non-standard analyzer, where position gaps matter) take the Lucene MemoryIndex path instead.
            try (TokenStream stream = analyzer.tokenStream(CONTENT_FIELD, scratch.utf8ToString())) {
                stream.reset();
                foundMatch = matcher.matches(stream);
                stream.end();
            }
            if (foundMatch) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds an {@link ExpressionEvaluator.Factory} for runtime {@code text} matching when the caller supplies a
     * {@link org.elasticsearch.xpack.esql.core.querydsl.query.Query} (e.g. a {@code MatchQuery} with options).
     * The query is compiled once into a Lucene {@link Query} against a synthetic
     * {@link RuntimeSearchExecutionContext}, and each row is then matched by indexing its value into a temporary
     * {@link MemoryIndex}. Like an indexed text field's {@code search_analyzer}/{@code analyzer} pair, the
     * {@code queryAnalyzer} covers the query string and the {@code valuesAnalyzer} covers the per-row values;
     * {@code null} selects the standard analyzer for either.
     */
    public static ExpressionEvaluator.Factory textEvaluatorForQuery(
        Source source,
        ExpressionEvaluator.Factory fieldEvaluator,
        org.elasticsearch.xpack.esql.core.querydsl.query.Query query,
        @Nullable NamedAnalyzer queryAnalyzer,
        @Nullable NamedAnalyzer valuesAnalyzer
    ) {
        Query luceneQuery = compileQuery(query, queryAnalyzer == null ? Lucene.STANDARD_ANALYZER : queryAnalyzer);
        if (luceneQuery instanceof MatchAllDocsQuery) {
            return ConstantEvaluators.CONSTANT_TRUE_FACTORY;
        }
        if (luceneQuery instanceof MatchNoDocsQuery) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        return new RuntimeSearchTextWithLuceneQueryEvaluator.Factory(
            source,
            fieldEvaluator,
            withPositionIncrementGap(valuesAnalyzer == null ? Lucene.STANDARD_ANALYZER : valuesAnalyzer),
            luceneQuery,
            context -> new MemoryIndex(),
            context -> new BytesRef()
        );
    }

    /**
     * The scoring counterpart of {@link #textEvaluatorForQuery}: builds an {@link ExpressionEvaluator.Factory}
     * evaluating per-row scores for the same compiled Lucene query. Scores use {@link BooleanSimilarity} — a
     * matching clause scores its boost, with no corpus statistics involved — so a match query scores
     * {@code boost x matched terms} and a phrase query scores its boost. A query rewriting to match-all
     * ({@code zero_terms_query: all}) scores a constant 1.0 and match-none scores 0.0.
     */
    public static ExpressionEvaluator.Factory textScoreEvaluatorForQuery(
        Source source,
        ExpressionEvaluator.Factory fieldEvaluator,
        org.elasticsearch.xpack.esql.core.querydsl.query.Query query,
        @Nullable NamedAnalyzer queryAnalyzer,
        @Nullable NamedAnalyzer valuesAnalyzer
    ) {
        Query luceneQuery = compileQuery(query, queryAnalyzer == null ? Lucene.STANDARD_ANALYZER : queryAnalyzer);
        if (luceneQuery instanceof MatchAllDocsQuery) {
            return ConstantEvaluators.constantDouble(1.0);
        }
        if (luceneQuery instanceof MatchNoDocsQuery) {
            return ConstantEvaluators.constantDouble(0.0);
        }
        return new RuntimeSearchScoreLuceneQueryEvaluator.Factory(
            source,
            fieldEvaluator,
            withPositionIncrementGap(valuesAnalyzer == null ? Lucene.STANDARD_ANALYZER : valuesAnalyzer),
            luceneQuery,
            context -> new MemoryIndex(),
            context -> new BytesRef()
        );
    }

    /**
     * Compiles an ES|QL query into a Lucene {@link Query} on {@link #CONTENT_FIELD} against a synthetic
     * {@link RuntimeSearchExecutionContext} using the given analyzer, which must be the analyzer the per-row
     * evaluator indexes with so tokenization stays consistent.
     */
    static Query compileQuery(org.elasticsearch.xpack.esql.core.querydsl.query.Query query, NamedAnalyzer namedAnalyzer) {
        try {
            return query.toQueryBuilder().toQuery(RuntimeSearchExecutionContext.create(List.of(CONTENT_FIELD), namedAnalyzer));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The values of a multivalued position are indexed as one document, so give the analyzer the same position
     * increment gap as an indexed text field, keeping phrases from matching across value boundaries.
     */
    private static NamedAnalyzer withPositionIncrementGap(NamedAnalyzer analyzer) {
        return new NamedAnalyzer(analyzer, TextFieldMapper.Defaults.POSITION_INCREMENT_GAP);
    }

    /**
     * Resolves the {@code analyzer} entry of a full-text function's options map into a {@link NamedAnalyzer}
     * through the evaluator context's registry lookup. Returns {@code null} when no analyzer was requested.
     * Option map values are untyped ({@code Options.populateMap} does not guarantee {@code String} for keyword
     * options), so the conversion is centralized here.
     */
    @Nullable
    public static NamedAnalyzer resolveNamedAnalyzer(Map<String, Object> options, EvaluatorMapper.ToEvaluator toEvaluator) {
        Object analyzerName = options.get(ANALYZER_FIELD.getPreferredName());
        return analyzerName == null ? null : resolveNamedAnalyzer(BytesRefs.toString(analyzerName), toEvaluator);
    }

    /**
     * Resolves an analyzer name (e.g. the values analyzer declared through {@code TO_TEXT}) into a
     * {@link NamedAnalyzer} through the evaluator context's registry lookup. Returns {@code null} for a
     * {@code null} name.
     */
    @Nullable
    public static NamedAnalyzer resolveNamedAnalyzer(@Nullable String name, EvaluatorMapper.ToEvaluator toEvaluator) {
        if (name == null) {
            return null;
        }
        return toNamedAnalyzer(name, toEvaluator.getAnalyzer(name));
    }

    /**
     * Wraps an analyzer resolved by {@code name} as a {@link NamedAnalyzer}. Registry lookups return one in practice,
     * but the lookup interfaces only promise an {@link Analyzer}.
     */
    static NamedAnalyzer toNamedAnalyzer(String name, Analyzer analyzer) {
        return analyzer instanceof NamedAnalyzer namedAnalyzer ? namedAnalyzer : new NamedAnalyzer(name, AnalyzerScope.GLOBAL, analyzer);
    }

    /**
     * Indexes all values of one position into {@code memoryIndex} as one document, like an indexed multivalued text
     * field: query terms may match across values, while the analyzer's position increment gap keeps phrases within
     * a single value. Returns the scratch buffer to reuse for the next row.
     */
    static BytesRef indexRow(MemoryIndex memoryIndex, BytesRefBlock fieldBlock, int position, Analyzer analyzer, BytesRef scratch) {
        memoryIndex.reset();
        int first = fieldBlock.getFirstValueIndex(position);
        int end = first + fieldBlock.getValueCount(position);
        for (int valueIndex = first; valueIndex < end; valueIndex++) {
            scratch = fieldBlock.getBytesRef(valueIndex, scratch);
            memoryIndex.addField(CONTENT_FIELD, scratch.utf8ToString(), analyzer);
        }
        return scratch;
    }

    /**
     * The distinct terms of a compiled runtime query on {@link #CONTENT_FIELD}, sorted. They are collected with
     * Lucene's {@link QueryVisitor}, so synonym expansions and phrase terms are included. This is the term list the
     * BM25 statistics pass counts.
     */
    static List<BytesRef> collectTerms(Query query) {
        Set<Term> terms = new HashSet<>();
        query.visit(QueryVisitor.termCollector(terms));
        return terms.stream()
            .filter(term -> CONTENT_FIELD.equals(term.field()))
            .map(term -> BytesRef.deepCopyOf(term.bytes()))
            .distinct()
            .sorted()
            .toList();
    }

    /**
     * Whether the compiled query expands terms at search time, as fuzzy and prefix queries do. Such terms are not
     * known when the statistics pass runs, so they cannot have statistics.
     */
    static boolean expandsTerms(Query query) {
        boolean[] expands = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                expands[0] = true;
            }
        });
        return expands[0];
    }

    /**
     * Builds the BM25 scorer of a runtime {@code MATCH} or {@code MATCH_PHRASE}; see {@link RuntimeBm25ScoreEvaluator}.
     * <p>
     * It runs where the scorer is built, on the data node, and first checks that node's analysis: every term of
     * {@code compiled} must be in {@code shippedTerms}, the term list the statistics were computed for. Otherwise it
     * throws {@link Bm25AnalyzerMismatchException}. The list may hold more terms than the query, because several BM25
     * conditions on the same field share one statistics group.
     *
     * @param functionName   {@code MATCH} or {@code MATCH_PHRASE}, for the error message
     * @param fieldSource    the source text of the field, for the error and warning messages and PROFILE
     * @param compiled       the query compiled with {@code valuesAnalyzer}, which BM25 also uses to analyze the query
     * @param valuesAnalyzer the analyzer of the row values; {@code null} selects the standard analyzer
     * @param shippedTerms   the sorted term list of the statistics group
     * @param statistics     N, the summed length and one df per term of {@code shippedTerms}, in that order
     */
    public static ExpressionEvaluator.Factory bm25ScoreEvaluator(
        Source source,
        String functionName,
        ExpressionEvaluator.Factory fieldEvaluator,
        String fieldSource,
        Query compiled,
        @Nullable NamedAnalyzer valuesAnalyzer,
        List<BytesRef> shippedTerms,
        List<ExpressionEvaluator.Factory> statistics
    ) {
        if (statistics.size() != shippedTerms.size() + 2) {
            throw new IllegalArgumentException(
                "expected [" + (shippedTerms.size() + 2) + "] BM25 statistics but got [" + statistics.size() + "]"
            );
        }
        if (expandsTerms(compiled)) {
            // The Verifier rejects the options that expand terms; this guards the scorer if one gets through.
            throw new IllegalArgumentException(
                "["
                    + functionName
                    + "] cannot score ["
                    + fieldSource
                    + "] with BM25: fuzzy and other term-expanding queries are not supported with [similarity_function] = [bm25]"
            );
        }
        List<BytesRef> nodeTerms = collectTerms(compiled);
        if (new HashSet<>(shippedTerms).containsAll(nodeTerms) == false) {
            throw new Bm25AnalyzerMismatchException(functionName, fieldSource, nodeTerms, shippedTerms);
        }
        if (compiled instanceof MatchAllDocsQuery) {
            return ConstantEvaluators.constantDouble(1.0);
        }
        if (compiled instanceof MatchNoDocsQuery) {
            return ConstantEvaluators.constantDouble(0.0);
        }
        return new RuntimeBm25ScoreEvaluator.Factory(
            source,
            fieldSource,
            fieldEvaluator,
            compiled,
            withPositionIncrementGap(valuesAnalyzer == null ? Lucene.STANDARD_ANALYZER : valuesAnalyzer),
            List.copyOf(shippedTerms),
            List.copyOf(statistics)
        );
    }

    @Evaluator(extraName = "TextWithLuceneQuery", warnExceptions = { IOException.class }, allNullsIsNull = false)
    static boolean processText(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed Analyzer analyzer,
        @Fixed Query query,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) MemoryIndex memoryIndex,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) throws IOException {
        if (fieldBlock == null) {
            return false;
        }
        final var valueCount = fieldBlock.getValueCount(position);
        if (valueCount == 0) {
            return false;
        }

        indexRow(memoryIndex, fieldBlock, position, analyzer, scratch);
        IndexSearcher searcher = memoryIndex.createSearcher();

        TopDocs topDocs = searcher.search(query, 1);
        return topDocs.scoreDocs.length > 0;
    }

    /**
     * The scoring counterpart of {@code TextWithLuceneQuery} above: the same one-document-per-row {@link MemoryIndex}
     * evaluation, but returning the query's {@link #BOOLEAN_SIMILARITY} score instead of whether it matched.
     */
    @Evaluator(extraName = "ScoreLuceneQuery", allNullsIsNull = false)
    static double scoreLuceneQuery(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed Analyzer analyzer,
        @Fixed Query query,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) MemoryIndex memoryIndex,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        if (fieldBlock == null) {
            return 0.0;
        }
        final var valueCount = fieldBlock.getValueCount(position);
        if (valueCount == 0) {
            return 0.0;
        }

        indexRow(memoryIndex, fieldBlock, position, analyzer, scratch);
        IndexSearcher searcher = memoryIndex.createSearcher();
        searcher.setSimilarity(BOOLEAN_SIMILARITY);

        try {
            TopDocs topDocs = searcher.search(query, 1);
            return topDocs.scoreDocs.length > 0 ? topDocs.scoreDocs[0].score : 0.0;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Scores 1.0 when the wrapped boolean match evaluator matched, 0.0 otherwise. Used for the runtime paths whose
     * pushed-down counterpart scores a constant under {@link #BOOLEAN_SIMILARITY}: exact (non-text) matches and
     * phrase matches, whose single clause scores its boost — 1.0 whenever options don't apply.
     */
    @Evaluator(extraName = "ScoreFromBoolean", allNullsIsNull = false)
    static double scoreFromBoolean(@Position int position, BooleanBlock matches) {
        if (matches == null || matches.isNull(position)) {
            return 0.0;
        }
        return matches.getBoolean(matches.getFirstValueIndex(position)) ? 1.0 : 0.0;
    }
}
