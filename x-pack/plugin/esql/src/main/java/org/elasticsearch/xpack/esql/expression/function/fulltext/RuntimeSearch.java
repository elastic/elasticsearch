/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;

/**
 * Runtime (per-row) evaluation of full-text functions on {@code text} expressions that are not index-mapped fields,
 * where there is no Lucene index to query. Instead, each value is analyzed on the fly and the resulting token stream
 * is matched directly against the analyzed query terms.
 * <p>
 * The block-level walking (multivalue any-value semantics, null handling, per-thread scratch) is shared here through
 * a single {@code Text} evaluator; what differs between functions is only how a single value's token stream is
 * matched, expressed as a {@link TokenStreamMatcher} ({@link AnyTermMatcher} for {@code match},
 * {@link PhraseMatcher} for {@code match_phrase}).
 */
public final class RuntimeSearch {

    public static final String CONTENT_FIELD = "content_field";

    private static final Similarity BOOLEAN_SIMILARITY = new BooleanSimilarity();

    private RuntimeSearch() {}

    /**
     * Decides whether a single value's token stream matches the query. The stream is already reset; implementations
     * consume it (typically through {@link TermToBytesRefAttribute}) and must not close it.
     */
    public interface TokenStreamMatcher {
        boolean matches(TokenStream stream) throws IOException;
    }

    /**
     * Matches when any analyzed token equals any of the query terms — the OR semantics of runtime {@code match}.
     */
    public record AnyTermMatcher(Set<BytesRef> queryTerms) implements TokenStreamMatcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            // TODO: Use the operator specified in the query options. For now, we use OR, meaning we stop as soon as
            // we find a match.
            while (stream.incrementToken()) {
                if (queryTerms.contains(term.getBytesRef())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Matches when all query terms appear in order at consecutive token positions (slop 0) — the semantics of
     * runtime {@code match_phrase}.
     */
    public record PhraseMatcher(List<BytesRef> queryTerms) implements TokenStreamMatcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute positionIncrement = stream.addAttribute(PositionIncrementAttribute.class);
            // matched[k] being true means the first k + 1 query terms matched a run of tokens at consecutive
            // positions, ending at the previous token (prev) or at the current one (curr).
            boolean[] prev = new boolean[queryTerms.size()];
            boolean[] curr = new boolean[queryTerms.size()];
            while (stream.incrementToken()) {
                if (positionIncrement.getPositionIncrement() != 1) {
                    // A position gap (or a stacked token) breaks adjacency: with slop 0 a phrase only matches
                    // tokens at consecutive positions.
                    Arrays.fill(prev, false);
                }
                BytesRef token = term.getBytesRef();
                for (int k = queryTerms.size() - 1; k > 0; k--) {
                    curr[k] = prev[k - 1] && ByteMatchers.equals(token, queryTerms.get(k));
                }
                curr[0] = ByteMatchers.equals(token, queryTerms.get(0));
                if (curr[queryTerms.size() - 1]) {
                    return true;
                }
                boolean[] swap = prev;
                prev = curr;
                curr = swap;
            }
            return false;
        }
    }

    /**
     * Analyzes the given query string into the ordered list of its terms, discarding position increments. That is
     * fine for {@code match} (its {@link AnyTermMatcher} is position-insensitive with any analyzer), but a phrase
     * needs the gaps a stopword-removing analyzer leaves behind: {@code match_phrase} only takes the
     * {@link PhraseMatcher} fast path with the standard analyzer, whose default configuration emits tokens at
     * consecutive positions.
     */
    static List<BytesRef> analyzeTerms(Analyzer analyzer, String query) throws IOException {
        List<BytesRef> terms = new ArrayList<>();

        try (TokenStream stream = analyzer.tokenStream(CONTENT_FIELD, query)) {
            stream.reset();
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                terms.add(BytesRef.deepCopyOf(term.getBytesRef()));
            }
            stream.end();
        }
        return terms;
    }

    /**
     * Like {@link #analyzeTerms}, but returns each distinct term with the number of times it occurs in the query.
     * Used for scoring, where a pushed-down match query gets one clause per occurrence: a term repeated N times
     * weighs N.
     */
    static Map<BytesRef, Integer> analyzeTermsWithCounts(Analyzer analyzer, String query) throws IOException {
        Map<BytesRef, Integer> terms = new HashMap<>();

        try (TokenStream stream = analyzer.tokenStream(CONTENT_FIELD, query)) {
            stream.reset();
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                terms.merge(BytesRef.deepCopyOf(term.getBytesRef()), 1, Integer::sum);
            }
            stream.end();
        }
        return terms;
    }

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
        @Fixed TokenStreamMatcher matcher,
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
    private static Query compileQuery(org.elasticsearch.xpack.esql.core.querydsl.query.Query query, NamedAnalyzer namedAnalyzer) {
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
        Analyzer analyzer = toEvaluator.getAnalyzer(name);
        // Registry lookups return NamedAnalyzer in practice, but the interface only promises Analyzer
        return analyzer instanceof NamedAnalyzer namedAnalyzer ? namedAnalyzer : new NamedAnalyzer(name, AnalyzerScope.GLOBAL, analyzer);
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
        final var startIndex = fieldBlock.getFirstValueIndex(position);
        if (valueCount == 0) {
            return false;
        }

        // All values of the position form one document, like an indexed multivalued text field: query terms may
        // match across values, while the analyzer's position increment gap keeps phrases within a single value.
        memoryIndex.reset();
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            scratch = fieldBlock.getBytesRef(valueIndex, scratch);
            memoryIndex.addField(CONTENT_FIELD, scratch.utf8ToString(), analyzer);
        }
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
        final var startIndex = fieldBlock.getFirstValueIndex(position);
        if (valueCount == 0) {
            return 0.0;
        }

        memoryIndex.reset();
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            scratch = fieldBlock.getBytesRef(valueIndex, scratch);
            memoryIndex.addField(CONTENT_FIELD, scratch.utf8ToString(), analyzer);
        }
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
