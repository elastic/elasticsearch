/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A {@code text} field of a strictly columnar index answers a text query from its values where it indexes no terms,
 * which every query here asks of the same mapping with an index too.
 */
public class TextNoTermsSearchTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of(
        "the quick brown fox jumps",
        "brown the quick fox",
        "quick brown",
        "nothing here at all"
    );

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "stop",
                new NamedAnalyzer("stop", AnalyzerScope.INDEX, new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET))
            )
        );
    }

    /** A dropped stopword leaves a gap the phrase has to keep, whichever side answers it. */
    public void testPhraseUnderAStopAnalyzer() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new MatchPhraseQueryBuilder("body", "the quick"),
            new MatchPhraseQueryBuilder("body", "quick brown"),
            new MatchQueryBuilder("body", "the")
        );
        final List<List<Integer>> indexed = matching(true, "stop", queries);
        final List<List<Integer>> notIndexed = matching(false, "stop", queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    private MapperService mapper(boolean indexed) throws IOException {
        return mapper(indexed, null);
    }

    private MapperService mapper(boolean indexed, String analyzer) throws IOException {
        return createMapperService(Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(), mapping(b -> {
            b.startObject("body").field("type", "text").field("index", indexed);
            if (analyzer != null) {
                b.field("analyzer", analyzer);
            }
            b.endObject();
        }));
    }

    public void testQueriesAnswerAsIndexed() throws IOException {
        final List<QueryBuilder> queries = new ArrayList<>();
        queries.add(new MatchQueryBuilder("body", "quick"));
        queries.add(new MatchQueryBuilder("body", "quick brown"));
        queries.add(new MatchQueryBuilder("body", "quick nothing"));
        queries.add(new MatchQueryBuilder("body", "quick brown").operator(Operator.AND));
        queries.add(new MatchQueryBuilder("body", "quick missing").operator(Operator.AND));
        queries.add(new MatchQueryBuilder("body", "quikc").fuzziness(Fuzziness.ONE));
        queries.add(new MatchPhraseQueryBuilder("body", "quick brown"));
        queries.add(new MatchPhraseQueryBuilder("body", "brown quick"));
        queries.add(new MatchPhraseQueryBuilder("body", "quick fox").slop(1));

        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    /** The term family looks for tokens, as it does where the terms are indexed. */
    public void testTermFamilyAnswersAsIndexed() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new TermQueryBuilder("body", "quick"),
            new TermQueryBuilder("body", "the quick brown fox jumps"),
            new TermsQueryBuilder("body", "quick", "nothing"),
            new PrefixQueryBuilder("body", "qui"),
            new WildcardQueryBuilder("body", "qu*ck"),
            new RegexpQueryBuilder("body", "qu.*k")
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertEquals("a term of the values, not the whole value", 1, confirmedClauses(new TermQueryBuilder("body", "quick")));
        assertEquals("one clause for every term", 1, confirmedClauses(new TermsQueryBuilder("body", "quick", "nothing")));
    }

    /** Every parser that builds a match query answers from the values, reading them once for all of its terms. */
    /** A wildcard inside query_string is normalized by the search analyzer, as it is on an indexed field. */
    public void testNormalizedWildcardFromQueryString() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new QueryStringQueryBuilder("body:qu*ck"),
            new QueryStringQueryBuilder("body:QU*CK"),
            new SimpleQueryStringBuilder("qu*ck").field("body"),
            new WildcardQueryBuilder("body", "QU*CK")
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    public void testQueryStringAndMultiMatch() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new QueryStringQueryBuilder("body:quick"),
            new QueryStringQueryBuilder("body:(quick brown)"),
            new QueryStringQueryBuilder("body:\"quick brown\""),
            new QueryStringQueryBuilder("quick brown").defaultField("body"),
            new MultiMatchQueryBuilder("quick brown", "body"),
            new MultiMatchQueryBuilder("quick brown", "body").type(MultiMatchQueryBuilder.Type.PHRASE),
            new SimpleQueryStringBuilder("quick brown").field("body")
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    /**
     * Every form scores through a two-phase iterator: nothing says in advance which document matches, and the
     * approximation has to stay within the range the search asks it about.
     */
    public void testEveryFormIsTwoPhase() throws IOException {
        for (QueryBuilder builder : List.of(
            new MatchQueryBuilder("body", "quick"),
            new MatchQueryBuilder("body", "quick brown"),
            new MatchPhraseQueryBuilder("body", "quick brown"),
            new MatchQueryBuilder("body", "quikc").fuzziness(Fuzziness.ONE)
        )) {
            final MapperService mapperService = mapper(false);
            withLuceneIndex(mapperService, iw -> {
                for (String doc : DOCS) {
                    iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
                }
            }, reader -> {
                final SearchExecutionContext context = createSearchExecutionContext(mapperService);
                final IndexSearcher searcher = newSearcher(reader);
                final Query query = searcher.rewrite(builder.toQuery(context));
                final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
                for (LeafReaderContext leaf : reader.leaves()) {
                    final ScorerSupplier supplier = weight.scorerSupplier(leaf);
                    if (supplier == null) {
                        continue;
                    }
                    final Scorer scorer = supplier.get(Long.MAX_VALUE);
                    assertNotNull(builder.toString() + " is two phase", scorer.twoPhaseIterator());
                }
            });
        }
    }

    /** How many reads of a document's values one query costs: one clause, one read. */
    public void testTermsOfOneFieldQueryShareOneClause() throws IOException {
        assertEquals("one clause for every term of a match", 1, confirmedClauses(new MatchQueryBuilder("body", "quick brown fox")));
        assertEquals("and for a phrase", 1, confirmedClauses(new MatchPhraseQueryBuilder("body", "quick brown")));
        assertEquals("and for a query_string field group", 1, confirmedClauses(new QueryStringQueryBuilder("body:(quick brown)")));
        assertEquals("and for multi_match over one field", 1, confirmedClauses(new MultiMatchQueryBuilder("quick brown", "body")));
    }

    private int confirmedClauses(QueryBuilder builder) throws IOException {
        final SearchExecutionContext context = createSearchExecutionContext(mapper(false));
        final int[] count = new int[1];
        builder.toQuery(context).visit(new QueryVisitor() {
            @Override
            public QueryVisitor getSubVisitor(Occur occur, Query parent) {
                if (parent instanceof ReanalyzingTextQuery) {
                    count[0]++;
                }
                return this;
            }
        });
        return count[0];
    }

    /** One point per matched query term, where an index scores BM25 from statistics a column does not hold. */
    public void testScoreCountsMatchedTerms() throws IOException {
        assertEquals(1.0f, scoreOf(new MatchQueryBuilder("body", "quick")), 0.0001f);
        assertEquals(2.0f, scoreOf(new MatchQueryBuilder("body", "quick brown")), 0.0001f);
        assertEquals(1.0f, scoreOf(new MatchQueryBuilder("body", "quick nothing")), 0.0001f);
        assertEquals(1.0f, scoreOf(new MatchPhraseQueryBuilder("body", "quick brown")), 0.0001f);
    }

    private float scoreOf(QueryBuilder query) throws IOException {
        final float[] score = new float[1];
        final MapperService mapperService = mapper(false);
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final ScoreDoc[] hits = newSearcher(reader).search(query.toQuery(context), 1).scoreDocs;
            score[0] = hits.length == 0 ? 0 : hits[0].score;
        });
        return score[0];
    }

    /**
     * The clauses a document can answer bound its score, which is what lets a collector stop reading documents that
     * cannot beat the ones it holds. A phrase is one clause, and a query naming a range of terms rather than the terms
     * themselves is left unbounded.
     */
    public void testTheClausesBoundTheScore() throws IOException {
        assertEquals(1.0f, maxScoreOf(new MatchQueryBuilder("body", "quick")), 0.0001f);
        assertEquals(2.0f, maxScoreOf(new MatchQueryBuilder("body", "quick brown")), 0.0001f);
        assertEquals(1.0f, maxScoreOf(new MatchPhraseQueryBuilder("body", "quick brown")), 0.0001f);
        assertEquals(Float.MAX_VALUE, maxScoreOf(new MatchQueryBuilder("body", "quikc").fuzziness(Fuzziness.ONE)), 0.0001f);
        for (QueryBuilder query : List.of(
            new MatchQueryBuilder("body", "quick"),
            new MatchQueryBuilder("body", "quick brown"),
            new MatchPhraseQueryBuilder("body", "quick brown")
        )) {
            assertThat(query.toString(), scoreOf(query), lessThanOrEqualTo(maxScoreOf(query)));
        }
    }

    private float maxScoreOf(QueryBuilder builder) throws IOException {
        final float[] max = new float[1];
        final MapperService mapperService = mapper(false);
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            final Weight weight = searcher.createWeight(searcher.rewrite(builder.toQuery(context)), ScoreMode.COMPLETE, 1f);
            for (LeafReaderContext leaf : reader.leaves()) {
                final ScorerSupplier supplier = weight.scorerSupplier(leaf);
                if (supplier != null) {
                    final Scorer scorer = supplier.get(Long.MAX_VALUE);
                    scorer.advanceShallow(0);
                    max[0] = Math.max(max[0], scorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS));
                }
            }
        });
        return max[0];
    }

    private List<List<Integer>> matching(boolean indexed, List<QueryBuilder> queries) throws IOException {
        return matching(indexed, null, queries);
    }

    private List<List<Integer>> matching(boolean indexed, String analyzer, List<QueryBuilder> queries) throws IOException {
        final MapperService mapperService = mapper(indexed, analyzer);
        final List<List<Integer>> perQuery = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            for (QueryBuilder query : queries) {
                final List<Integer> hits = new ArrayList<>();
                for (ScoreDoc hit : searcher.search(query.toQuery(context), 10).scoreDocs) {
                    hits.add(hit.doc);
                }
                hits.sort(null);
                perQuery.add(hits);
            }
        });
        return perQuery;
    }
}
