/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.MatchBoolPrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.ZeroTermsQueryOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
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
                new NamedAnalyzer("stop", AnalyzerScope.INDEX, new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)),
                "synonyms",
                new NamedAnalyzer("synonyms", AnalyzerScope.INDEX, synonymAnalyzer()),
                "english",
                new NamedAnalyzer("english", AnalyzerScope.INDEX, new EnglishAnalyzer())
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

    /** A synonym of several words is a phrase, whichever side answers it. */
    public void testMultiWordSynonymIsAPhrase() throws IOException {
        final List<QueryBuilder> queries = List.of(new MatchQueryBuilder("body", "qb"), new MatchQueryBuilder("body", "qb fox"));
        final List<List<Integer>> indexed = matching(true, "synonyms", queries);
        final List<List<Integer>> notIndexed = matching(false, "synonyms", queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertEquals("not the document where the words are apart", List.of(0, 2), notIndexed.get(0));
    }

    /** The standard analyzer, with {@code qb} a synonym of {@code quick brown}. */
    private static Analyzer synonymAnalyzer() {
        final SynonymMap synonyms;
        try {
            SynonymMap.Builder builder = new SynonymMap.Builder(true);
            builder.add(new CharsRef("qb"), SynonymMap.Builder.join(new String[] { "quick", "brown" }, new CharsRefBuilder()), true);
            synonyms = builder.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                return new TokenStreamComponents(tokenizer, new SynonymGraphFilter(new LowerCaseFilter(tokenizer), synonyms, true));
            }
        };
    }

    /**
     * Only a strictly columnar index is taken to keep the values a text query reads. A field indexing no terms in any
     * other mode answers as it always has, from its doc values and whole, so nothing reads its values for a token.
     */
    public void testOutsideAStrictlyColumnarIndexNothingReadsTheValues() throws IOException {
        // The modes that keep the values a text query reads, and so the only ones this applies to.
        for (IndexMode mode : IndexMode.values()) {
            assertEquals(
                mode.getName(),
                mode == IndexMode.COLUMNAR || mode == IndexMode.LOGSDB_COLUMNAR || mode == IndexMode.VECTORDB_COLUMNAR,
                mode.isStrictColumnar()
            );
        }

        for (IndexMode mode : List.of(IndexMode.STANDARD)) {
            final MapperService mapperService = createMapperService(
                Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build(),
                mapping(b -> b.startObject("body").field("type", "text").field("index", false).field("doc_values", true).endObject())
            );
            withLuceneIndex(mapperService, iw -> {
                for (String doc : DOCS) {
                    iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
                }
            }, reader -> {
                final SearchExecutionContext context = createSearchExecutionContext(mapperService);
                final TextFamilyFieldType field = (TextFamilyFieldType) context.getFieldType("body");
                assertFalse(mode.getName(), field.answersTextQueryFromValues(context));

                // The whole value is what it compares against, so a token of a longer value finds nothing, while
                // the value itself, asked for whole, finds the document holding it.
                final IndexSearcher searcher = newSearcher(reader);
                assertEquals(mode.getName(), 0, searcher.count(new MatchQueryBuilder("body", "quick").toQuery(context)));
                assertEquals(mode.getName(), 0, searcher.count(new MatchQueryBuilder("body", "brown fox").toQuery(context)));
                assertEquals(mode.getName(), 1, searcher.count(new TermQueryBuilder("body", "quick brown").toQuery(context)));
            });
        }
    }

    /**
     * A terms query holds as many terms as its own limit allows, which is far more than the clauses a boolean query
     * takes, so the terms are asked for in one clause rather than one each.
     */
    public void testATermsQueryHoldsMoreTermsThanABooleanQueryTakes() throws IOException {
        final List<String> terms = new ArrayList<>();
        for (int i = 0; i < IndexSearcher.getMaxClauseCount() + 1; i++) {
            terms.add("term" + i);
        }
        terms.add("quick");
        final QueryBuilder query = new TermsQueryBuilder("body", terms);
        assertEquals(query.toString(), matching(true, List.of(query)), matching(false, List.of(query)));
        assertEquals("the documents holding the one term they share", List.of(0, 1, 2), matching(false, List.of(query)).get(0));
    }

    /** How many of a query's terms a document has to answer, which a field reading its values asks as an index does. */
    public void testMinimumShouldMatchIsAsked() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new MatchQueryBuilder("body", "quick brown nothing").minimumShouldMatch("3"),
            new MatchQueryBuilder("body", "quick brown nothing").minimumShouldMatch("2"),
            new MatchQueryBuilder("body", "quick brown nothing").minimumShouldMatch("1"),
            new MatchQueryBuilder("body", "quick brown nothing").minimumShouldMatch("67%"),
            new MultiMatchQueryBuilder("quick brown nothing", "body").minimumShouldMatch("3"),
            new MatchBoolPrefixQueryBuilder("body", "quick brown nothi").minimumShouldMatch("3"),
            new MatchBoolPrefixQueryBuilder("body", "quick brown nothi").minimumShouldMatch("2")
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertEquals("no document holds all three", List.of(), notIndexed.get(0));
    }

    /**
     * {@code cross_fields} blends the term statistics several fields keep in an index. A field answered from its
     * values keeps none, so a blend reaching one is refused rather than leaving its matches out.
     */
    public void testCrossFieldsOverSeveralFieldsIsRefused() throws IOException {
        final MapperService mapper = createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(b -> {
                b.startObject("body").field("type", "text").field("index", false).endObject();
                b.startObject("other").field("type", "text").endObject();
            })
        );
        final SearchExecutionContext context = createSearchExecutionContext(mapper);
        final QueryBuilder blended = new MultiMatchQueryBuilder("quick", "body", "other").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> blended.toQuery(context));
        assertThat(e.getMessage(), containsString("Cannot run [cross_fields] over field [body]"));

        // One field of its own still answers, since nothing is blended there.
        assertNotNull(new MultiMatchQueryBuilder("quick", "body").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).toQuery(context));
    }

    /** Every multi match type answers from the values, including the one that blends a term across fields. */
    public void testEveryMultiMatchTypeAnswers() throws IOException {
        final List<QueryBuilder> queries = new ArrayList<>();
        for (MultiMatchQueryBuilder.Type type : MultiMatchQueryBuilder.Type.values()) {
            queries.add(new MultiMatchQueryBuilder("quick", "body").type(type));
            queries.add(new MultiMatchQueryBuilder("quick brown", "body").type(type));
        }
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    /**
     * A query the analyzer leaves no term of, which {@code zero_terms_query} answers for. The query standing in for
     * the clauses names no field, so nothing reads a document's values for it.
     */
    public void testAQueryOfNoTermsAnswersAsItIsAskedTo() throws IOException {
        final List<QueryBuilder> queries = new ArrayList<>();
        for (MultiMatchQueryBuilder.Type type : MultiMatchQueryBuilder.Type.values()) {
            queries.add(new MultiMatchQueryBuilder("the", "body").type(type).analyzer("stop").zeroTermsQuery(ZeroTermsQueryOption.ALL));
            queries.add(new MultiMatchQueryBuilder("the", "body").type(type).analyzer("stop").zeroTermsQuery(ZeroTermsQueryOption.NONE));
        }
        queries.add(new MatchQueryBuilder("body", "the").analyzer("stop").zeroTermsQuery(ZeroTermsQueryOption.ALL));
        queries.add(new MatchQueryBuilder("body", "the").analyzer("stop").zeroTermsQuery(ZeroTermsQueryOption.NONE));

        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertEquals("all of them, where it is asked to", List.of(0, 1, 2, 3), notIndexed.get(0));
        assertEquals("none of them otherwise", List.of(), notIndexed.get(1));
    }

    /**
     * A document of several values answers as an index of the same values answers. Its values are read as one
     * document, so a term spelled by any of them keeps it, while a phrase still sits inside a single value.
     */
    public void testSeveralValuesAnswerAsIndexed() throws IOException {
        final List<List<String>> docs = List.of(
            List.of("the quick brown", "fox jumps"),
            List.of("quick", "brown"),
            List.of("nothing", "here at all")
        );
        final List<QueryBuilder> queries = List.of(
            QueryBuilders.matchQuery("body", "quick fox"),
            QueryBuilders.matchQuery("body", "quick fox").operator(Operator.AND),
            QueryBuilders.matchPhraseQuery("body", "quick brown"),
            QueryBuilders.matchPhraseQuery("body", "brown fox"),
            QueryBuilders.termQuery("body", "jumps"),
            QueryBuilders.queryStringQuery("body:quick AND body:jumps")
        );
        final List<List<Integer>> indexed = matchingValues(true, null, docs, queries);
        final List<List<Integer>> notIndexed = matchingValues(false, null, docs, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertEquals("a term of either value answers for the document", List.of(0, 1), indexed.get(0));
        assertEquals("both terms, spread over two values, answer for it too", List.of(0), indexed.get(1));
        // This analyzer leaves no gap between one value and the next, so a phrase runs from one into the other,
        // and the values answer it where an index of them does.
        assertEquals(List.of(0), indexed.get(3));
    }

    /** A conjunction answers from the values as an index of them answers, however its terms are spread. */
    public void testConjunctionsAnswerAsIndexed() throws IOException {
        final List<QueryBuilder> queries = List.of(
            QueryBuilders.matchQuery("body", "quick brown").operator(Operator.AND),
            QueryBuilders.matchQuery("body", "quick nothing").operator(Operator.AND),
            QueryBuilders.queryStringQuery("body:quick AND body:brown"),
            QueryBuilders.queryStringQuery("body:quick AND body:absent"),
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("body", "quick")).must(QueryBuilders.matchQuery("body", "fox"))
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertFalse("and some of them answer, so the comparison means something", indexed.get(0).isEmpty());
    }

    /**
     * Random values and random queries over them, answered from the values exactly as an index of them answers. This
     * is what holds the read of a value's bytes to the question: a document it ruled out that an index returns shows
     * up here.
     */
    public void testRandomQueriesAnswerAsIndexed() throws IOException {
        final List<String> docs = new ArrayList<>();
        for (int i = between(20, 50); i > 0; i--) {
            docs.add(randomText());
        }
        final List<QueryBuilder> queries = new ArrayList<>();
        for (int i = between(20, 40); i > 0; i--) {
            queries.add(randomTextQuery());
        }
        final List<List<Integer>> indexed = matching(true, null, docs, queries);
        final List<List<Integer>> notIndexed = matching(false, null, docs, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString() + " over " + docs, indexed.get(q), notIndexed.get(q));
        }
    }

    /**
     * Which queries the values are read for before being analyzed. A query naming its terms outright is read for;
     * one naming them by a range of them is not, and nor is a disjunction holding such a clause, which a document
     * answers without holding any term named anywhere in it.
     */
    public void testWhichQueriesTheValuesAreReadFor() throws IOException {
        final MapperService mapperService = mapper(false);
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        assertRead("a term", QueryBuilders.termQuery("body", "quick"), context, true);
        assertRead("one word of a match", QueryBuilders.matchQuery("body", "quick"), context, true);
        assertRead("several words of a match", QueryBuilders.matchQuery("body", "quick brown"), context, true);
        assertRead("a phrase", QueryBuilders.matchPhraseQuery("body", "quick brown"), context, true);
        assertRead("a field and a term", QueryBuilders.queryStringQuery("body:quick"), context, true);
        assertRead("two of them", QueryBuilders.queryStringQuery("body:quick body:brown"), context, true);
        assertRead("both of them", QueryBuilders.queryStringQuery("body:quick AND body:brown"), context, true);
        assertRead("a simple query string", QueryBuilders.simpleQueryStringQuery("quick").field("body"), context, true);
        assertRead("terms", QueryBuilders.termsQuery("body", "quick", "brown"), context, true);
        assertRead("a match of every word", QueryBuilders.matchQuery("body", "quick brown").operator(Operator.AND), context, true);
        assertRead("a prefix", QueryBuilders.prefixQuery("body", "qui"), context, false);
        assertRead("a match holding a prefix", QueryBuilders.matchBoolPrefixQuery("body", "quick bro"), context, false);
        assertRead("a fuzziness", QueryBuilders.matchQuery("body", "quick").fuzziness(Fuzziness.ONE), context, false);
        assertRead("a wildcard", QueryBuilders.wildcardQuery("body", "qui*"), context, false);
    }

    /**
     * Asserts what every clause answered from the values makes of their bytes. A compound query wraps each of its
     * clauses, so a query string naming two of them is two reads rather than one.
     */
    /**
     * Which queries a document's tokens are walked for rather than indexed one document at a time. A query of terms
     * is walked however its clauses are put together, and one naming a range of terms is indexed as it was.
     */
    public void testWhichQueriesTheTokensAreWalkedFor() throws IOException {
        final MapperService mapperService = mapper(false);
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        assertWalked("a term", QueryBuilders.termQuery("body", "quick"), context, true);
        assertWalked("one word of a match", QueryBuilders.matchQuery("body", "quick"), context, true);
        assertWalked("several words of a match", QueryBuilders.matchQuery("body", "quick brown"), context, true);
        assertWalked("a match of every word", QueryBuilders.matchQuery("body", "quick brown").operator(Operator.AND), context, true);
        assertWalked(
            "a minimum of them to match",
            QueryBuilders.matchQuery("body", "quick brown fox").minimumShouldMatch("2"),
            context,
            true
        );
        assertWalked("a phrase", QueryBuilders.matchPhraseQuery("body", "quick brown"), context, true);
        assertWalked("terms", QueryBuilders.termsQuery("body", "quick", "brown"), context, true);
        assertWalked("a phrase with slop", QueryBuilders.matchPhraseQuery("body", "quick fox").slop(1), context, false);
        assertWalked("a fuzziness", QueryBuilders.matchQuery("body", "quick").fuzziness(Fuzziness.ONE), context, false);
        assertWalked("a match holding a prefix", QueryBuilders.matchBoolPrefixQuery("body", "quick bro"), context, false);
    }

    private void assertWalked(String what, QueryBuilder builder, SearchExecutionContext context, boolean walked) throws IOException {
        final List<ReanalyzingTextQuery> answeredFromValues = new ArrayList<>();
        collectValueQueries(builder.toQuery(context), answeredFromValues);
        assertFalse(what + " is answered from the values", answeredFromValues.isEmpty());
        for (ReanalyzingTextQuery answered : answeredFromValues) {
            final Query in = answered.getQuery();
            final boolean byWalk = ReanalyzingTextQuery.walkablePhrase(in) != null
                || ReanalyzingTextQuery.walkableClauses(in, "body") != null;
            assertEquals(what + (walked ? " is walked" : " is indexed a document at a time") + ": " + in, walked, byWalk);
        }
    }

    private void assertRead(String what, QueryBuilder builder, SearchExecutionContext context, boolean read) throws IOException {
        final List<ReanalyzingTextQuery> answeredFromValues = new ArrayList<>();
        collectValueQueries(builder.toQuery(context), answeredFromValues);
        assertFalse(what + " is answered from the values", answeredFromValues.isEmpty());
        for (ReanalyzingTextQuery answered : answeredFromValues) {
            assertEquals(
                what + (read ? " names terms its values are read for" : " names no term that must be held") + ": " + answered,
                read,
                CandidateValues.of(answered.getQuery(), "body").reads()
            );
        }
    }

    private static void collectValueQueries(Query query, List<ReanalyzingTextQuery> found) {
        switch (query) {
            case ReanalyzingTextQuery answered -> found.add(answered);
            case BooleanQuery bool -> bool.forEach(clause -> collectValueQueries(clause.query(), found));
            case BoostQuery boost -> collectValueQueries(boost.getQuery(), found);
            case ConstantScoreQuery constant -> collectValueQueries(constant.getQuery(), found);
            default -> {
            }
        }
    }

    /**
     * The yes-answer runs on the analyzer the query carries, which has to be the field's own: the index-wide
     * analyzer a query is built with wraps every field's and is none of them, so nothing can be read for its
     * tokens.
     */
    public void testTheQueryCarriesTheAnalyzerItsValuesAreReadWith() throws IOException {
        final SearchExecutionContext context = createSearchExecutionContext(mapper(false));
        final List<ReanalyzingTextQuery> answeredFromValues = new ArrayList<>();
        collectValueQueries(QueryBuilders.matchQuery("body", "quick").toQuery(context), answeredFromValues);
        assertEquals(1, answeredFromValues.size());
        final ReanalyzingTextQuery answered = answeredFromValues.get(0);
        final CandidateValues candidates = CandidateValues.of(answered.getQuery(), "body").readingTokensOf(answered.valuesAnalyzer());
        assertEquals(
            "a value holding the term as a token is answered without being analyzed",
            CandidateValues.Answer.YES,
            candidates.held(List.<Object>of(new BytesRef("the quick brown fox")))
        );
    }

    /** The analyzer decides whether the values are read at all, whatever the query asks. */
    public void testAStemmedFieldIsNotReadFor() throws IOException {
        assertTrue(CandidateValues.valuesSpellTheirTokens(createSearchExecutionContext(mapper(false)).getIndexAnalyzer("body")));
        assertFalse(
            CandidateValues.valuesSpellTheirTokens(createSearchExecutionContext(mapper(false, "english")).getIndexAnalyzer("body"))
        );
    }

    /**
     * A stemmer leaves tokens its values do not spell, so a value's bytes say nothing about what it holds. The
     * values answer the same queries as an index of them all the same, by analyzing every document as before.
     */
    public void testAStemmedFieldAnswersAsIndexed() throws IOException {
        final List<String> docs = List.of("happy families", "happier still", "the happiest of all", "nothing here");
        final List<QueryBuilder> queries = List.of(
            QueryBuilders.matchQuery("body", "happy"),
            QueryBuilders.matchQuery("body", "happier"),
            QueryBuilders.matchQuery("body", "happiest"),
            QueryBuilders.matchQuery("body", "families"),
            QueryBuilders.matchPhraseQuery("body", "happy families")
        );
        final List<List<Integer>> indexed = matching(true, "english", docs, queries);
        final List<List<Integer>> notIndexed = matching(false, "english", docs, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
        assertFalse("and the stemmed queries do answer, so the comparison means something", indexed.get(0).isEmpty());
    }

    /** Words that put the read's edges in the data: the ASCII letters in both cases, and bytes outside ASCII. */
    private static final String[] WORDS = {
        "quick",
        "brown",
        "fox",
        "jumps",
        "the",
        "QUICK",
        "Brown",
        "FOX",
        "caf\u00e9",
        "Caf\u00e9",
        "na\u00efve",
        "\u0105ngstr\u00f6m",
        "\u0104NGSTR\u00d6M",
        "\u03b1\u03bb\u03c6\u03b1",
        "\u0391\u039b\u03a6\u0391",
        "\u043c\u043e\u0441\u043a\u0432\u0430",
        "\u041c\u041e\u0421\u041a\u0412\u0410",
        "zeppel\u0130n",
        "\u212Aelvin",
        "x",
        "xx" };

    private String randomText() {
        final StringBuilder text = new StringBuilder();
        for (int w = between(0, 8); w > 0; w--) {
            text.append(text.isEmpty() ? "" : randomFrom(" ", "  ", ", ", ". ", " - ")).append(randomFrom(WORDS));
        }
        return text.toString();
    }

    private QueryBuilder randomTextQuery() {
        final String one = randomFrom(WORDS);
        final String other = randomFrom(WORDS);
        return switch (between(0, 6)) {
            case 0 -> QueryBuilders.matchQuery("body", one);
            case 1 -> QueryBuilders.matchQuery("body", one + " " + other);
            case 2 -> QueryBuilders.matchPhraseQuery("body", one + " " + other);
            case 3 -> QueryBuilders.termQuery("body", one.toLowerCase(Locale.ROOT));
            case 4 -> QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("body", one))
                .mustNot(QueryBuilders.matchQuery("body", other));
            case 5 -> QueryBuilders.matchQuery("body", one + " " + other).minimumShouldMatch("2");
            default -> QueryBuilders.matchBoolPrefixQuery("body", one + " " + other);
        };
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
        // One term asks nothing of positions, but a field holding no terms has nothing to answer it with either,
        // so its values are read for it as they are for a phrase of several.
        queries.add(new MatchPhrasePrefixQueryBuilder("body", "qui"));
        queries.add(new MatchPhrasePrefixQueryBuilder("body", "quick bro"));
        queries.add(new MatchPhrasePrefixQueryBuilder("body", "nothing"));
        // A boolean prefix holds a prefix clause the field wraps itself, beside the terms before it.
        queries.add(new MatchBoolPrefixQueryBuilder("body", "bro"));
        queries.add(new MatchBoolPrefixQueryBuilder("body", "quick bro"));
        queries.add(new MatchBoolPrefixQueryBuilder("body", "the quick bro"));

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
            new MultiMatchQueryBuilder("quick bro", "body").type(MultiMatchQueryBuilder.Type.BOOL_PREFIX),
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
        // A clause naming a range of terms leaves the whole query unbounded, however many of its clauses name
        // terms themselves: that clause can answer too, and the bound has to hold for the clauses together.
        assertEquals(Float.MAX_VALUE, maxScoreOf(new MatchBoolPrefixQueryBuilder("body", "quick bro")), 0.0001f);
        for (QueryBuilder query : List.of(
            new MatchQueryBuilder("body", "quick"),
            new MatchQueryBuilder("body", "quick brown"),
            new MatchPhraseQueryBuilder("body", "quick brown"),
            new MatchBoolPrefixQueryBuilder("body", "bro"),
            new MatchBoolPrefixQueryBuilder("body", "quick bro"),
            new MatchBoolPrefixQueryBuilder("body", "the quick bro"),
            new MatchQueryBuilder("body", "quikc").fuzziness(Fuzziness.ONE),
            new MatchPhrasePrefixQueryBuilder("body", "quick bro")
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
        return matching(indexed, analyzer, DOCS, queries);
    }

    private List<List<Integer>> matching(boolean indexed, String analyzer, List<String> docs, List<QueryBuilder> queries)
        throws IOException {
        final List<List<String>> ofOneValue = new ArrayList<>(docs.size());
        for (String doc : docs) {
            ofOneValue.add(List.of(doc));
        }
        return matchingValues(indexed, analyzer, ofOneValue, queries);
    }

    private List<List<Integer>> matchingValues(boolean indexed, String analyzer, List<List<String>> docs, List<QueryBuilder> queries)
        throws IOException {
        final MapperService mapperService = mapper(indexed, analyzer);
        final List<List<Integer>> perQuery = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (List<String> doc : docs) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.array("body", doc.toArray(String[]::new)))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            for (QueryBuilder query : queries) {
                final List<Integer> hits = new ArrayList<>();
                // Every matching document, not a page of them: the two sides weigh a hit differently by design, so
                // what is compared is which documents answer, not the order they come back in.
                for (ScoreDoc hit : searcher.search(query.toQuery(context), Math.max(10, docs.size())).scoreDocs) {
                    hits.add(hit.doc);
                }
                hits.sort(null);
                perQuery.add(hits);
            }
        });
        return perQuery;
    }
}
