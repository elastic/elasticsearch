/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** Tests query building against {@link RuntimeSearchExecutionContext}. */
public class HighlightQueryBuildersTests extends ESTestCase {

    private static final List<String> TITLE = List.of("title");
    private static final List<String> TITLE_BODY = List.of("title", "body");

    private static final NamedAnalyzer STOP_ANALYZER = new NamedAnalyzer(
        "_stop",
        AnalyzerScope.GLOBAL,
        new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)
    );

    private static Query translate(Expression query, List<String> fields) {
        return translate(query, fields, Lucene.STANDARD_ANALYZER);
    }

    private static Query translate(Expression query, List<String> fields, NamedAnalyzer analyzer) {
        QueryBuilder builder = HighlightQueryBuilders.toQueryBuilder(query, fields);
        return HighlightQueryBuilders.toLuceneQuery(builder, RuntimeSearchExecutionContext.create(fields, analyzer));
    }

    private static Query translateLiteral(String text) {
        return translate(of(text), TITLE);
    }

    private static Match match(String field, String text, MapExpression options) {
        return new Match(EMPTY, getFieldAttribute(field, KEYWORD), of(text), options);
    }

    private static MatchPhrase matchPhrase(String field, String text, MapExpression options) {
        return new MatchPhrase(EMPTY, getFieldAttribute(field, KEYWORD), of(text), options);
    }

    private static QueryString queryString(String text, MapExpression options) {
        return new QueryString(EMPTY, of(text), options, TEST_CFG);
    }

    private static MapExpression options(Object... keyValues) {
        List<Expression> entries = new ArrayList<>(keyValues.length);
        for (Object keyValue : keyValues) {
            entries.add(of(keyValue));
        }
        return new MapExpression(EMPTY, entries);
    }

    public void testLiteralDisjunctionOfTerms() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translateLiteral("foo bar"));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
            assertThat(clause.query(), instanceOf(TermQuery.class));
        }
    }

    public void testLiteralBlankInputIsMatchNoDocs() {
        assertThat(translateLiteral(""), instanceOf(MatchNoDocsQuery.class));
        assertThat(translateLiteral("   "), instanceOf(MatchNoDocsQuery.class));
    }

    public void testLiteralAllTermsFilteredIsEmptyBoolean() {
        // query_string represents a query containing only stop words as an empty boolean query.
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(of("the"), TITLE, STOP_ANALYZER));
        assertThat(bq.clauses(), hasSize(0));
    }

    public void testVerifyUsesProvidedAnalyzer() {
        // A failing analyzer makes it observable which analyzer verify uses without mocking the query machinery.
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                throw new IllegalStateException("test analyzer was used");
            }
        };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightQueryBuilders.verify(of("fox"), TITLE, analyzer)
        );
        assertThat(e.getMessage(), containsString("test analyzer was used"));
    }

    public void testLiteralLeadingWildcardAllowed() {
        assertThat(translateLiteral("*ox"), instanceOf(WildcardQuery.class));
    }

    public void testLiteralFuzzyDistances() {
        assertThat(asInstanceOf(FuzzyQuery.class, translateLiteral("quick~")).getMaxEdits(), equalTo(1));
        assertThat(asInstanceOf(FuzzyQuery.class, translateLiteral("fx~")).getMaxEdits(), equalTo(0));
        assertThat(asInstanceOf(FuzzyQuery.class, translateLiteral("fox~2")).getMaxEdits(), equalTo(2));
    }

    public void testLiteralRegexpIsCaseSensitive() {
        assertThat(asInstanceOf(RegexpQuery.class, translateLiteral("/M(ount|t)/")).getRegexp(), equalTo(new Term("title", "M(ount|t)")));
        assertThat(asInstanceOf(RegexpQuery.class, translateLiteral("/m(ount|t)/")).getRegexp(), equalTo(new Term("title", "m(ount|t)")));
    }

    public void testLiteralRegexpMultiFieldFanOutIsCaseSensitive() {
        List<Term> regexps = terms(translate(of("/M(ount|t)/"), TITLE_BODY));
        assertThat(regexps, containsInAnyOrder(new Term("title", "M(ount|t)"), new Term("body", "M(ount|t)")));
    }

    public void testMatchMultipleTermsDefaultsToShould() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(match("title", "quick fox", null), TITLE));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
        }
        assertThat(terms(bq), containsInAnyOrder(new Term("title", "quick"), new Term("title", "fox")));
    }

    public void testMatchOperatorAnd() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(match("title", "quick fox", options("operator", "AND")), TITLE));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.MUST));
        }
    }

    public void testMatchMinimumShouldMatch() {
        BooleanQuery bq = asInstanceOf(
            BooleanQuery.class,
            translate(match("title", "quick brown fox", options("minimum_should_match", "2")), TITLE)
        );
        assertThat(bq.clauses(), hasSize(3));
        assertThat(bq.getMinimumNumberShouldMatch(), equalTo(2));
    }

    public void testMatchFuzziness() {
        FuzzyQuery fuzzy = asInstanceOf(FuzzyQuery.class, translate(match("title", "fox", options("fuzziness", "AUTO")), TITLE));
        assertThat(fuzzy.getTerm(), equalTo(new Term("title", "fox")));
        assertThat(fuzzy.getMaxEdits(), equalTo(1));
    }

    public void testMatchBoost() {
        BoostQuery boost = asInstanceOf(BoostQuery.class, translate(match("title", "fox", options("boost", 2.0)), TITLE));
        assertThat(boost.getBoost(), equalTo(2.0f));
        assertThat(boost.getQuery(), instanceOf(TermQuery.class));
    }

    public void testMatchPhrase() {
        PhraseQuery phrase = asInstanceOf(PhraseQuery.class, translate(matchPhrase("title", "quick fox", null), TITLE));
        assertThat(phrase.getTerms(), equalTo(new Term[] { new Term("title", "quick"), new Term("title", "fox") }));
        assertThat(phrase.getSlop(), equalTo(0));
    }

    public void testMatchPhraseSlop() {
        PhraseQuery phrase = asInstanceOf(PhraseQuery.class, translate(matchPhrase("title", "quick fox", options("slop", 2)), TITLE));
        assertThat(phrase.getSlop(), equalTo(2));
    }

    public void testQueryStringFieldQualifiedTargetsThatField() {
        assertThat(terms(translate(queryString("title:fox", null), TITLE_BODY)), containsInAnyOrder(new Term("title", "fox")));
    }

    public void testUnqualifiedQueryExpandsOverAllFields() {
        for (Expression query : List.of(queryString("fox", null), of("fox"))) {
            assertThat(terms(translate(query, TITLE_BODY)), containsInAnyOrder(new Term("title", "fox"), new Term("body", "fox")));
        }
    }

    public void testQueryStringDefaultFieldOption() {
        assertThat(
            terms(translate(queryString("fox", options("default_field", "body")), TITLE_BODY)),
            containsInAnyOrder(new Term("body", "fox"))
        );
    }

    public void testAnd() {
        And and = new And(EMPTY, match("title", "fox", null), match("body", "bar", null));
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(and, TITLE_BODY));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.MUST));
        }
        assertThat(terms(bq), containsInAnyOrder(new Term("title", "fox"), new Term("body", "bar")));
    }

    public void testOr() {
        Or or = new Or(EMPTY, match("title", "fox", null), match("body", "bar", null));
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(or, TITLE_BODY));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
        }
    }

    public void testNot() {
        Not not = new Not(EMPTY, match("title", "fox", null));
        // Non-scoring queries are wrapped in a zero-boost query.
        BoostQuery boost = asInstanceOf(BoostQuery.class, translate(not, TITLE));
        assertThat(boost.getBoost(), equalTo(0.0f));
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, boost.getQuery());
        BooleanClause filter = bq.clauses().stream().filter(c -> c.occur() == BooleanClause.Occur.FILTER).findFirst().orElseThrow();
        BooleanClause mustNot = bq.clauses().stream().filter(c -> c.occur() == BooleanClause.Occur.MUST_NOT).findFirst().orElseThrow();
        assertThat(filter.query(), instanceOf(MatchAllDocsQuery.class));
        assertThat(mustNot.query(), instanceOf(TermQuery.class));
    }

    public void testMatchLenientOptionHonored() {
        TermQuery term = asInstanceOf(TermQuery.class, translate(match("title", "fox", options("lenient", true)), TITLE));
        assertThat(term.getTerm(), equalTo(new Term("title", "fox")));
    }

    public void testMatchPhraseZeroTermsQueryOptionHonored() {
        assertThat(translate(matchPhrase("title", "fox", options("zero_terms_query", "all")), TITLE), instanceOf(TermQuery.class));
    }

    public void testQueryStringFuzzinessOptionHonored() {
        FuzzyQuery fuzzy = asInstanceOf(FuzzyQuery.class, translate(queryString("fox~", options("fuzziness", "2")), TITLE));
        assertThat(fuzzy.getMaxEdits(), equalTo(2));
    }

    public void testMatchPhraseFieldOutsideOnIsMatchNone() {
        assertThat(translate(matchPhrase("body", "quick fox", null), TITLE), instanceOf(MatchNoDocsQuery.class));
    }

    public void testQueryStringDefaultFieldOutsideOnIsMatchNone() {
        assertThat(translate(queryString("fox", options("default_field", "body")), TITLE), instanceOf(MatchNoDocsQuery.class));
    }

    public void testMatchInvalidOperatorThrows() {
        expectThrows(IllegalArgumentException.class, () -> translate(match("title", "fox", options("operator", "xor")), TITLE));
    }

    // PreMapper stores the rewritten Query DSL builder on the KQL function.
    public void testKqlUsesRewrittenQueryBuilder() {
        Kql kql = new Kql(EMPTY, of("title: fox"), null, new MatchQueryBuilder("title", "fox"), TEST_CFG);
        TermQuery term = asInstanceOf(TermQuery.class, translate(kql, TITLE));
        assertThat(term.getTerm(), equalTo(new Term("title", "fox")));
    }

    // Before PreMapper, KQL rewrites against the synthetic mapping.
    public void testKqlFreshBuilderParsesAgainstSyntheticMapping() {
        Kql kql = new Kql(EMPTY, of("title: fox"), null, TEST_CFG);
        TermQuery term = asInstanceOf(TermQuery.class, translate(kql, TITLE));
        assertThat(term.getTerm(), equalTo(new Term("title", "fox")));
    }

    public void testKqlFieldOutsideOnIsMatchNone() {
        Kql kql = new Kql(EMPTY, of("body: fox"), null, TEST_CFG);
        assertThat(translate(kql, TITLE), instanceOf(MatchNoDocsQuery.class));
    }

    private static List<Term> terms(Query query) {
        List<Term> collected = new ArrayList<>();
        collectTerms(query, collected);
        return collected;
    }

    private static void collectTerms(Query query, List<Term> collected) {
        switch (query) {
            case TermQuery term -> collected.add(term.getTerm());
            case FuzzyQuery fuzzy -> collected.add(fuzzy.getTerm());
            case RegexpQuery regexp -> collected.add(regexp.getRegexp());
            case WildcardQuery wildcard -> collected.add(wildcard.getTerm());
            case PhraseQuery phrase -> collected.addAll(List.of(phrase.getTerms()));
            case BoostQuery boost -> collectTerms(boost.getQuery(), collected);
            case ConstantScoreQuery constant -> collectTerms(constant.getQuery(), collected);
            case BooleanQuery bool -> bool.clauses().forEach(clause -> collectTerms(clause.query(), collected));
            case DisjunctionMaxQuery disMax -> disMax.getDisjuncts().forEach(disjunct -> collectTerms(disjunct, collected));
            default -> throw new AssertionError("unexpected query type: " + query.getClass());
        }
    }
}
