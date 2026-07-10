/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** Unit tests for {@link HighlightQueryTranslator}. */
public class HighlightQueryTranslatorTests extends ESTestCase {

    private static final List<String> TITLE = List.of("title");
    private static final List<String> TITLE_BODY = List.of("title", "body");

    private static Query translate(Expression query, List<String> fields) {
        return HighlightQueryTranslator.translate(query, fields, new StandardAnalyzer());
    }

    private static Query translateLiteral(String text) {
        return HighlightQueryTranslator.translateLiteral(text, TITLE, new StandardAnalyzer());
    }

    private static Match match(String field, String text, MapExpression options) {
        return new Match(EMPTY, getFieldAttribute(field, KEYWORD), of(text), options, TEST_CFG);
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
        assertThat(HighlightQueryTranslator.translateLiteral(null, TITLE, new StandardAnalyzer()), instanceOf(MatchNoDocsQuery.class));
    }

    public void testLiteralAllTermsFilteredIsMatchNoDocs() {
        Query query = HighlightQueryTranslator.translateLiteral("the", TITLE, new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET));
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testLiteralLeadingWildcardAllowed() {
        assertThat(translateLiteral("*ox"), instanceOf(WildcardQuery.class));
    }

    public void testLiteralFuzzyDistances() {
        FuzzyQuery query = asInstanceOf(FuzzyQuery.class, translateLiteral("quick~"));
        assertThat(query.getMaxEdits(), equalTo(1));

        query = asInstanceOf(FuzzyQuery.class, translateLiteral("fx~"));
        assertThat(query.getMaxEdits(), equalTo(0));

        query = asInstanceOf(FuzzyQuery.class, translateLiteral("fox~2"));
        assertThat(query.getMaxEdits(), equalTo(2));
    }

    // Query DSL query_string builds regexp queries case-sensitively (unlike wildcard/prefix, the pattern is not
    // lowercased through the analyzer), so an uppercase pattern must stay uppercase and not match a lowercased term.
    public void testLiteralRegexpIsCaseSensitive() {
        RegexpQuery upper = asInstanceOf(RegexpQuery.class, translateLiteral("/M(ount|t)/"));
        assertThat(upper.getRegexp(), equalTo(new Term("title", "M(ount|t)")));

        RegexpQuery lower = asInstanceOf(RegexpQuery.class, translateLiteral("/m(ount|t)/"));
        assertThat(lower.getRegexp(), equalTo(new Term("title", "m(ount|t)")));
    }

    public void testLiteralRegexpMultiFieldFanOutIsCaseSensitive() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(of("/M(ount|t)/"), TITLE_BODY));
        assertThat(bq.clauses(), hasSize(2));
        List<Term> regexps = new ArrayList<>();
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
            regexps.add(asInstanceOf(RegexpQuery.class, clause.query()).getRegexp());
        }
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

    public void testMatchOperatorIsCaseInsensitive() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(match("title", "quick fox", options("operator", "and")), TITLE));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.MUST));
        }
    }

    public void testMatchRejectsInvalidOperator() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(match("title", "quick fox", options("operator", "xor")), TITLE)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT MATCH [operator] must be one of [OR, AND], found [xor]"));
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

    public void testMatchRejectsUnsupportedOption() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(match("title", "fox", options("lenient", true)), TITLE)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT does not support the [lenient] option of [MATCH]"));
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

    public void testMatchPhraseRejectsUnsupportedOption() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(matchPhrase("title", "fox", options("zero_terms_query", "all")), TITLE)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT does not support the [zero_terms_query] option of [MATCH_PHRASE]"));
    }

    public void testMatchPhraseRejectsFieldOutsideOn() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(matchPhrase("body", "quick fox", null), TITLE)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT query field [body] is not in ON fields [title]"));
    }

    public void testQueryStringFieldQualifiedTargetsThatField() {
        Query query = translate(queryString("title:fox", null), TITLE_BODY);
        assertThat(terms(query), containsInAnyOrder(new Term("title", "fox")));
    }

    public void testUnqualifiedQueryExpandsOverAllFields() {
        for (Expression query : List.of(queryString("fox", null), of("fox"))) {
            BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(query, TITLE_BODY));
            assertThat(bq.clauses(), hasSize(2));
            for (BooleanClause clause : bq.clauses()) {
                assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
            }
            assertThat(terms(bq), containsInAnyOrder(new Term("title", "fox"), new Term("body", "fox")));
        }
    }

    public void testQueryStringDefaultFieldOption() {
        Query query = translate(queryString("fox", options("default_field", "body")), TITLE_BODY);
        assertThat(terms(query), containsInAnyOrder(new Term("body", "fox")));
    }

    public void testQueryStringRejectsUnsupportedOption() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(queryString("fox", options("fuzziness", "AUTO")), TITLE_BODY)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT does not support the [fuzziness] option of [QSTR]"));
    }

    public void testQueryStringRejectsDefaultFieldOutsideOn() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(queryString("fox", options("default_field", "body")), TITLE)
        );
        assertThat(e.getMessage(), equalTo("HIGHLIGHT query field [body] is not in ON fields [title]"));
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
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, translate(not, TITLE));
        assertThat(bq.clauses(), hasSize(2));
        BooleanClause must = bq.clauses().stream().filter(c -> c.occur() == BooleanClause.Occur.MUST).findFirst().orElseThrow();
        BooleanClause mustNot = bq.clauses().stream().filter(c -> c.occur() == BooleanClause.Occur.MUST_NOT).findFirst().orElseThrow();
        assertThat(must.query(), instanceOf(MatchAllDocsQuery.class));
        assertThat(mustNot.query(), instanceOf(TermQuery.class));
    }

    private static List<Term> terms(Query query) {
        List<Term> collected = new ArrayList<>();
        collectTerms(query, collected);
        return collected;
    }

    private static void collectTerms(Query query, List<Term> collected) {
        if (query instanceof TermQuery term) {
            collected.add(term.getTerm());
        } else if (query instanceof FuzzyQuery fuzzy) {
            collected.add(fuzzy.getTerm());
        } else if (query instanceof PhraseQuery phrase) {
            collected.addAll(List.of(phrase.getTerms()));
        } else if (query instanceof BoostQuery boost) {
            collectTerms(boost.getQuery(), collected);
        } else if (query instanceof BooleanQuery bool) {
            bool.clauses().forEach(clause -> collectTerms(clause.query(), collected));
        } else {
            throw new AssertionError("unexpected query type: " + query.getClass());
        }
    }
}
