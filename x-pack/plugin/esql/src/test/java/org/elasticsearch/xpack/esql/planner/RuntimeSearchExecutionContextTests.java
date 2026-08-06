/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.ZeroTermsQueryOption;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** Tests the synthetic fields exposed by {@link RuntimeSearchExecutionContext}. */
public class RuntimeSearchExecutionContextTests extends ESTestCase {

    private static final List<String> TITLE_BODY = List.of("title", "body");

    private static Query query(QueryBuilder builder) {
        return HighlightQueryBuilders.toLuceneQuery(builder, RuntimeSearchExecutionContext.create(TITLE_BODY));
    }

    public void testMatchBuildsTermsOnSyntheticField() {
        BooleanQuery bq = asInstanceOf(BooleanQuery.class, query(new MatchQueryBuilder("title", "fox jumps")));
        assertThat(bq.clauses(), hasSize(2));
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.occur(), equalTo(BooleanClause.Occur.SHOULD));
            assertThat(clause.query(), instanceOf(TermQuery.class));
        }
        assertThat(terms(bq), containsInAnyOrder(new Term("title", "fox"), new Term("title", "jumps")));
    }

    public void testUnmappedFieldIsMatchNone() {
        assertThat(query(new MatchQueryBuilder("nope", "fox")), instanceOf(MatchNoDocsQuery.class));
    }

    public void testExistsIsMatchNone() {
        assertThat(query(QueryBuilders.queryStringQuery("_exists_:title").field("title")), instanceOf(MatchNoDocsQuery.class));
    }

    public void testBareLiteralExpandsOverOnFields() {
        // The default "*" field expands to all synthetic fields.
        DisjunctionMaxQuery disMax = asInstanceOf(DisjunctionMaxQuery.class, query(QueryBuilders.queryStringQuery("fox")));
        assertThat(terms(disMax), containsInAnyOrder(new Term("title", "fox"), new Term("body", "fox")));
    }

    // Exercise all builder options to catch new dependencies on shard state.

    public void testMatchQueryEveryOption() {
        MatchQueryBuilder builder = new MatchQueryBuilder("title", "quick fox").operator(Operator.AND)
            .analyzer("default")
            .fuzziness(Fuzziness.ONE)
            .prefixLength(1)
            .maxExpansions(10)
            .fuzzyTranspositions(randomBoolean())
            .fuzzyRewrite("top_terms_10")
            .minimumShouldMatch("1")
            .lenient(randomBoolean())
            .zeroTermsQuery(ZeroTermsQueryOption.ALL)
            .autoGenerateSynonymsPhraseQuery(randomBoolean())
            .boost(1.5f)
            .queryName("named");
        assertNotNull(query(builder));
    }

    public void testMatchPhraseEveryOption() {
        MatchPhraseQueryBuilder builder = new MatchPhraseQueryBuilder("title", "quick fox").analyzer("default")
            .slop(2)
            .zeroTermsQuery(ZeroTermsQueryOption.ALL)
            .boost(0.5f)
            .queryName("named");
        assertNotNull(query(builder));
    }

    public void testQueryStringEveryOption() {
        String syntax = "title:fox* OR \"quick fox\" OR fox~ OR /f[ao]x/ OR [a TO f] OR *ox OR _exists_:title OR (+fox -bar)";
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery(syntax)
            .field("title")
            .field("body", 2.0f)
            .defaultOperator(Operator.OR)
            .analyzer("default")
            .quoteAnalyzer("default")
            .quoteFieldSuffix(".exact")
            .allowLeadingWildcard(true)
            .analyzeWildcard(true)
            .enablePositionIncrements(true)
            .escape(false)
            .fuzziness(Fuzziness.AUTO)
            .fuzzyMaxExpansions(10)
            .fuzzyPrefixLength(1)
            .fuzzyTranspositions(randomBoolean())
            .fuzzyRewrite("constant_score")
            .rewrite("constant_score")
            .maxDeterminizedStates(5000)
            .phraseSlop(1)
            .tieBreaker(0.3f)
            .timeZone("+01:00")
            .minimumShouldMatch("1")
            .lenient(randomBoolean())
            .type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
            .boost(2.0f)
            .queryName("named");
        assertNotNull(query(builder));
    }

    private static List<Term> terms(Query query) {
        List<Term> collected = new ArrayList<>();
        collectTerms(query, collected);
        return collected;
    }

    private static void collectTerms(Query query, List<Term> collected) {
        switch (query) {
            case TermQuery term -> collected.add(term.getTerm());
            case BooleanQuery bool -> bool.clauses().forEach(clause -> collectTerms(clause.query(), collected));
            case DisjunctionMaxQuery disMax -> disMax.getDisjuncts().forEach(disjunct -> collectTerms(disjunct, collected));
            default -> throw new AssertionError("unexpected query type: " + query.getClass());
        }
    }
}
