/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * The field names collected here are what pre-analysis asks field-caps for, so that a request filter applied to a view's
 * output can bind against real attributes. Under-collecting is the dangerous direction: a missing field binds to NULL and
 * the filter silently matches nothing, so the cases that cannot be enumerated must report
 * {@link QueryDslFieldNameExtractor.Result#requiresAllFields()} rather than a partial set.
 */
public class QueryDslFieldNameExtractorTests extends ESTestCase {

    private static QueryDslFieldNameExtractor.Result extract(org.elasticsearch.index.query.QueryBuilder filter) {
        return QueryDslFieldNameExtractor.extract(filter, EsqlTestUtils.TEST_CFG);
    }

    public void testTermCollectsField() {
        var result = extract(QueryBuilders.termQuery("region", "eu"));
        assertFalse(result.requiresAllFields());
        assertThat(result.fieldNames(), containsInAnyOrder("region"));
    }

    public void testTermsRangeExistsCollectFields() {
        assertThat(extract(QueryBuilders.termsQuery("status", List.of(200, 300))).fieldNames(), containsInAnyOrder("status"));
        assertThat(extract(QueryBuilders.rangeQuery("cnt").gt(0)).fieldNames(), containsInAnyOrder("cnt"));
        assertThat(extract(QueryBuilders.existsQuery("host.name")).fieldNames(), containsInAnyOrder("host.name"));
    }

    public void testMatchAndMatchPhraseCollectFields() {
        assertThat(extract(QueryBuilders.matchQuery("message", "boom")).fieldNames(), containsInAnyOrder("message"));
        assertThat(extract(QueryBuilders.matchPhraseQuery("message", "boom")).fieldNames(), containsInAnyOrder("message"));
    }

    /**
     * The Kibana-style shape: a bool combining clauses over fields the panel query need not mention. {@code must},
     * {@code filter} and {@code must_not} all restrict which rows match, so all of their fields are collected.
     * <p>
     * The {@code should} field is deliberately absent. Alongside a {@code must}/{@code filter} and without an explicit
     * {@code minimum_should_match}, a {@code should} arm gates scoring rather than matching, so {@link QueryDslTranslator}
     * does not translate it — the field genuinely is not referenced by the filter, and loading it would be waste. See
     * {@link #testRequiredShouldClausesAreCollected} for the shapes where {@code should} does restrict matching.
     */
    public void testBoolCollectsFromRestrictingSections() {
        var filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("service.name", "checkout"))
            .filter(QueryBuilders.rangeQuery("@timestamp").gte("now-15m"))
            .should(QueryBuilders.termQuery("region", "eu"))
            .mustNot(QueryBuilders.existsQuery("error.stack"));
        var result = extract(filter);
        assertFalse(result.requiresAllFields());
        assertThat(result.fieldNames(), containsInAnyOrder("service.name", "@timestamp", "error.stack"));
    }

    /**
     * A {@code should} arm restricts matching — and so must be collected — when it is required: either the bool has no
     * {@code must}/{@code filter} to satisfy, or {@code minimum_should_match} demands one.
     */
    public void testRequiredShouldClausesAreCollected() {
        var shouldOnly = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("region", "eu"))
            .should(QueryBuilders.termQuery("zone", "b"));
        assertThat(extract(shouldOnly).fieldNames(), containsInAnyOrder("region", "zone"));

        var withMinimumShouldMatch = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("service.name", "checkout"))
            .should(QueryBuilders.termQuery("region", "eu"))
            .minimumShouldMatch(1);
        assertThat(extract(withMinimumShouldMatch).fieldNames(), containsInAnyOrder("service.name", "region"));
    }

    public void testMatchAllCollectsNothing() {
        var result = extract(QueryBuilders.matchAllQuery());
        assertFalse(result.requiresAllFields());
        assertThat(result.fieldNames(), empty());
    }

    /**
     * {@code multi_match} resolves its fields by matching patterns against the source's complete field list, so its
     * references are unknowable here — even when it names the fields explicitly.
     */
    public void testMultiMatchRequiresAllFields() {
        var explicit = new MultiMatchQueryBuilder("eu", "region", "service.name");
        assertTrue(extract(explicit).requiresAllFields());

        var allFields = new MultiMatchQueryBuilder("eu");
        assertTrue(extract(allFields).requiresAllFields());
    }

    /** A multi_match nested inside a bool must still force the fallback, not just contribute its siblings' fields. */
    public void testMultiMatchNestedInBoolRequiresAllFields() {
        var filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("region", "eu"))
            .must(new MultiMatchQueryBuilder("boom", "message"));
        assertTrue(extract(filter).requiresAllFields());
    }

    /**
     * An unsupported construct leaves the collected set possibly incomplete, so it must report the fallback. Such a
     * filter fails the query fail-closed once it reaches a view, so over-requesting costs nothing.
     */
    public void testUnsupportedConstructRequiresAllFields() {
        assertTrue(extract(QueryBuilders.wildcardQuery("region", "e*")).requiresAllFields());
        var mixed = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("region", "eu"))
            .must(QueryBuilders.wildcardQuery("service.name", "check*"));
        assertTrue(extract(mixed).requiresAllFields());
    }

    /** The fallback result carries no names — callers must not mistake it for "no fields referenced". */
    public void testFallbackResultCarriesNoNames() {
        var result = extract(QueryBuilders.wildcardQuery("region", "e*"));
        assertThat(result.requiresAllFields(), equalTo(true));
        assertThat(result.fieldNames(), empty());
    }
}
