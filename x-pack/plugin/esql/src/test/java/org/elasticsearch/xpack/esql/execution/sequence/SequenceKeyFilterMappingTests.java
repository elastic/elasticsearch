/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;

import java.util.List;
import java.util.Map;

/** Native mapper contexts prove that optional narrowing cannot replace ordinary field validation. */
public class SequenceKeyFilterMappingTests extends MapperServiceTestCase {
    /** The whitelist preserves native scalar precision and retains only the ordinary named query. */
    public void testNativeKeywordBooleanAndIntegralTerms() throws Exception {
        for (Map.Entry<String, Object> sample : Map.<String, Object>of(
            "keyword",
            "value",
            "boolean",
            true,
            "byte",
            127,
            "short",
            32767,
            "integer",
            Integer.MAX_VALUE,
            "long",
            9_007_199_254_740_993L
        ).entrySet()) {
            MapperService mapper = mapper("{\"type\":\"" + sample.getKey() + "\"}");
            SearchExecutionContext context = createSearchExecutionContext(mapper);
            var base = new TermQueryBuilder("phase", "A").queryName("ordinary-base");
            Query filtered = guarded(base, new TermQueryBuilder("key", sample.getValue()), context).toQuery(context);
            assertTrue("native mapping must admit its exact canonical scalar", filtered instanceof BooleanQuery);
            BooleanQuery bool = (BooleanQuery) filtered;
            assertEquals(2, bool.clauses().size());
            assertTrue(bool.clauses().stream().allMatch(clause -> clause.occur() == BooleanClause.Occur.FILTER));
            assertEquals(List.of("ordinary-base"), context.copyNamedQueries().keySet().stream().toList());
        }
    }

    /** Terms must not be applied to doc-values-only, source-only, null-substituting or field-alias producers. */
    public void testUnsupportedNativeStorageAndAliasFallBack() throws Exception {
        for (String keyMapping : List.of(
            "{\"type\":\"keyword\",\"index\":false}",
            "{\"type\":\"keyword\",\"doc_values\":false}",
            "{\"type\":\"keyword\",\"null_value\":\"replacement\"}",
            "{\"type\":\"alias\",\"path\":\"physical\"}"
        )) {
            MapperService mapper = mapper(keyMapping);
            SearchExecutionContext context = createSearchExecutionContext(mapper);
            var base = new TermQueryBuilder("phase", "A");
            Query expected = base.toQuery(context);
            Query filtered = guarded(base, new TermQueryBuilder("key", "value"), context).toQuery(context);
            assertEquals(expected, filtered);
        }
    }

    /** A legal mapper paired with the wrong canonical type must remain unfiltered instead of coercing the key. */
    public void testCanonicalValueTypeAndNarrowIntegerRangeFallBack() throws Exception {
        for (Map.Entry<String, Object> sample : Map.<String, Object>of(
            "keyword",
            1,
            "boolean",
            "true",
            "byte",
            128,
            "short",
            32768,
            "integer",
            1L,
            "long",
            1
        ).entrySet()) {
            SearchExecutionContext context = createSearchExecutionContext(mapper("{\"type\":\"" + sample.getKey() + "\"}"));
            var base = new TermQueryBuilder("phase", "A");
            assertEquals(base.toQuery(context), guarded(base, new TermQueryBuilder("key", sample.getValue()), context).toQuery(context));
        }
    }

    /** The empty-string compatibility setting changes term semantics and must disable that optional literal. */
    public void testEmptyKeywordNullCompatibilityFallsBack() throws Exception {
        Settings settings = Settings.builder().put(FieldMapper.EMPTY_KEYWORD_STRING_AS_NULL_SETTING.getKey(), true).build();
        MapperService mapper = createMapperService(settings, mapping("{\"type\":\"keyword\"}"));
        SearchExecutionContext context = createSearchExecutionContext(mapper);
        var base = new TermQueryBuilder("phase", "A");
        assertEquals(base.toQuery(context), guarded(base, new TermQueryBuilder("key", ""), context).toQuery(context));
    }

    /** MatchNone in the optional position cannot erase an ordinary source query's malformed numeric-bound failure. */
    public void testOrdinaryBaseRefusalSurvivesOptionalMatchNone() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(mapper("{\"type\":\"keyword\"}"));
        var base = new RangeQueryBuilder("number").gte("not-a-number");
        Exception ordinary = expectThrows(Exception.class, () -> guarded(base, null, context).toQuery(context));
        Exception filtered = expectThrows(Exception.class, () -> guarded(base, new MatchNoneQueryBuilder(), context).toQuery(context));
        assertEquals(ordinary.getClass(), filtered.getClass());
        assertEquals(ordinary.getMessage(), filtered.getMessage());
    }

    /** Optional clause exhaustion must leave both the real ordinary query and its shared visitor count intact. */
    public void testOptionalClauseBudgetNeverPublishesPrefix() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(mapper("{\"type\":\"keyword\"}"));
        var base = new TermQueryBuilder("phase", "A");
        QueryBuilder alternatives = new BoolQueryBuilder().minimumShouldMatch(1)
            .should(new BoolQueryBuilder().filter(new TermQueryBuilder("key", "first")))
            .should(new BoolQueryBuilder().filter(new TermQueryBuilder("key", "second")));
        Query expected = base.toQuery(context);
        for (int limit : List.of(1, 2)) {
            var visitor = new MaxClauseCountQueryVisitor(limit);
            assertEquals(expected, guarded(base, alternatives, context).toQuery(context, visitor));
            assertEquals("failed optional construction must not mutate the ordinary visitor", 1, visitor.getNumClauses());
        }
        var visitor = new MaxClauseCountQueryVisitor(4);
        assertTrue(guarded(base, alternatives, context).toQuery(context, visitor) instanceof BooleanQuery);
        assertEquals(3, visitor.getNumClauses());
    }

    private static SequenceSourceQueryBuilder guarded(QueryBuilder base, QueryBuilder predicate, SearchExecutionContext context) {
        return new SequenceSourceQueryBuilder(
            predicate == null ? base : SequenceSourceQueryBuilder.withKeyFilter(base, predicate),
            List.of(context.index().getName())
        );
    }

    private MapperService mapper(String keyMapping) throws Exception {
        return createMapperService(mapping(keyMapping));
    }

    private static String mapping(String keyMapping) {
        return """
            {"_doc":{"properties":{"@timestamp":{"type":"date_nanos"},"phase":{"type":"keyword"},
              "number":{"type":"long"},"physical":{"type":"keyword"},"key":%s}}}
            """.formatted(keyMapping);
    }
}
