/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
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
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
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

    /** Child overhead and parent admission must be tried before publishing the optional filter. */
    public void testActualChildAndParentBreakerAdmissionPreservesRequiredHits() throws Exception {
        var mapper = mapper("{\"type\":\"keyword\"}");
        try (var dir = newDirectory(); var writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (String scope : List.of("allowed", "denied")) {
                writer.addDocument(
                    List.of(
                        new StringField("phase", "A", Field.Store.NO),
                        new StringField("key", "wanted", Field.Store.NO),
                        new StringField("physical", scope, Field.Store.YES)
                    )
                );
            }
            try (var reader = DirectoryReader.open(writer)) {
                var searcher = newSearcher(reader);
                var mapping = createSearchExecutionContext(mapper, searcher);
                var base = new TermQueryBuilder("phase", "A");
                var membership = new TermQueryBuilder("physical", "allowed");
                var requiredScope = new BoolQueryBuilder().must(base).filter(membership);
                var off = guarded(requiredScope, null, mapping);
                var on = guarded(requiredScope, new TermQueryBuilder("key", "wanted"), mapping);
                var measurement = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);
                off.toQuery(mapping, measurement);
                long requiredBytes = measurement.getEstimatedBytes();
                assertTrue(requiredBytes > 0);
                for (String limitingBreaker : List.of(CircuitBreaker.REQUEST, CircuitBreaker.PARENT)) {
                    String boundary = (2 * requiredBytes) + "b";
                    var settings = Settings.builder()
                        .put("indices.breaker.total.use_real_memory", false)
                        .put("indices.breaker.request.limit", CircuitBreaker.REQUEST.equals(limitingBreaker) ? boundary : "10mb")
                        .put("indices.breaker.total.limit", CircuitBreaker.PARENT.equals(limitingBreaker) ? boundary : "95%")
                        .put("indices.breaker.request.overhead", 2.0)
                        .build();
                    var service = new HierarchyCircuitBreakerService(
                        CircuitBreakerMetrics.NOOP,
                        settings,
                        List.of(),
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    );
                    var breaker = service.getBreaker(CircuitBreaker.REQUEST);
                    var context = new SearchExecutionContext(mapping, breaker);
                    Query expected = off.toQuery(context);
                    try {
                        assertEquals(requiredBytes, breaker.getUsed());
                        var hits = searcher.search(expected, 2).scoreDocs;
                        assertEquals(1, hits.length);
                        assertEquals("allowed", searcher.storedFields().document(hits[0].doc).get("physical"));
                    } finally {
                        context.releaseQueryConstructionMemory();
                    }
                    assertEquals(0, breaker.getUsed());
                    assertEquals(0, service.stats().getStats(limitingBreaker).getTrippedCount());
                    try {
                        Query actual = on.toQuery(context);
                        assertEquals(expected, actual);
                        var hits = searcher.search(actual, 2).scoreDocs;
                        assertEquals(1, hits.length);
                        assertEquals("allowed", searcher.storedFields().document(hits[0].doc).get("physical"));
                        assertEquals(1, service.stats().getStats(limitingBreaker).getTrippedCount());
                        assertEquals(requiredBytes, breaker.getUsed());
                        assertEquals(requiredBytes, context.getQueryConstructionMemoryUsed());
                    } finally {
                        context.releaseQueryConstructionMemory();
                    }
                    assertEquals(0, breaker.getUsed());
                    // An enclosing bool can add a required sibling after the plugin has accepted two leaves.
                    var partialOff = guarded(base, null, mapping);
                    var partialOn = guarded(base, membership, mapping);
                    var offWithSibling = new BoolQueryBuilder().must(partialOff).filter(membership);
                    var onWithSibling = new BoolQueryBuilder().must(partialOn).filter(membership);
                    try {
                        assertEquals(1, searcher.count(offWithSibling.toQuery(context)));
                    } finally {
                        context.releaseQueryConstructionMemory();
                    }
                    try {
                        expectThrows(CircuitBreakingException.class, () -> onWithSibling.toQuery(context));
                    } finally {
                        context.releaseQueryConstructionMemory();
                    }
                    assertEquals(0, breaker.getUsed());

                    // Reproduce the interleaving between AbstractQueryBuilder's conversion and its final charge.
                    var accepted = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);
                    assertTrue(partialOn.toQuery(context, accepted) instanceof BooleanQuery);
                    assertEquals(requiredBytes, accepted.getEstimatedBytes());
                    assertEquals(0, breaker.getUsed());
                    breaker.addEstimateBytesAndMaybeBreak(1, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
                    try {
                        assertEquals(2, searcher.count(partialOff.toQuery(context)));
                        context.releaseQueryConstructionMemory();
                        expectThrows(
                            CircuitBreakingException.class,
                            () -> context.addCircuitBreakerMemory(accepted.getEstimatedBytes(), ChildMemoryCircuitBreaker.CATEGORY_QUERY)
                        );
                    } finally {
                        context.releaseQueryConstructionMemory();
                        breaker.addWithoutBreaking(-1, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
                    }
                    assertEquals(0, breaker.getUsed());
                    for (var required : List.of(off, on)) {
                        var tooLarge = new BoolQueryBuilder().must(required).filter(new TermQueryBuilder("physical", "extra"));
                        try {
                            expectThrows(CircuitBreakingException.class, () -> tooLarge.toQuery(context));
                        } finally {
                            context.releaseQueryConstructionMemory();
                        }
                        assertEquals(0, breaker.getUsed());
                    }
                }
            }
        }
        assertWarnings(
            "[indices.breaker.total.limit] should be specified using a percentage of the heap. "
                + "Absolute size settings will be forbidden in a future release"
        );
    }

    public void testRawVisitorCeilingWhenChildOverheadIsBelowOne() throws Exception {
        var mapping = createSearchExecutionContext(mapper("{\"type\":\"keyword\"}"));
        var term = new TermQueryBuilder("key", "value");
        var off = guarded(term, null, mapping);
        var on = guarded(term, term, mapping);
        var measure = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);
        off.toQuery(mapping, measure);
        long oneLeaf = measure.getEstimatedBytes();
        var settings = Settings.builder()
            .put("indices.breaker.total.use_real_memory", false)
            .put("indices.breaker.request.limit", (oneLeaf + oneLeaf / 2) + "b")
            .put("indices.breaker.request.overhead", 0.5)
            .build();
        var service = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        var breaker = service.getBreaker(CircuitBreaker.REQUEST);
        var context = new SearchExecutionContext(mapping, breaker);
        // The real child/parent reservation fits; merging two raw leaves into the root visitor would trip it.
        breaker.addEstimateBytesAndMaybeBreak(2 * oneLeaf, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
        breaker.addWithoutBreaking(-2 * oneLeaf, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
        try {
            Query expected = off.toQuery(context);
            context.releaseQueryConstructionMemory();
            assertEquals(expected, on.toQuery(context));
            assertEquals(oneLeaf, breaker.getUsed());
        } finally {
            context.releaseQueryConstructionMemory();
        }
        assertEquals(0, breaker.getUsed());
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
