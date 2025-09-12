/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.instanceOf;

/** Tests for the rrf retriever. */
public class RRFRetrieverBuilderTests extends ESTestCase {

    /** Tests extraction errors related to compound retrievers. These tests require a compound retriever which is why they are here. */
    public void testRetrieverExtractionErrors() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"retriever\":{\"rrf_nl\":{\"retrievers\":"
                    + "[{\"standard\":{\"search_after\":[1]}},{\"standard\":{\"search_after\":[2]}}]}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ssb.parseXContent(parser, true, nf -> true)
                    .rewrite(
                        new QueryRewriteContext(
                            parserConfig(),
                            null,
                            null,
                            TransportVersion.current(),
                            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                            null,
                            new PointInTimeBuilder(new BytesArray("pitid")),
                            null,
                            null,
                            false
                        )
                    )
            );
            assertEquals("[search_after] cannot be used in children of compound retrievers", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"retriever\":{\"rrf_nl\":{\"retrievers\":"
                    + "[{\"standard\":{\"terminate_after\":1}},{\"standard\":{\"terminate_after\":2}}]}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ssb.parseXContent(parser, true, nf -> true)
                    .rewrite(
                        new QueryRewriteContext(
                            parserConfig(),
                            null,
                            null,
                            TransportVersion.current(),
                            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                            null,
                            new PointInTimeBuilder(new BytesArray("pitid")),
                            null,
                            null,
                            false
                        )
                    )
            );
            assertEquals("[terminate_after] cannot be used in children of compound retrievers", iae.getMessage());
        }
    }

    public void testRRFRetrieverParsingSyntax() throws IOException {
        BiConsumer<String, float[]> testCase = (json, expectedWeights) -> {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
                SearchSourceBuilder ssb = new SearchSourceBuilder().parseXContent(parser, true, nf -> true);
                assertThat(ssb.retriever(), instanceOf(RRFRetrieverBuilder.class));
                RRFRetrieverBuilder rrf = (RRFRetrieverBuilder) ssb.retriever();
                assertArrayEquals(expectedWeights, rrf.weights(), 0.001f);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        String legacyJson = """
            {
              "retriever": {
                "rrf_nl": {
                  "retrievers": [
                    { "standard": { "query": { "match_all": {} } } },
                    { "standard": { "query": { "match_all": {} } } }
                  ]
                }
              }
            }
            """;
        testCase.accept(legacyJson, new float[] { 1.0f, 1.0f });

        String weightedJson = """
            {
              "retriever": {
                "rrf_nl": {
                  "retrievers": [
                    { "retriever": { "standard": { "query": { "match_all": {} } } }, "weight": 2.5 },
                    { "retriever": { "standard": { "query": { "match_all": {} } } }, "weight": 0.5 }
                  ]
                }
              }
            }
            """;
        testCase.accept(weightedJson, new float[] { 2.5f, 0.5f });

        String mixedJson = """
            {
              "retriever": {
                "rrf_nl": {
                  "retrievers": [
                    { "standard": { "query": { "match_all": {} } } },
                    { "retriever": { "standard": { "query": { "match_all": {} } } }, "weight": 0.6 }
                  ]
                }
              }
            }
            """;
        testCase.accept(mixedJson, new float[] { 1.0f, 0.6f });
    }

    public void testMultiFieldsParamsRewrite() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            false
        );

        // No wildcards, no per-field boosting
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo"
        );

        // Non-default rank window size
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            DEFAULT_RANK_WINDOW_SIZE * 2,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo2"
        );
    }

    public void testMultiFieldsParamsRewriteWithWeights() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Simple per-field boosting
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.5f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 2.0f),
            "bar"
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "*_field_1", 2.5f),
            Map.of("semantic_field_1", 2.5f),
            "baz"
        );

        // Multiple boosts defined on the same field
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "field_1", 3.0f, "*_field_1", 2.5f, "semantic_*", 1.5f),
            Map.of("semantic_field_1", 3.75f, "semantic_field_2", 1.5f),
            "baz2"
        );

        // All-fields wildcard with weights
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("*^2.0"),
            "qux",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("*", 2.0f),
            Map.of("semantic_field_1", 2.0f, "semantic_field_2", 2.0f),
            "qux"
        );

        // Zero weights (testing that zero is allowed as non-negative)
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^0", "field_2^1.0"),
            "zero_test",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 0.0f, "field_2", 1.0f),
            Map.of(),
            "zero_test"
        );
    }

    public void testNegativeWeightValidation() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test negative weight validation
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^-1.0"),
            "negative_test",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> rrfRetrieverBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("[rrf] per-field weights must be non-negative", iae.getMessage());
    }

    public void testSearchRemoteIndex() {
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(
            "local-index",
            List.of(),
            Map.of("remote-cluster", "remote-index")
        );
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            false
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            null,
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> rrfRetrieverBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("[rrf] cannot specify [query] when querying remote indices", iae.getMessage());
    }

    public void testPerFieldWeightsBasic() {
        // Test that per-field weights are accepted and parsed correctly
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test with per-field weights in the simplified format
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^2.0", "field_2^0.5", "semantic_field_1"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // This should not throw an exception anymore
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testLexicalFieldWeightPropagation() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^2.0", "field_2^0.5"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
        RRFRetrieverBuilder rewrittenRrf = (RRFRetrieverBuilder) rewritten;

        // Find the StandardRetrieverBuilder with MultiMatchQuery
        StandardRetrieverBuilder standardRetriever = null;
        for (CompoundRetrieverBuilder.RetrieverSource source : rewrittenRrf.innerRetrievers()) {
            if (source.retriever() instanceof StandardRetrieverBuilder stdRetriever) {
                QueryBuilder topDocsQuery = stdRetriever.topDocsQuery();
                if (topDocsQuery instanceof MultiMatchQueryBuilder) {
                    standardRetriever = stdRetriever;
                    break;
                }
            }
        }

        assertNotNull("StandardRetrieverBuilder with MultiMatchQuery should exist", standardRetriever);
        MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) standardRetriever.topDocsQuery();
        Map<String, Float> actualFields = multiMatch.fields();
        Map<String, Float> expectedFields = Map.of("field_1", 2.0f, "field_2", 0.5f);
        assertEquals(expectedFields, actualFields);
    }

    public void testInferenceFieldWeights() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("semantic_field_1^3.0", "semantic_field_2^0.5"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
        RRFRetrieverBuilder rewrittenRrf = (RRFRetrieverBuilder) rewritten;

        // Find the inner RRFRetrieverBuilder produced by innerNormalizerGenerator
        RRFRetrieverBuilder innerRrf = null;
        for (CompoundRetrieverBuilder.RetrieverSource source : rewrittenRrf.innerRetrievers()) {
            if (source.retriever() instanceof RRFRetrieverBuilder) {
                innerRrf = (RRFRetrieverBuilder) source.retriever();
                break;
            }
        }

        assertNotNull("Inner RRFRetrieverBuilder should exist", innerRrf);
        float[] actualWeights = innerRrf.weights();
        assertEquals("Should have exactly 2 weights", 2, actualWeights.length);
        
        // Sort both arrays to ensure deterministic comparison regardless of HashMap iteration order
        float[] expectedWeights = new float[] { 3.0f, 0.5f };
        Arrays.sort(actualWeights);
        Arrays.sort(expectedWeights);
        assertArrayEquals(expectedWeights, actualWeights, 0.001f);
    }

    public void testNegativeWeightsRejected() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field^-1"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> rrfRetrieverBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("[rrf] per-field weights must be non-negative", iae.getMessage());
    }

    public void testZeroWeightsAccepted() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field^0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // This should not throw an exception
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testLargeWeightValues() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test very large weight values
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^1000000", "field_2^1.0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // This should not throw an exception
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testMixedWeightedAndUnweightedFields() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test mixing weighted and unweighted fields in simplified syntax
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("title^2.5", "content", "tags^1.5", "description"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // Should successfully rewrite mixed weighted/unweighted fields
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testDecimalWeightPrecision() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test various decimal weight precisions
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field1^0.1", "field2^2.75", "field3^10.999"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // Should handle decimal precision correctly
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testSimplifiedSyntaxWithGlobPatterns() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test glob patterns with weights in simplified syntax
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("title_*^2.0", "content_*^1.5", "meta_*"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // Should handle glob patterns with weights correctly
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    public void testExtremelyLargeWeights() {
        final String indexName = "test-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, List.of(), null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false
        );

        // Test potential overflow scenarios with very large float values
        float largeWeight = 1e20f;
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^" + largeWeight, "field_2^1.0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );

        // This should not throw an exception - large weights should be handled gracefully
        RetrieverBuilder rewritten = rrfRetrieverBuilder.doRewrite(queryRewriteContext);
        assertNotSame(rrfRetrieverBuilder, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new SearchModule(Settings.EMPTY, List.of()).getNamedXContents();
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRetrieverBuilder.NAME),
                (p, c) -> RRFRetrieverBuilder.fromXContent(p, (RetrieverParserContext) c)
            )
        );
        // Add an entry with no license requirement for unit testing
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRetrieverBuilder.NAME + "_nl"),
                (p, c) -> RRFRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    private static ResolvedIndices createMockResolvedIndices(
        String localIndexName,
        List<String> inferenceFields,
        Map<String, String> remoteIndexNames
    ) {
        Index index = new Index(localIndexName, randomAlphaOfLength(10));
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(1)
            .numberOfReplicas(0);

        for (String inferenceField : inferenceFields) {
            indexMetadataBuilder.putInferenceField(
                new InferenceFieldMetadata(inferenceField, randomAlphaOfLengthBetween(3, 5), new String[] { inferenceField }, null)
            );
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        if (remoteIndexNames != null) {
            for (Map.Entry<String, String> entry : remoteIndexNames.entrySet()) {
                remoteIndices.put(entry.getKey(), new OriginalIndices(new String[] { entry.getValue() }, IndicesOptions.DEFAULT));
            }
        }

        return new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(new String[] { localIndexName }, IndicesOptions.DEFAULT),
            Map.of(index, indexMetadataBuilder.build())
        );
    }

    private static void assertMultiFieldsParamsRewrite(
        RRFRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery
    ) {
        Set<Object> expectedInnerRetrievers = Set.of(
            CompoundRetrieverBuilder.RetrieverSource.from(
                new StandardRetrieverBuilder(
                    new MultiMatchQueryBuilder(expectedQuery).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                        .fields(expectedNonInferenceFields)
                )
            ),
            Set.of(
                expectedInferenceFields.entrySet()
                    .stream()
                    .map(
                        e -> CompoundRetrieverBuilder.RetrieverSource.from(
                            new StandardRetrieverBuilder(new MatchQueryBuilder(e.getKey(), expectedQuery))
                        )
                    )
                    .toArray()
            )
        );

        RetrieverBuilder rewritten = retriever.doRewrite(ctx);
        assertNotSame(retriever, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);

        RRFRetrieverBuilder rewrittenRrf = (RRFRetrieverBuilder) rewritten;
        assertEquals(retriever.rankWindowSize(), rewrittenRrf.rankWindowSize());
        assertEquals(retriever.rankConstant(), rewrittenRrf.rankConstant());
        assertEquals(expectedInnerRetrievers, getInnerRetrieversAsSet(rewrittenRrf));
    }

    private static void assertMultiFieldsParamsRewriteWithWeights(
        RRFRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery
    ) {
        Set<Object> expectedInnerRetrievers = new HashSet<>();
        expectedInnerRetrievers.add(
            CompoundRetrieverBuilder.RetrieverSource.from(
                new StandardRetrieverBuilder(
                    new MultiMatchQueryBuilder(expectedQuery).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                        .fields(expectedNonInferenceFields)
                )
            )
        );

        if (expectedInferenceFields.isEmpty() == false) {
            expectedInnerRetrievers.add(
                Set.of(
                    expectedInferenceFields.entrySet()
                        .stream()
                        .map(
                            e -> CompoundRetrieverBuilder.RetrieverSource.from(
                                new StandardRetrieverBuilder(new MatchQueryBuilder(e.getKey(), expectedQuery))
                            )
                        )
                        .toArray()
                )
            );
        }

        RetrieverBuilder rewritten = retriever.doRewrite(ctx);
        assertNotSame(retriever, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);

        RRFRetrieverBuilder rewrittenRrf = (RRFRetrieverBuilder) rewritten;
        assertEquals(retriever.rankWindowSize(), rewrittenRrf.rankWindowSize());
        assertEquals(retriever.rankConstant(), rewrittenRrf.rankConstant());
        assertEquals(expectedInnerRetrievers, getInnerRetrieversAsSet(rewrittenRrf));
    }

    private static Set<Object> getInnerRetrieversAsSet(RRFRetrieverBuilder retriever) {
        Set<Object> innerRetrieversSet = new HashSet<>();
        for (CompoundRetrieverBuilder.RetrieverSource innerRetriever : retriever.innerRetrievers()) {
            if (innerRetriever.retriever() instanceof RRFRetrieverBuilder innerRrfRetriever) {
                assertEquals(retriever.rankWindowSize(), innerRrfRetriever.rankWindowSize());
                assertEquals(retriever.rankConstant(), innerRrfRetriever.rankConstant());
                innerRetrieversSet.add(getInnerRetrieversAsSet(innerRrfRetriever));
            } else {
                innerRetrieversSet.add(innerRetriever);
            }
        }

        return innerRetrieversSet;
    }
}
