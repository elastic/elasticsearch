/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.AbstractRetrieverBuilderTests;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.rank.linear.ScoreNormalizer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.instanceOf;

/** Tests for the rrf retriever. */
public class RRFRetrieverBuilderTests extends AbstractRetrieverBuilderTests<RRFRetrieverBuilder> {

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
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(Map.of(indexName, testInferenceFields), null, Map.of());
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

        // Basic per-field weights test with inference fields
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^2.0", "field_2^0.5", "semantic_field_1"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 2.0f, "field_2", 0.5f),
            Map.of("semantic_field_1", 1.0f),
            "test query"
        );

        // Inference fields with specific weights
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("semantic_field_1^3.0", "semantic_field_2^0.5"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of(),
            Map.of("semantic_field_1", 3.0f, "semantic_field_2", 0.5f),
            "test query"
        );

        // Zero weights are accepted (additional test beyond the earlier zero_test)
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field^0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(rrfRetrieverBuilder, queryRewriteContext, Map.of("field", 0.0f), Map.of(), "test query");

        // Large weight values are handled correctly
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^1000000", "field_2^1.0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1000000.0f, "field_2", 1.0f),
            Map.of(),
            "test query"
        );

        // Mixed weighted and unweighted fields in simplified syntax
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("title^2.5", "content", "tags^1.5", "description"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("title", 2.5f, "content", 1.0f, "tags", 1.5f, "description", 1.0f),
            Map.of(),
            "test query"
        );

        // Decimal weight precision handling
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field1^0.1", "field2^2.75", "field3^10.999"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field1", 0.1f, "field2", 2.75f, "field3", 10.999f),
            Map.of(),
            "test query"
        );

        // Simplified syntax with glob patterns and weights
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("title_*^2.0", "content_*^1.5", "meta_*"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("title_*", 2.0f, "content_*", 1.5f, "meta_*", 1.0f),
            Map.of(),
            "test query"
        );

        // Extremely large weight values handling
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^1e20", "field_2^1.0"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1e20f, "field_2", 1.0f),
            Map.of(),
            "test query"
        );

        // Lexical field weight propagation (no inference fields)
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^2.0", "field_2^0.5"),
            "test query",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewriteWithWeights(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 2.0f, "field_2", 0.5f),
            Map.of(),
            "test query"
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
            Map.of("local-index", List.of()),
            Map.of("remote-cluster", "remote-index"),
            Map.of()
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

    private void assertMultiFieldsParamsRewrite(
        RRFRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery
    ) {
        assertMultiFieldsParamsRewrite(retriever, ctx, expectedNonInferenceFields, expectedInferenceFields, expectedQuery, null);
    }

    @Override
    protected float[] getWeights(RRFRetrieverBuilder builder) {
        return builder.weights();
    }

    @Override
    protected ScoreNormalizer[] getScoreNormalizers(RRFRetrieverBuilder builder) {
        return null;
    }

    @Override
    protected void assertCompoundRetriever(RRFRetrieverBuilder originalRetriever, RetrieverBuilder rewrittenRetriever) {
        assert (rewrittenRetriever instanceof RRFRetrieverBuilder);
        RRFRetrieverBuilder actualRetrieverBuilder = (RRFRetrieverBuilder) rewrittenRetriever;
        assertEquals(originalRetriever.rankWindowSize(), actualRetrieverBuilder.rankWindowSize());
        assertEquals(originalRetriever.rankConstant(), actualRetrieverBuilder.rankConstant());
    }
}
