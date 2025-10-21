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
import org.elasticsearch.core.Tuple;
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
                            null,
                            null,
                            null,
                            new PointInTimeBuilder(new BytesArray("pitid")),
                            null,
                            null
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
                            null,
                            null,
                            null,
                            new PointInTimeBuilder(new BytesArray("pitid")),
                            null,
                            null
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
            null
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

        // Non-default rank window size and rank constant
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            DEFAULT_RANK_WINDOW_SIZE * 2,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT / 2,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo2"
        );

        // No wildcards, per-field boosting
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.5f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 2.0f),
            "bar"
        );

        // Zero weights
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1^0", "field_2^1.0"),
            "zero_test",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 0.0f, "field_2", 1.0f),
            Map.of(),
            "zero_test"
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
        assertMultiFieldsParamsRewrite(
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
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "field_1", 3.0f, "*_field_1", 2.5f, "semantic_*", 1.5f),
            Map.of("semantic_field_1", 3.75f, "semantic_field_2", 1.5f),
            "baz2"
        );

        // All-fields wildcard
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("*", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "qux"
        );
    }

    public void testMultiIndexMultiFieldsParamsRewrite() {
        String indexName = "test-index";
        String anotherIndexName = "test-another-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(
            Map.of(
                indexName,
                List.of("semantic_field_1", "semantic_field_2"),
                anotherIndexName,
                List.of("semantic_field_2", "semantic_field_3")
            ),
            null,
            Map.of() // use random and different inference IDs for semantic_text fields
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
            null
        );

        // No wildcards, no per-field boosting
        RRFRetrieverBuilder retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.0f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.0f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(indexName)), // field with different inference IDs, we filter on index name
                1.0f,
                new Tuple<>("semantic_field_2", List.of(anotherIndexName)),
                1.0f
            ),
            "foo",
            null
        );

        // Non-default rank window size and non-default rank_constant
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            DEFAULT_RANK_WINDOW_SIZE * 2,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT * 2,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.0f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.0f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(anotherIndexName)),
                1.0f
            ),
            "foo2",
            null
        );

        // No wildcards, per-field boosting
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.5f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.5f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(indexName)),
                2.0f,
                new Tuple<>("semantic_field_2", List.of(anotherIndexName)),
                2.0f
            ),
            "bar",
            null
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "*_field_1", 2.5f), List.of()),
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 2.5f),
            "baz",
            null
        );

        // Multiple boosts defined on the same field
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "field_1", 3.0f, "*_field_1", 2.5f, "semantic_*", 1.5f), List.of()),
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                3.75f,
                new Tuple<>("semantic_field_2", List.of(indexName)),
                1.5f,
                new Tuple<>("semantic_field_2", List.of(anotherIndexName)),
                1.5f,
                new Tuple<>("semantic_field_3", List.of(anotherIndexName)),
                1.5f
            ),
            "baz2",
            null
        );

        // All-fields wildcard
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("*", 1.0f), List.of()), // no index filter for the lexical retriever
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of(anotherIndexName)),
                1.0f,
                new Tuple<>("semantic_field_3", List.of(anotherIndexName)),
                1.0f
            ),
            "qux",
            null
        );
    }

    public void testMultiIndexMultiFieldsParamsRewriteWithSameInferenceIds() {
        String indexName = "test-index";
        String anotherIndexName = "test-another-index";
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(
            Map.of(
                indexName,
                List.of("semantic_field_1", "semantic_field_2"),
                anotherIndexName,
                List.of("semantic_field_2", "semantic_field_3")
            ),
            null,
            Map.of("semantic_field_2", "common_inference_id") // use the same inference ID for semantic_field_2
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
            null
        );

        // No wildcards, no per-field boosting
        RRFRetrieverBuilder retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.0f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.0f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 1.0f, new Tuple<>("semantic_field_2", List.of()), 1.0f),
            "foo",
            null
        );

        // Non-default rank window size and rank constant
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            DEFAULT_RANK_WINDOW_SIZE * 2,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT * 2,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.0f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.0f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 1.0f, new Tuple<>("semantic_field_2", List.of()), 1.0f),
            "foo2",
            null
        );

        // No wildcards, per-field boosting
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(
                Map.of("field_1", 1.0f, "field_2", 1.5f),
                List.of(indexName),
                Map.of("field_1", 1.0f, "field_2", 1.5f, "semantic_field_1", 1.0f),
                List.of(anotherIndexName)
            ),
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 1.0f, new Tuple<>("semantic_field_2", List.of()), 2.0f),
            "bar",
            null
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "*_field_1", 2.5f), List.of()), // on index filter on the lexical query
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 2.5f),
            "baz",
            null
        );

        // Multiple boosts defined on the same field
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "field_1", 3.0f, "*_field_1", 2.5f, "semantic_*", 1.5f), List.of()),
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                3.75f,
                new Tuple<>("semantic_field_2", List.of()), // no index filter since both indices have this field
                1.5f,
                new Tuple<>("semantic_field_3", List.of(anotherIndexName)),
                1.5f
            ),
            "baz2",
            null
        );

        // All-fields wildcard
        retriever = new RRFRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("*", 1.0f), List.of()), // on index filter on the lexical query
            Map.of(
                new Tuple<>("semantic_field_1", List.of(indexName)),
                1.0f,
                new Tuple<>("semantic_field_2", List.of()), // no index filter since both indices have this field
                1.0f,
                new Tuple<>("semantic_field_3", List.of(anotherIndexName)),
                1.0f
            ),
            "qux",
            null
        );
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
            null
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
