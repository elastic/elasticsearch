/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.retriever.AbstractRetrieverBuilderTests;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class LinearRetrieverBuilderTests extends AbstractRetrieverBuilderTests<LinearRetrieverBuilder> {
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
        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Non-default rank window size
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE * 2,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo2",
            MinMaxScoreNormalizer.INSTANCE
        );

        // No wildcards, per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.5f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 2.0f),
            "bar",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Zero weights
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1^0", "field_2^1.0"),
            "zero_test",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_1", 0.0f, "field_2", 1.0f),
            Map.of(),
            "zero_test",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "*_field_1", 2.5f),
            Map.of("semantic_field_1", 2.5f),
            "baz",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Multiple boosts defined on the same field
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "field_1", 3.0f, "*_field_1", 2.5f, "semantic_*", 1.5f),
            Map.of("semantic_field_1", 3.75f, "semantic_field_2", 1.5f),
            "baz2",
            MinMaxScoreNormalizer.INSTANCE
        );

        // All-fields wildcard
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("*", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "qux",
            MinMaxScoreNormalizer.INSTANCE
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
        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // Non-default rank window size
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE * 2,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // No wildcards, per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "*_field_1", 2.5f), List.of()),
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 2.5f),
            "baz",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Multiple boosts defined on the same field
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // All-fields wildcard
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
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
        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // Non-default rank window size
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE * 2,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // No wildcards, per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_1", "field_2^1.5", "semantic_field_1", "semantic_field_2^2"),
            "bar",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // Glob matching on inference and non-inference fields with per-field boosting
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "*_field_1^2.5"),
            "baz",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of(Map.of("field_*", 1.5f, "*_field_1", 2.5f), List.of()), // on index filter on the lexical query
            Map.of(new Tuple<>("semantic_field_1", List.of(indexName)), 2.5f),
            "baz",
            MinMaxScoreNormalizer.INSTANCE
        );

        // Multiple boosts defined on the same field
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("field_*^1.5", "field_1^3.0", "*_field_1^2.5", "semantic_*^1.5"),
            "baz2",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
        );

        // All-fields wildcard
        retriever = new LinearRetrieverBuilder(
            null,
            List.of("*"),
            "qux",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
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
            MinMaxScoreNormalizer.INSTANCE
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

        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            null,
            null,
            "foo",
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[0],
            new ScoreNormalizer[0]
        );

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> retriever.doRewrite(queryRewriteContext));
        assertEquals("[linear] cannot specify [query] when querying remote indices", iae.getMessage());
    }

    public void testTopLevelNormalizerWithRetrieversArray() {
        StandardRetrieverBuilder standardRetriever = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "elasticsearch"));
        KnnRetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
            "title_vector",
            new float[] { 0.1f, 0.2f, 0.3f },
            null,
            10,
            100,
            10f,
            null,
            null
        );

        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            List.of(
                CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever),
                CompoundRetrieverBuilder.RetrieverSource.from(knnRetriever)
            ),
            null,
            null,
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[] { 1.0f, 2.0f },
            new ScoreNormalizer[] { null, null }
        );

        assertEquals(MinMaxScoreNormalizer.INSTANCE, retriever.getNormalizers()[0]);
        assertEquals(MinMaxScoreNormalizer.INSTANCE, retriever.getNormalizers()[1]);
    }

    public void testTopLevelNormalizerWithPerRetrieverOverrides() {
        StandardRetrieverBuilder standardRetriever = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "elasticsearch"));
        KnnRetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
            "title_vector",
            new float[] { 0.1f, 0.2f, 0.3f },
            null,
            10,
            100,
            10f,
            null,
            null
        );

        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            List.of(
                CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever),
                CompoundRetrieverBuilder.RetrieverSource.from(knnRetriever)
            ),
            null,
            null,
            MinMaxScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[] { 1.0f, 2.0f },
            new ScoreNormalizer[] { L2ScoreNormalizer.INSTANCE, null }
        );

        assertEquals(L2ScoreNormalizer.INSTANCE, retriever.getNormalizers()[0]);
        assertEquals(MinMaxScoreNormalizer.INSTANCE, retriever.getNormalizers()[1]);
    }

    public void testNullNormalizersWithoutTopLevelUsesIdentity() {
        StandardRetrieverBuilder standardRetriever = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "elasticsearch"));

        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            List.of(CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever)),
            null,
            null,
            null,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[] { 1.0f },
            new ScoreNormalizer[] { null }
        );

        assertEquals(IdentityScoreNormalizer.INSTANCE, retriever.getNormalizers()[0]);
    }

    public void testMixedNormalizerInheritanceScenario() {
        StandardRetrieverBuilder standardRetriever1 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "elasticsearch"));
        StandardRetrieverBuilder standardRetriever2 = new StandardRetrieverBuilder(new MatchQueryBuilder("content", "search"));
        StandardRetrieverBuilder standardRetriever3 = new StandardRetrieverBuilder(new MatchQueryBuilder("tags", "java"));

        LinearRetrieverBuilder retriever = new LinearRetrieverBuilder(
            List.of(
                CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever1),
                CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever2),
                CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever3)
            ),
            null,
            null,
            L2ScoreNormalizer.INSTANCE,
            DEFAULT_RANK_WINDOW_SIZE,
            new float[] { 1.0f, 2.0f, 3.0f },
            new ScoreNormalizer[] { null, MinMaxScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE }
        );
        assertEquals(L2ScoreNormalizer.INSTANCE, retriever.getNormalizers()[0]);
        assertEquals(MinMaxScoreNormalizer.INSTANCE, retriever.getNormalizers()[1]);
        assertEquals(IdentityScoreNormalizer.INSTANCE, retriever.getNormalizers()[2]);
    }

    @Override
    protected float[] getWeights(LinearRetrieverBuilder builder) {
        return builder.getWeights();
    }

    @Override
    protected ScoreNormalizer[] getScoreNormalizers(LinearRetrieverBuilder builder) {
        return builder.getNormalizers();
    }

    @Override
    protected void assertCompoundRetriever(LinearRetrieverBuilder originalRetriever, RetrieverBuilder rewrittenRetriever) {
        assertTrue(rewrittenRetriever instanceof LinearRetrieverBuilder);
        LinearRetrieverBuilder actualRetrieverBuilder = (LinearRetrieverBuilder) rewrittenRetriever;
        assertEquals(originalRetriever.rankWindowSize(), actualRetrieverBuilder.rankWindowSize());
    }
}
