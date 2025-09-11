/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.IVF_FORMAT;
import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class LinearRetrieverBuilderTests extends ESTestCase {
    public void testMultiFieldsParamsRewrite() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(Map.of(indexName, testInferenceFields), null, Map.of());
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
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
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
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
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
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
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
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

    private static ResolvedIndices createMockResolvedIndices(
        Map<String, List<String>> localIndexInferenceFields,
        Map<String, String> remoteIndexNames,
        Map<String, String> commonInferenceIds
    ) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();

        for (var indexEntry : localIndexInferenceFields.entrySet()) {
            String indexName = indexEntry.getKey();
            List<String> inferenceFields = indexEntry.getValue();

            Index index = new Index(indexName, randomAlphaOfLength(10));

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0);

            for (String inferenceField : inferenceFields) {
                String inferenceId = commonInferenceIds.containsKey(inferenceField)
                    ? commonInferenceIds.get(inferenceField)
                    : randomAlphaOfLengthBetween(3, 5);

                indexMetadataBuilder.putInferenceField(
                    new InferenceFieldMetadata(inferenceField, inferenceId, new String[] { inferenceField }, null)
                );
            }

            indexMetadata.put(index, indexMetadataBuilder.build());
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        if (remoteIndexNames != null) {
            for (Map.Entry<String, String> entry : remoteIndexNames.entrySet()) {
                remoteIndices.put(entry.getKey(), new OriginalIndices(new String[] { entry.getValue() }, IndicesOptions.DEFAULT));
            }
        }

        return new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(localIndexInferenceFields.keySet().toArray(new String[0]), IndicesOptions.DEFAULT),
            indexMetadata
        );
    }

    private static void assertMultiFieldsParamsRewrite(
        LinearRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery,
        ScoreNormalizer expectedNormalizer
    ) {
        Map<Tuple<String, List<String>>, Float> inferenceFields = new HashMap<>();
        expectedInferenceFields.forEach((key, value) -> inferenceFields.put(new Tuple<>(key, List.of()), value));

        assertMultiIndexMultiFieldsParamsRewrite(
            retriever,
            ctx,
            Map.of(expectedNonInferenceFields, List.of()),
            inferenceFields,
            expectedQuery,
            expectedNormalizer
        );
    }

    private static void assertMultiIndexMultiFieldsParamsRewrite(
        LinearRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<Map<String, Float>, List<String>> expectedNonInferenceFields,
        Map<Tuple<String, List<String>>, Float> expectedInferenceFields,
        String expectedQuery,
        ScoreNormalizer expectedNormalizer
    ) {
        Set<QueryBuilder> expectedLexicalQueryBuilders = expectedNonInferenceFields.entrySet().stream().map(entry -> {
            Map<String, Float> fields = entry.getKey();
            List<String> indices = entry.getValue();

            QueryBuilder queryBuilder = new MultiMatchQueryBuilder(expectedQuery).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                .fields(fields);

            if (indices.isEmpty() == false) {
                queryBuilder = new BoolQueryBuilder().must(queryBuilder).filter(new TermsQueryBuilder("_index", indices));
            }
            return queryBuilder;
        }).collect(Collectors.toSet());

        Set<InnerRetriever> expectedInnerSemanticRetrievers = expectedInferenceFields.entrySet().stream().map(entry -> {
            var groupedInferenceField = entry.getKey();
            var fieldName = groupedInferenceField.v1();
            var indices = groupedInferenceField.v2();
            var weight = entry.getValue();
            QueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, expectedQuery);
            if (indices.isEmpty() == false) {
                queryBuilder = new BoolQueryBuilder().must(queryBuilder).filter(new TermsQueryBuilder("_index", indices));
            }
            return new InnerRetriever(new StandardRetrieverBuilder(queryBuilder), weight, expectedNormalizer);
        }).collect(Collectors.toSet());

        RetrieverBuilder rewritten = retriever.doRewrite(ctx);
        assertNotSame(retriever, rewritten);
        assertTrue(rewritten instanceof LinearRetrieverBuilder);
        LinearRetrieverBuilder rewrittenLinear = (LinearRetrieverBuilder) rewritten;
        assertEquals(retriever.rankWindowSize(), rewrittenLinear.rankWindowSize());

        boolean assertedLexical = false;
        boolean assertedSemantic = false;

        for (InnerRetriever topInnerRetriever : getInnerRetrieversAsSet(rewrittenLinear)) {
            assertEquals(expectedNormalizer, topInnerRetriever.normalizer);
            assertEquals(1.0f, topInnerRetriever.weight, 0.0f);

            if (topInnerRetriever.retriever instanceof StandardRetrieverBuilder standardRetrieverBuilder) {
                assertFalse("the lexical retriever is only asserted once", assertedLexical);
                assertFalse(expectedNonInferenceFields.isEmpty());

                QueryBuilder topDocsQueryBuilder = standardRetrieverBuilder.topDocsQuery();
                if (expectedLexicalQueryBuilders.size() == 1) {
                    assertEquals(topDocsQueryBuilder, expectedLexicalQueryBuilders.iterator().next());
                } else {
                    assertTrue(topDocsQueryBuilder instanceof BoolQueryBuilder);
                    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) topDocsQueryBuilder;
                    assertEquals(new HashSet<>(expectedLexicalQueryBuilders), new HashSet<>(boolQueryBuilder.should()));
                }
                assertedLexical = true;
            } else {
                assertFalse("the semantic retriever is only asserted once", assertedSemantic);
                assertFalse(expectedInferenceFields.isEmpty());
                assertEquals(expectedInnerSemanticRetrievers, topInnerRetriever.retriever);
                assertedSemantic = true;
            }
        }
    }

    private static Set<InnerRetriever> getInnerRetrieversAsSet(LinearRetrieverBuilder retriever) {
        float[] weights = retriever.getWeights();
        ScoreNormalizer[] normalizers = retriever.getNormalizers();

        int i = 0;
        Set<InnerRetriever> innerRetrieversSet = new HashSet<>();
        for (CompoundRetrieverBuilder.RetrieverSource innerRetriever : retriever.innerRetrievers()) {
            float weight = weights[i];
            ScoreNormalizer normalizer = normalizers[i];

            if (innerRetriever.retriever() instanceof LinearRetrieverBuilder innerLinearRetriever) {
                assertEquals(retriever.rankWindowSize(), innerLinearRetriever.rankWindowSize());
                innerRetrieversSet.add(new InnerRetriever(getInnerRetrieversAsSet(innerLinearRetriever), weight, normalizer));
            } else {
                innerRetrieversSet.add(new InnerRetriever(innerRetriever.retriever(), weight, normalizer));
            }

            i++;
        }

        return innerRetrieversSet;
    }

    private static class InnerRetriever {
        private final Object retriever;
        private final float weight;
        private final ScoreNormalizer normalizer;

        InnerRetriever(RetrieverBuilder retriever, float weight, ScoreNormalizer normalizer) {
            this.retriever = retriever;
            this.weight = weight;
            this.normalizer = normalizer;
        }

        InnerRetriever(Set<InnerRetriever> innerRetrievers, float weight, ScoreNormalizer normalizer) {
            this.retriever = innerRetrievers;
            this.weight = weight;
            this.normalizer = normalizer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InnerRetriever that = (InnerRetriever) o;
            return Float.compare(weight, that.weight) == 0
                && Objects.equals(retriever, that.retriever)
                && Objects.equals(normalizer, that.normalizer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(retriever, weight, normalizer);
        }
    }

    public void testTopLevelNormalizerWithRetrieversArray() {
        StandardRetrieverBuilder standardRetriever = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "elasticsearch"));
        KnnRetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
            "title_vector",
            new float[] { 0.1f, 0.2f, 0.3f },
            null,
            10,
            100,
            IVF_FORMAT.isEnabled() ? 10f : null,
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
            IVF_FORMAT.isEnabled() ? 10f : null,
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

}
