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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class LinearRetrieverBuilderTests extends ESTestCase {
    public void testSimplifiedParamsRewrite() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields);
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
        assertSimplifiedParamsRewrite(
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
        assertSimplifiedParamsRewrite(
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
        assertSimplifiedParamsRewrite(
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
        assertSimplifiedParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("field_*", 1.5f, "*_field_1", 2.5f),
            Map.of("semantic_field_1", 2.5f),
            "baz",
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
        assertSimplifiedParamsRewrite(
            retriever,
            queryRewriteContext,
            Map.of("*", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "qux",
            MinMaxScoreNormalizer.INSTANCE
        );
    }

    private static ResolvedIndices createMockResolvedIndices(String indexName, List<String> inferenceFields) {
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
            indexMetadataBuilder.putInferenceField(
                new InferenceFieldMetadata(inferenceField, randomAlphaOfLengthBetween(3, 5), new String[] { inferenceField }, null)
            );
        }

        return new MockResolvedIndices(
            Map.of(),
            new OriginalIndices(new String[] { indexName }, IndicesOptions.DEFAULT),
            Map.of(index, indexMetadataBuilder.build())
        );
    }

    private static void assertSimplifiedParamsRewrite(
        LinearRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery,
        ScoreNormalizer expectedNormalizer
    ) {
        Set<InnerRetriever> expectedInnerRetrievers = Set.of(
            new InnerRetriever(
                new StandardRetrieverBuilder(
                    new MultiMatchQueryBuilder(expectedQuery).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                        .fields(expectedNonInferenceFields)
                ),
                1.0f,
                expectedNormalizer
            ),
            new InnerRetriever(
                expectedInferenceFields.entrySet()
                    .stream()
                    .map(
                        e -> new InnerRetriever(
                            new StandardRetrieverBuilder(new MatchQueryBuilder(e.getKey(), expectedQuery)),
                            e.getValue(),
                            expectedNormalizer
                        )
                    )
                    .collect(Collectors.toSet()),
                1.0f,
                expectedNormalizer
            )
        );

        RetrieverBuilder rewritten = retriever.doRewrite(ctx);
        assertNotSame(retriever, rewritten);
        assertTrue(rewritten instanceof LinearRetrieverBuilder);

        LinearRetrieverBuilder rewrittenLinear = (LinearRetrieverBuilder) rewritten;
        assertEquals(retriever.rankWindowSize(), rewrittenLinear.rankWindowSize());
        assertEquals(expectedInnerRetrievers, getInnerRetrieversAsSet(rewrittenLinear));
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
}
