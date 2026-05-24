/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
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
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder.RetrieverSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.rank.linear.ScoreNormalizer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractRetrieverBuilderTests<T extends CompoundRetrieverBuilder<T>> extends ESTestCase {

    protected abstract float[] getWeights(T builder);

    protected abstract ScoreNormalizer[] getScoreNormalizers(T builder);

    protected abstract void assertCompoundRetriever(T originalRetriever, RetrieverBuilder rewrittenRetriever);

    protected static ResolvedIndices createMockResolvedIndices(
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

    protected void assertMultiFieldsParamsRewrite(
        T retriever,
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

    @SuppressWarnings("unchecked")
    protected void assertMultiIndexMultiFieldsParamsRewrite(
        T retriever,
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
        assertCompoundRetriever(retriever, rewritten);

        boolean assertedLexical = false;
        boolean assertedSemantic = false;

        for (InnerRetriever topInnerRetriever : getInnerRetrieversAsSet(retriever, (T) rewritten)) {
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

    @SuppressWarnings("unchecked")
    private Set<InnerRetriever> getInnerRetrieversAsSet(T originalRetriever, T rewrittenRetriever) {
        float[] weights = getWeights(rewrittenRetriever);
        ScoreNormalizer[] normalizers = getScoreNormalizers(rewrittenRetriever);

        int i = 0;
        Set<InnerRetriever> innerRetrieversSet = new HashSet<>();
        for (RetrieverSource innerRetriever : rewrittenRetriever.innerRetrievers()) {
            float weight = weights[i];
            ScoreNormalizer normalizer = normalizers != null ? normalizers[i] : null;

            if (innerRetriever.retriever() instanceof CompoundRetrieverBuilder<?> compoundRetriever) {
                assertCompoundRetriever(originalRetriever, compoundRetriever);
                innerRetrieversSet.add(
                    new InnerRetriever(getInnerRetrieversAsSet(originalRetriever, (T) compoundRetriever), weight, normalizer)
                );
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
