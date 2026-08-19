/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.diversification.DiversifyRetrieverBuilder;
import org.elasticsearch.search.diversification.ResultDiversificationType;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractInferenceFieldDiversifyRetrieverIT extends ESIntegTestCase {
    static final int VECTOR_DIMENSIONS = 128;  // Use a dimension count that is compatible with BIT element type

    String indexName = null;
    final Map<String, TaskType> inferenceIds = new HashMap<>();
    final List<InferenceFieldConfig> inferenceFields = new ArrayList<>();

    /**
     * An inference field to test diversification on, and metadata about the inference endpoint that generates its embeddings.
     */
    record InferenceFieldConfig(String fieldName, String inferenceId, TaskType taskType, DenseVectorFieldMapper.ElementType elementType) {}

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Before
    private void createInferenceEndpoints() throws IOException {
        for (TaskType taskType : supportedTaskTypes()) {
            for (DenseVectorFieldMapper.ElementType elementType : DenseVectorFieldMapper.ElementType.values()) {
                String inferenceId = randomIdentifier();
                createInferenceEndpoint(taskType, elementType, inferenceId);
                inferenceFields.add(new InferenceFieldConfig(randomIdentifier(), inferenceId, taskType, elementType));
            }
        }
    }

    @After
    private void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }

        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
        inferenceIds.clear();
        inferenceFields.clear();
    }

    abstract Set<TaskType> supportedTaskTypes();

    abstract XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    abstract Object generateFieldValue();

    public void testDiversifyOnInferenceField() throws Exception {
        indexName = randomIndexName();
        final int docCount = randomIntBetween(10, 20);
        final Map<String, String> fieldNameToInferenceIdMap = inferenceFields.stream()
            .collect(Collectors.toMap(InferenceFieldConfig::fieldName, InferenceFieldConfig::inferenceId));
        assertAcked(prepareCreate(indexName).setMapping(generateMapping(fieldNameToInferenceIdMap)));

        // Index each document with multiple values per inference field so that each document has multiple chunk embeddings, which
        // exercises the diversify retriever's best embedding code path.
        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        for (int i = 0; i < docCount; i++) {
            Map<String, Object> source = new HashMap<>();
            List<Object> fieldValue = List.of(generateFieldValue(), generateFieldValue());
            for (String fieldName : fieldNameToInferenceIdMap.keySet()) {
                source.put(fieldName, fieldValue);
            }
            bulk.add(client().prepareIndex(indexName).setSource(source));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertNoFailures(bulk.get(TEST_REQUEST_TIMEOUT));
        ensureGreen(indexName);

        for (InferenceFieldConfig inferenceField : inferenceFields) {
            int embeddingLength = DenseVectorFieldMapperTestUtils.getEmbeddingLength(inferenceField.elementType(), VECTOR_DIMENSIONS);
            for (DenseVectorFieldMapper.ElementType elementType : DenseVectorFieldMapper.ElementType.values()) {
                // Query vectors using any element type should work, provided that they have the same embedding length as the field's
                // embeddings. Bit vectors pack 8 dimensions into each byte, so they can only be compared against other bit vectors.
                if (DenseVectorFieldMapperTestUtils.getEmbeddingLength(elementType, VECTOR_DIMENSIONS) != embeddingLength) {
                    continue;
                }

                VectorData queryVector = generateRandomQueryVector(elementType);
                assertDiversify(inferenceField, queryVector, docCount);
            }
        }
    }

    private void assertDiversify(InferenceFieldConfig inferenceField, VectorData queryVector, int docCount) throws Exception {
        final int diversifyCount = randomIntBetween(1, docCount);
        DiversifyRetrieverBuilder retriever = new DiversifyRetrieverBuilder(
            CompoundRetrieverBuilder.RetrieverSource.from(new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            ResultDiversificationType.MMR,
            inferenceField.fieldName(),
            docCount,
            diversifyCount,
            queryVector,
            null,
            randomFloatBetween(0.0f, 1.0f, true)
        );

        SearchSourceBuilder source = new SearchSourceBuilder().retriever(retriever).size(docCount);
        SearchRequest request = new SearchRequest(new String[] { indexName }, source);

        // Issue the search from the coordinating-only node: it holds no shards, so every hit must be serialized from a data node across
        // the transport layer.
        String message = Strings.format(
            "Diversifying on field [%s] with task type [%s] and element type [%s]",
            inferenceField.fieldName(),
            inferenceField.taskType(),
            inferenceField.elementType()
        );
        assertNoFailuresAndResponse(internalCluster().coordOnlyNodeClient().search(request), response -> {
            // Diversification trims the returned hits, but the inner retriever's total hit count is preserved
            assertThat(message, response.getHits().getTotalHits().value(), equalTo((long) docCount));

            SearchHit[] hits = response.getHits().getHits();
            assertThat(message, hits.length, equalTo(diversifyCount));
        });
    }

    void createInferenceEndpoint(TaskType taskType, DenseVectorFieldMapper.ElementType elementType, String inferenceId) throws IOException {
        assertThat(taskType, either(equalTo(TaskType.TEXT_EMBEDDING)).or(equalTo(TaskType.EMBEDDING)));
        Map<String, Object> serviceSettings = generateServiceSettings(elementType);
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    static VectorData generateRandomQueryVector(DenseVectorFieldMapper.ElementType elementType) {
        int embeddingLength = DenseVectorFieldMapperTestUtils.getEmbeddingLength(elementType, VECTOR_DIMENSIONS);
        return switch (elementType) {
            case FLOAT, BFLOAT16 -> VectorData.fromFloats(VectorTestUtils.randomFloatVector(embeddingLength));
            // Byte and bit vectors are both represented as byte arrays, where a bit vector packs 8 dimensions into each byte
            case BYTE, BIT -> VectorData.fromBytes(VectorTestUtils.randomByteVector(embeddingLength));
        };
    }

    private static Map<String, Object> generateServiceSettings(DenseVectorFieldMapper.ElementType elementType) {
        List<SimilarityMeasure> supportedSimilarities = new ArrayList<>(
            DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType)
        );
        // Dot product requires unit vectors. We generate random vectors in this test suite, which may or may not be unit vectors.
        supportedSimilarities.remove(SimilarityMeasure.DOT_PRODUCT);

        return Map.of(
            "model",
            "my_model",
            "dimensions",
            VECTOR_DIMENSIONS,
            "similarity",
            randomFrom(supportedSimilarities),
            "element_type",
            elementType,
            "api_key",
            "my_api_key"
        );
    }
}
