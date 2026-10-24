/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

/**
 * Base class for tests that filter a knn query on an inference field. The knn query is built inside the field's chunks nested scope, so
 * these tests check that its filters still apply to the root documents. Searches are issued from a coordinating-only node so that the
 * query crosses the transport layer.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractInferenceFieldKnnFilterIT extends ESIntegTestCase {
    static final int VECTOR_DIMENSIONS = 128;  // Use a dimension count that is compatible with BIT element type

    private static final String DOC_1_TEXT = "the first document";
    private static final String DOC_2_TEXT = "the second document";
    private static final String DOC_3_TEXT = "the third document";

    private String indexName = null;
    private String inferenceId;
    private TaskType taskType;

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
    private void setUpIndex() throws IOException {
        taskType = randomFrom(supportedTaskTypes());
        inferenceId = randomIdentifier();
        IntegrationTestUtils.createInferenceEndpoint(
            client(),
            taskType,
            inferenceId,
            generateDenseServiceSettings(randomFrom(DenseVectorFieldMapper.ElementType.values()))
        );

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("keyword_field").field("type", "keyword").endObject();
        addInferenceFieldsToMapping(mapping, Map.of("inference_field", inferenceId, "filter_inference_field", inferenceId));
        mapping.startObject("nested_field").field("type", "nested");
        {
            mapping.startObject("properties");
            mapping.startObject("tag").field("type", "keyword").endObject();
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject().endObject();

        indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setMapping(mapping));
        ensureGreen(indexName);

        indexDocs(
            Map.of(
                "doc_1",
                Map.of(
                    "keyword_field",
                    "foo",
                    "inference_field",
                    DOC_1_TEXT,
                    "filter_inference_field",
                    "some text",
                    "nested_field",
                    List.of(Map.of("tag", "a"))
                ),
                "doc_2",
                Map.of(
                    "keyword_field",
                    "bar",
                    "inference_field",
                    DOC_2_TEXT,
                    "filter_inference_field",
                    "some text",
                    "nested_field",
                    List.of(Map.of("tag", "b"))
                ),
                "doc_3",
                Map.of("keyword_field", "foo", "inference_field", DOC_3_TEXT)
            )
        );
    }

    @After
    private void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
        if (inferenceId != null) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), taskType, inferenceId);
        }
    }

    abstract Set<TaskType> supportedTaskTypes();

    abstract void addInferenceFieldsToMapping(XContentBuilder mapping, Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    /**
     * Filters on another inference field exclude documents without a value for that field.
     */
    public void testFilterOnOtherInferenceField() throws Exception {
        assertKnnHits(knnQuery("filter_inference_field", "some", 10), "doc_1", "doc_2");
        assertKnnHits(QueryBuilders.matchQuery("filter_inference_field", "some"), "doc_1", "doc_2");
        assertKnnHits(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("filter_inference_field", "some")), "doc_1", "doc_2");
        assertKnnHits(QueryBuilders.existsQuery("filter_inference_field"), "doc_1", "doc_2");
    }

    /**
     * Root field filters still apply when combined with a filter on another inference field.
     */
    public void testSemanticFilterCombinedWithRootFieldFilter() throws Exception {
        assertKnnHits(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("keyword_field", "foo"))
                .filter(QueryBuilders.matchQuery("filter_inference_field", "some")),
            "doc_1"
        );
    }

    /**
     * Nested query filters on a nested field other than the chunks field.
     */
    public void testNestedQueryFilter() throws Exception {
        assertKnnHits(QueryBuilders.nestedQuery("nested_field", QueryBuilders.termQuery("nested_field.tag", "a"), ScoreMode.None), "doc_1");
    }

    /**
     * Filtered alias with a nested query on a nested field other than the chunks field.
     */
    public void testNestedQueryFilteredAlias() throws Exception {
        String aliasName = randomIdentifier();
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(
                    indexName,
                    aliasName,
                    QueryBuilders.nestedQuery("nested_field", QueryBuilders.termQuery("nested_field.tag", "a"), ScoreMode.None)
                )
        );
        assertKnnHits(aliasName, knnQuery("inference_field", "document", 10), "doc_1");
    }

    private void indexDocs(Map<String, Map<String, Object>> docs) {
        BulkRequestBuilder bulkRequest = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        docs.forEach((id, source) -> bulkRequest.add(prepareIndex(indexName).setId(id).setSource(source)));
        assertNoFailures(bulkRequest.get(TEST_REQUEST_TIMEOUT));
    }

    private void assertKnnHits(QueryBuilder filter, String... expectedIds) throws Exception {
        assertKnnHits(indexName, knnQuery("inference_field", "document", 10).addFilterQuery(filter), expectedIds);
    }

    private void assertKnnHits(String target, QueryBuilder query, String... expectedIds) throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);

        // Use the coordinating-only node so that the query is serialized over the wire (coordinating node -> data node)
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { target }, source)),
            response -> assertSearchHits(response, expectedIds)
        );
    }

    private KnnVectorQueryBuilder knnQuery(String field, String text, int k) {
        return new KnnVectorQueryBuilder(field, switch (taskType) {
            case EMBEDDING -> new EmbeddingQueryVectorBuilder(inferenceId, new InferenceStringGroup(text), null);
            case TEXT_EMBEDDING -> new TextEmbeddingQueryVectorBuilder(null, text);
            default -> throw new AssertionError("Unhandled task type [" + taskType + "]");
        }, k, k * 10, null, null);
    }

    private static Map<String, Object> generateDenseServiceSettings(DenseVectorFieldMapper.ElementType elementType) {
        List<SimilarityMeasure> supportedSimilarities = new ArrayList<>(
            DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType)
        );
        // Dot product requires unit vectors, which the mock inference services do not produce
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
