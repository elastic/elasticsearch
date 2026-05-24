/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class LearningToRankExplainIT extends BaseMlIntegTestCase {

    private static final String LTR_SEARCH_INDEX = "ltr-search-index";
    private static final String LTR_MODEL = "ltr-model";
    private static final int NUMBER_OF_NODES = 3;
    private static final String DEFAULT_SEARCH_REQUEST_BODY = """
        {
            "query": {
                "match": { "product": { "query": "TV" } }
            },
            "rescore": {
                "window_size": 10,
                "learning_to_rank": {
                    "model_id": "ltr-model",
                    "params": { "keyword": "TV" }
                }
            }
        }""";

    @Before
    public void setupCluster() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(NUMBER_OF_NODES);
        ensureStableCluster();
        createLtrModel();
    }

    public void testLtrExplainWithSingleShard() throws IOException {
        runLtrExplainTest(1, 1, 2, new float[] { 15f, 11f });
    }

    public void testLtrExplainWithMultipleShards() throws IOException {
        runLtrExplainTest(randomIntBetween(2, NUMBER_OF_NODES), 0, 2, new float[] { 15f, 11f });
    }

    public void testLtrExplainWithReplicas() throws IOException {
        runLtrExplainTest(1, randomIntBetween(1, NUMBER_OF_NODES - 1), 2, new float[] { 15f, 11f });
    }

    public void testLtrExplainWithMultipleShardsAndReplicas() throws IOException {
        runLtrExplainTest(randomIntBetween(2, NUMBER_OF_NODES), randomIntBetween(1, NUMBER_OF_NODES - 1), 2, new float[] { 15f, 11f });
    }

    private void runLtrExplainTest(int numberOfShards, int numberOfReplicas, long expectedTotalHits, float[] expectedScores)
        throws IOException {
        createLtrIndex(numberOfShards, numberOfReplicas);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, DEFAULT_SEARCH_REQUEST_BODY)) {
            assertResponse(
                client().prepareSearch(LTR_SEARCH_INDEX)
                    .setSource(new SearchSourceBuilder().parseXContent(parser, true, Predicates.always()))
                    .setExplain(true),
                searchResponse -> {
                    assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(expectedTotalHits));
                    for (int i = 0; i < expectedScores.length; i++) {
                        // Check expected score
                        SearchHit hit = searchResponse.getHits().getHits()[i];
                        assertThat(hit.getScore(), equalTo(expectedScores[i]));

                        // Check explanation is present and contains the right data
                        assertThat(hit.getExplanation(), notNullValue());
                        assertThat(hit.getExplanation().getValue().floatValue(), equalTo(hit.getScore()));
                        assertThat(hit.getExplanation().getDescription(), equalTo("rescored using LTR model ltr-model"));
                    }
                }
            );
        }
    }

    private void createLtrIndex(int numberOfShards, int numberOfReplicas) {
        client().admin()
            .indices()
            .prepareCreate(LTR_SEARCH_INDEX)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                    .build()
            )
            .setMapping("product", "type=keyword", "best_seller", "type=boolean")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(LTR_SEARCH_INDEX);
        indexRequest.source("product", "TV", "best_seller", true);
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(LTR_SEARCH_INDEX);
        indexRequest.source("product", "TV", "best_seller", false);
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(LTR_SEARCH_INDEX);
        indexRequest.source("product", "VCR", "best_seller", true);
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(LTR_SEARCH_INDEX);
        indexRequest.source("product", "VCR", "best_seller", true);
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(LTR_SEARCH_INDEX);
        indexRequest.source("product", "Laptop", "best_seller", true);
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    private void createLtrModel() throws IOException {
        client().execute(
            PutTrainedModelAction.INSTANCE,
            new PutTrainedModelAction.Request(
                TrainedModelConfig.builder()
                    .setModelId(LTR_MODEL)
                    .setInferenceConfig(
                        LearningToRankConfig.builder(LearningToRankConfig.EMPTY_PARAMS)
                            .setLearningToRankFeatureExtractorBuilders(
                                List.of(
                                    new QueryExtractorBuilder(
                                        "best_seller",
                                        QueryProvider.fromParsedQuery(QueryBuilders.termQuery("best_seller", "true"))
                                    ),
                                    new QueryExtractorBuilder(
                                        "product_match",
                                        QueryProvider.fromParsedQuery(QueryBuilders.termQuery("product", "{{keyword}}"))
                                    )
                                )
                            )
                            .build()
                    )
                    .setParsedDefinition(
                        new TrainedModelDefinition.Builder().setPreProcessors(Collections.emptyList())
                            .setTrainedModel(
                                Ensemble.builder()
                                    .setFeatureNames(List.of("best_seller", "product_bm25"))
                                    .setTargetType(TargetType.REGRESSION)
                                    .setTrainedModels(
                                        List.of(
                                            Tree.builder()
                                                .setFeatureNames(List.of("best_seller"))
                                                .setTargetType(TargetType.REGRESSION)
                                                .setRoot(
                                                    TreeNode.builder(0)
                                                        .setSplitFeature(0)
                                                        .setSplitGain(12d)
                                                        .setThreshold(1d)
                                                        .setOperator(Operator.GTE)
                                                        .setDefaultLeft(true)
                                                        .setLeftChild(1)
                                                        .setRightChild(2)
                                                )
                                                .addLeaf(1, 1)
                                                .addLeaf(2, 5)
                                                .build(),
                                            Tree.builder()
                                                .setFeatureNames(List.of("product_match"))
                                                .setTargetType(TargetType.REGRESSION)
                                                .setRoot(
                                                    TreeNode.builder(0)
                                                        .setSplitFeature(0)
                                                        .setSplitGain(12d)
                                                        .setThreshold(1d)
                                                        .setOperator(Operator.LT)
                                                        .setDefaultLeft(true)
                                                        .setLeftChild(1)
                                                        .setRightChild(2)
                                                )
                                                .addLeaf(1, 10)
                                                .addLeaf(2, 1)
                                                .build()
                                        )
                                    )
                                    .build()
                            )
                    )
                    .validate(true)
                    .build(),
                false
            )
        ).actionGet();
    }
}
