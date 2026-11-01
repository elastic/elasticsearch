/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Profiles a search with a {@code learning_to_rank} rescorer. The feature queries of the model must be profiled once each next to the
 * main query, tagged with the rescorer, and the breakdown of the rescorer must split its time between building the feature extractors,
 * extracting the features of each document, and inference.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class LearningToRankProfileIT extends BaseMlIntegTestCase {

    private static final String INDEX = "ltr-profile-index";
    private static final String MODEL_ID = "ltr-profile-model";
    private static final Map<String, Object> RESCORER_0 = Map.of("rescorer", 0);

    @Before
    public void setupCluster() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureStableCluster();
        createModel();
        createIndex();
    }

    public void testProfile() throws IOException {
        SearchSourceBuilder source;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
                "profile": true,
                "query": { "match": { "product": "TV" } },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": { "model_id": "ltr-profile-model" }
                }
            }""")) {
            source = new SearchSourceBuilder().parseXContent(parser, true, Predicates.always());
        }
        assertResponse(client().prepareSearch(INDEX).setSource(source), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
            Map<String, SearchProfileShardResult> shards = response.getSearchProfileShardResults();
            assertThat(shards.size(), equalTo(1));
            SearchProfileQueryPhaseResult queryPhase = shards.values().iterator().next().getQueryPhase();
            List<ProfileResult> queries = queryPhase.getQueryProfileResults().get(0).getQueryResults();
            // the main query, then each feature query exactly once, tagged with the position of the rescorer
            assertThat(
                queries.stream().map(ProfileResult::getLuceneDescription).toList(),
                equalTo(List.of("product:TV", "best_seller:T", "product:TV"))
            );
            assertThat(queries.stream().map(ProfileResult::getDebugInfo).toList(), equalTo(List.of(Map.of(), RESCORER_0, RESCORER_0)));

            List<ProfileResult> rescorers = queryPhase.getRescoreProfileResults();
            assertThat(rescorers, hasSize(1));
            ProfileResult rescorer = rescorers.get(0);
            assertThat(rescorer.getQueryName(), equalTo("learning_to_rank"));
            assertThat(rescorer.getLuceneDescription(), equalTo("LTR model [" + MODEL_ID + "]"));
            assertThat(rescorer.getProfiledChildren(), empty());
            assertThat(rescorer.getDebugInfo(), equalTo(Map.of("window_size", 10)));

            Map<String, Long> breakdown = rescorer.getTimeBreakdown();
            assertThat(
                breakdown.keySet(),
                equalTo(
                    Set.of(
                        "build_feature_extractors",
                        "build_feature_extractors_count",
                        "feature_extraction",
                        "feature_extraction_count",
                        "inference",
                        "inference_count"
                    )
                )
            );
            assertThat(breakdown.get("build_feature_extractors_count"), equalTo(1L));
            // both matching documents are rescored
            assertThat(breakdown.get("feature_extraction_count"), equalTo(2L));
            assertThat(breakdown.get("inference_count"), equalTo(2L));
            long steps = 0;
            for (String step : List.of("build_feature_extractors", "feature_extraction", "inference")) {
                assertThat(breakdown.get(step), greaterThan(0L));
                steps += breakdown.get(step);
            }
            assertThat(steps, lessThanOrEqualTo(rescorer.getTime()));
        });
    }

    private void createIndex() {
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .setMapping("product", "type=keyword", "best_seller", "type=boolean")
            .get();
        BulkResponse bulkResponse = client().prepareBulk()
            .add(prepareIndex(INDEX).setSource("product", "TV", "best_seller", true))
            .add(prepareIndex(INDEX).setSource("product", "TV", "best_seller", false))
            .add(prepareIndex(INDEX).setSource("product", "VCR", "best_seller", true))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    private void createModel() throws IOException {
        client().execute(
            PutTrainedModelAction.INSTANCE,
            new PutTrainedModelAction.Request(
                TrainedModelConfig.builder()
                    .setModelId(MODEL_ID)
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
                                        QueryProvider.fromParsedQuery(QueryBuilders.termQuery("product", "TV"))
                                    )
                                )
                            )
                            .build()
                    )
                    .setParsedDefinition(
                        new TrainedModelDefinition.Builder().setPreProcessors(List.of())
                            .setTrainedModel(
                                Ensemble.builder()
                                    .setFeatureNames(List.of("best_seller", "product_match"))
                                    .setTargetType(TargetType.REGRESSION)
                                    .setTrainedModels(List.of(tree("best_seller", 1, 5), tree("product_match", 10, 1)))
                                    .build()
                            )
                    )
                    .validate(true)
                    .build(),
                false
            )
        ).actionGet();
    }

    /**
     * A tree that returns {@code below} when the feature is lower than 1, and {@code above} otherwise.
     */
    private static Tree tree(String featureName, double below, double above) {
        return Tree.builder()
            .setFeatureNames(List.of(featureName))
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
            .addLeaf(1, below)
            .addLeaf(2, above)
            .build();
    }
}
