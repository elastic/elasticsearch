/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.job.retention.UnusedStatsRemover;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

public class UnusedStatsRemoverIT extends BaseMlIntegTestCase {

    private OriginSettingClient client;

    @Before
    public void createComponents() {
        client = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        MlStatsIndex.createStatsIndexAndAliasIfNecessary(
            client(),
            clusterService().state(),
            TestIndexNameExpressionResolver.newInstance(client().threadPool().getThreadContext()),
            TEST_REQUEST_TIMEOUT,
            future
        );
        future.actionGet();
    }

    public void testRemoveUnusedStats() throws Exception {
        String modelId = "model-with-stats";
        putDFA(modelId);

        // Existing analytics and models
        indexStatDocument(new DataCounts("analytics-with-stats", 1, 1, 1), DataCounts.documentId("analytics-with-stats"));
        indexStatDocument(new InferenceStats(1, 1, 1, 1, modelId, "test", Instant.now()), InferenceStats.docId(modelId, "test"));
        indexStatDocument(
            new InferenceStats(1, 1, 1, 1, TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(), "test", Instant.now()),
            InferenceStats.docId(TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(), "test")
        );

        // Unused analytics/model stats
        indexStatDocument(new DataCounts("missing-analytics-with-stats", 1, 1, 1), DataCounts.documentId("missing-analytics-with-stats"));
        indexStatDocument(
            new InferenceStats(1, 1, 1, 1, "missing-model", "test", Instant.now()),
            InferenceStats.docId("missing-model", "test")
        );

        refreshStatsIndex();
        runUnusedStatsRemover();

        final String index = MlStatsIndex.TEMPLATE_NAME + "-000001";

        // Validate expected docs
        assertDocExists(index, InferenceStats.docId(modelId, "test"));
        assertDocExists(index, DataCounts.documentId("analytics-with-stats"));
        assertDocExists(index, InferenceStats.docId(TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(), "test"));

        // Validate removed docs
        assertDocDoesNotExist(index, InferenceStats.docId("missing-model", "test"));
        assertDocDoesNotExist(index, DataCounts.documentId("missing-analytics-with-stats"));
    }

    public void testRemovingUnusedStatsFromReadOnlyIndexShouldFailSilently() throws Exception {
        String modelId = "model-with-stats";
        putDFA(modelId);

        indexStatDocument(
            new InferenceStats(1, 1, 1, 1, "missing-model", "test", Instant.now()),
            InferenceStats.docId("missing-model", "test")
        );
        makeIndexReadOnly();
        refreshStatsIndex();

        runUnusedStatsRemover();
        refreshStatsIndex();

        final String index = MlStatsIndex.TEMPLATE_NAME + "-000001";
        assertDocExists(index, InferenceStats.docId("missing-model", "test")); // should still exist
    }

    private void putDFA(String modelId) {
        prepareIndex("foo").setId("some-empty-doc").setSource("{}", XContentType.JSON).get();

        PutDataFrameAnalyticsAction.Request analyticsRequest = new PutDataFrameAnalyticsAction.Request(
            new DataFrameAnalyticsConfig.Builder().setId("analytics-with-stats")
                .setModelMemoryLimit(ByteSizeValue.ofGb(1))
                .setSource(new DataFrameAnalyticsSource(new String[] { "foo" }, null, null, null))
                .setDest(new DataFrameAnalyticsDest("bar", null))
                .setAnalysis(new Regression("prediction"))
                .build()
        );
        client.execute(PutDataFrameAnalyticsAction.INSTANCE, analyticsRequest).actionGet();

        TrainedModelDefinition.Builder definition = new TrainedModelDefinition.Builder().setPreProcessors(Collections.emptyList())
            .setTrainedModel(
                Tree.builder().setFeatureNames(Arrays.asList("foo", "bar")).setRoot(TreeNode.builder(0).setLeafValue(42)).build()
            );

        TrainedModelConfig modelConfig = TrainedModelConfig.builder()
            .setModelId(modelId)
            .setInferenceConfig(RegressionConfig.EMPTY_PARAMS)
            .setInput(new TrainedModelInput(Arrays.asList("foo", "bar")))
            .setParsedDefinition(definition)
            .validate(true)
            .build();

        client.execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(modelConfig, false)).actionGet();
    }

    private void indexStatDocument(ToXContentObject object, String docId) throws Exception {
        IndexRequest doc = new IndexRequest(MlStatsIndex.writeAlias()).id(docId);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            object.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")));
            doc.source(builder);
            client.index(doc).actionGet();
        }
    }

    private void refreshStatsIndex() {
        client().admin().indices().prepareRefresh(MlStatsIndex.indexPattern()).get();
    }

    private void runUnusedStatsRemover() {
        PlainActionFuture<Boolean> deletionListener = new PlainActionFuture<>();
        new UnusedStatsRemover(client, new TaskId("test", 0L)).remove(10000.0f, deletionListener, () -> false);
        deletionListener.actionGet();
    }

    private void makeIndexReadOnly() {
        client().admin()
            .indices()
            .prepareUpdateSettings(MlStatsIndex.indexPattern())
            .setSettings(Settings.builder().put("index.blocks.write", true))
            .get();
    }

    private void assertDocExists(String index, String docId) {
        assertTrue(client().prepareGet(index, docId).get().isExists());
    }

    private void assertDocDoesNotExist(String index, String docId) {
        assertFalse(client().prepareGet(index, docId).get().isExists());
    }
}
