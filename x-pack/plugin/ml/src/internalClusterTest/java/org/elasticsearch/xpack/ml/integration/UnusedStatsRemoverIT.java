/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
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
        MlStatsIndex.createStatsIndexAndAliasIfNecessary(client(), clusterService().state(), new IndexNameExpressionResolver(), future);
        future.actionGet();
    }

    public void testRemoveUnusedStats() throws Exception {

        client().prepareIndex("foo").setId("some-empty-doc").setSource("{}", XContentType.JSON).get();

        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(new DataFrameAnalyticsConfig.Builder()
            .setId("analytics-with-stats")
            .setModelMemoryLimit(new ByteSizeValue(1, ByteSizeUnit.GB))
            .setSource(new DataFrameAnalyticsSource(new String[]{"foo"}, null, null))
            .setDest(new DataFrameAnalyticsDest("bar", null))
            .setAnalysis(new Regression("prediction"))
            .build());
        client.execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();

        client.execute(PutTrainedModelAction.INSTANCE,
            new PutTrainedModelAction.Request(TrainedModelConfig.builder()
                .setModelId("model-with-stats")
                .setInferenceConfig(RegressionConfig.EMPTY_PARAMS)
                .setInput(new TrainedModelInput(Arrays.asList("foo", "bar")))
                .setParsedDefinition(new TrainedModelDefinition.Builder()
                    .setPreProcessors(Collections.emptyList())
                    .setTrainedModel(Tree.builder()
                        .setFeatureNames(Arrays.asList("foo", "bar"))
                        .setRoot(TreeNode.builder(0).setLeafValue(42))
                        .build())
                )
                .validate(true)
                .build())).actionGet();

        indexStatDocument(new DataCounts("analytics-with-stats", 1, 1, 1),
            DataCounts.documentId("analytics-with-stats"));
        indexStatDocument(new DataCounts("missing-analytics-with-stats", 1, 1, 1),
            DataCounts.documentId("missing-analytics-with-stats"));
        indexStatDocument(new InferenceStats(1,
            1,
            1,
            1,
            TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(),
            "test",
            Instant.now()),
            InferenceStats.docId(TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(), "test"));
        indexStatDocument(new InferenceStats(1,
                1,
                1,
                1,
                "missing-model",
                "test",
                Instant.now()),
            InferenceStats.docId("missing-model", "test"));
        indexStatDocument(new InferenceStats(1,
                1,
                1,
                1,
                "model-with-stats",
                "test",
                Instant.now()),
            InferenceStats.docId("model-with-stats", "test"));
        client().admin().indices().prepareRefresh(MlStatsIndex.indexPattern()).get();

        PlainActionFuture<Boolean> deletionListener = new PlainActionFuture<>();
        UnusedStatsRemover statsRemover = new UnusedStatsRemover(client);
        statsRemover.remove(10000.0f, deletionListener, () -> false);
        deletionListener.actionGet();

        client().admin().indices().prepareRefresh(MlStatsIndex.indexPattern()).get();

        final String initialStateIndex = MlStatsIndex.TEMPLATE_NAME + "-000001";

        // Make sure that stats that should exist still exist
        assertTrue(client().prepareGet(initialStateIndex,
            InferenceStats.docId("model-with-stats", "test")).get().isExists());
        assertTrue(client().prepareGet(initialStateIndex,
            InferenceStats.docId(TrainedModelProvider.MODELS_STORED_AS_RESOURCE.iterator().next(), "test")).get().isExists());
        assertTrue(client().prepareGet(initialStateIndex, DataCounts.documentId("analytics-with-stats")).get().isExists());

        // make sure that unused stats were deleted
        assertFalse(client().prepareGet(initialStateIndex, DataCounts.documentId("missing-analytics-with-stats")).get().isExists());
        assertFalse(client().prepareGet(initialStateIndex,
            InferenceStats.docId("missing-model", "test")).get().isExists());
    }

    private void indexStatDocument(ToXContentObject object, String docId) throws Exception {
        ToXContent.Params params =  new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE,
            Boolean.toString(true)));
        IndexRequest doc = new IndexRequest(MlStatsIndex.writeAlias());
        doc.id(docId);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            object.toXContent(builder, params);
            doc.source(builder);
            client.index(doc).actionGet();
        }
    }
}
