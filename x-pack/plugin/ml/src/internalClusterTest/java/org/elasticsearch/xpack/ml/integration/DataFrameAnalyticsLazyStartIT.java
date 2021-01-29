/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import java.util.Collection;
import java.util.Set;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig.DEFAULT_MODEL_MEMORY_LIMIT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameAnalyticsLazyStartIT extends BaseMlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Before
    public void setupCluster() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)));
        ensureStableCluster();
    }

    private void ensureStableCluster() {
        ensureStableCluster(internalCluster().getNodeNames().length, TimeValue.timeValueSeconds(60));
    }

    public void testNoMlNodesLazyStart() throws Exception {
        String indexName = "data";
        createIndex(indexName);

        DataFrameAnalyticsConfig.Builder dataFrameAnalyticsConfig = new DataFrameAnalyticsConfig
            .Builder()
            .setSource(new DataFrameAnalyticsSource(new String[]{indexName}, null, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setDest(new DataFrameAnalyticsDest("foo", null));
        {
            String analyticsId = "not-lazy-dfa";
            client().execute(
                PutDataFrameAnalyticsAction.INSTANCE,
                new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).build()))
                .actionGet();
            Exception ex = expectThrows(Exception.class,
                () -> client().execute(
                    StartDataFrameAnalyticsAction.INSTANCE,
                    new StartDataFrameAnalyticsAction.Request(analyticsId)
                ).actionGet());
            assertThat(ex.getMessage(), containsString("No ML node to run on"));
        }
        {
            String analyticsId = "lazy-dfa";
            client().execute(
                PutDataFrameAnalyticsAction.INSTANCE,
                new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).setAllowLazyStart(true).build()))
                .actionGet();
            client().execute(StartDataFrameAnalyticsAction.INSTANCE, new StartDataFrameAnalyticsAction.Request(analyticsId)).actionGet();
            // it is starting lazily
            assertBusy(() -> {
                assertThat(client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE,
                    new GetDataFrameAnalyticsStatsAction.Request(analyticsId))
                    .actionGet()
                    .getResponse()
                    .results()
                    .get(0)
                    .getState(), equalTo(DataFrameAnalyticsState.STARTING));
            });
            client().execute(StopDataFrameAnalyticsAction.INSTANCE, new StopDataFrameAnalyticsAction.Request(analyticsId)).actionGet();
            assertBusy(() -> {
                assertThat(client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE,
                    new GetDataFrameAnalyticsStatsAction.Request(analyticsId))
                    .actionGet()
                    .getResponse()
                    .results()
                    .get(0)
                    .getState(), equalTo(DataFrameAnalyticsState.STOPPED));
            });
        }
    }

    public void testNoMlNodesButWithLazyNodes() throws Exception {
        String indexName = "data";
        createIndex(indexName);

        client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 10_000))
            .get();

        String analyticsId = "not-lazy-dfa-with-lazy-nodes";
        DataFrameAnalyticsConfig.Builder dataFrameAnalyticsConfig = new DataFrameAnalyticsConfig
            .Builder()
            .setId(analyticsId)
            .setSource(new DataFrameAnalyticsSource(new String[]{indexName}, null, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setDest(new DataFrameAnalyticsDest("foo", null));
        client().execute(
            PutDataFrameAnalyticsAction.INSTANCE,
            new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).build()))
            .actionGet();
        client().execute(StartDataFrameAnalyticsAction.INSTANCE, new StartDataFrameAnalyticsAction.Request(analyticsId)).actionGet();
        // it is starting lazily
        assertBusy(() -> {
            assertThat(client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE,
                new GetDataFrameAnalyticsStatsAction.Request(analyticsId))
                .actionGet()
                .getResponse()
                .results()
                .get(0)
                .getState(), equalTo(DataFrameAnalyticsState.STARTING));
        });
        client().execute(StopDataFrameAnalyticsAction.INSTANCE, new StopDataFrameAnalyticsAction.Request(analyticsId)).actionGet();
        assertBusy(() -> {
            assertThat(client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE,
                new GetDataFrameAnalyticsStatsAction.Request(analyticsId))
                .actionGet()
                .getResponse()
                .results()
                .get(0)
                .getState(), equalTo(DataFrameAnalyticsState.STOPPED));
        });

        client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(MachineLearning.MAX_LAZY_ML_NODES.getKey()))
            .get();
    }

    public void testExplainWithLazyStartSet() {
        String indexName = "data";
        createIndex(indexName);

        String analyticsId = "not-lazy-dfa-with-lazy-nodes";
        DataFrameAnalyticsConfig.Builder dataFrameAnalyticsConfig = new DataFrameAnalyticsConfig
            .Builder()
            .setId(analyticsId)
            .setSource(new DataFrameAnalyticsSource(new String[]{indexName}, null, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setDest(new DataFrameAnalyticsDest("foo", null));

        Exception ex = expectThrows(Exception.class, () -> client().execute(
            ExplainDataFrameAnalyticsAction.INSTANCE,
            new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).buildForExplain()))
            .actionGet());
        assertThat(ex.getMessage(), containsString("No ML node to run on"));


        ExplainDataFrameAnalyticsAction.Response response = client().execute(
            ExplainDataFrameAnalyticsAction.INSTANCE,
            new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).setAllowLazyStart(true).buildForExplain()))
            .actionGet();

        assertThat(response.getMemoryEstimation().getExpectedMemoryWithoutDisk(), equalTo(DEFAULT_MODEL_MEMORY_LIMIT));
        assertThat(response.getMemoryEstimation().getExpectedMemoryWithDisk(), equalTo(DEFAULT_MODEL_MEMORY_LIMIT));
    }

    public void testExplainWithLazyMlNodes() {
        String indexName = "data";
        createIndex(indexName);

        client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 10_000))
            .get();

        String analyticsId = "not-lazy-dfa-with-lazy-nodes";
        DataFrameAnalyticsConfig.Builder dataFrameAnalyticsConfig = new DataFrameAnalyticsConfig
            .Builder()
            .setId(analyticsId)
            .setSource(new DataFrameAnalyticsSource(new String[]{indexName}, null, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setDest(new DataFrameAnalyticsDest("foo", null));

        ExplainDataFrameAnalyticsAction.Response response = client().execute(
            ExplainDataFrameAnalyticsAction.INSTANCE,
            new PutDataFrameAnalyticsAction.Request(dataFrameAnalyticsConfig.setId(analyticsId).buildForExplain()))
            .actionGet();

        assertThat(response.getMemoryEstimation().getExpectedMemoryWithoutDisk(), equalTo(DEFAULT_MODEL_MEMORY_LIMIT));
        assertThat(response.getMemoryEstimation().getExpectedMemoryWithDisk(), equalTo(DEFAULT_MODEL_MEMORY_LIMIT));
    }

    private void createIndex(String indexName) {
        client().admin().indices().prepareCreate(indexName).get();
        client().prepareIndex(indexName)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"field\": 1, \"other\": 2}", XContentType.JSON)
            .get();
    }
}
