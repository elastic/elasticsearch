/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Base class of ML integration tests that use a native data_frame_analytics process
 */
abstract class MlNativeDataFrameAnalyticsIntegTestCase extends MlNativeIntegTestCase {

    private List<DataFrameAnalyticsConfig> analytics = new ArrayList<>();

    @Override
    protected void cleanUpResources() {
        cleanUpAnalytics();
    }

    private void cleanUpAnalytics() {
        for (DataFrameAnalyticsConfig config : analytics) {
            try {
                assertThat(deleteAnalytics(config.getId()).isAcknowledged(), is(true));
                assertThat(searchStoredProgress(config.getId()).getHits().getTotalHits().value, equalTo(0L));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    protected void registerAnalytics(DataFrameAnalyticsConfig config) {
        if (analytics.add(config) == false) {
            throw new IllegalArgumentException("analytics config [" + config.getId() + "] is already registered");
        }
    }

    protected PutDataFrameAnalyticsAction.Response putAnalytics(DataFrameAnalyticsConfig config) {
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        return client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse deleteAnalytics(String id) {
        DeleteDataFrameAnalyticsAction.Request request = new DeleteDataFrameAnalyticsAction.Request(id);
        return client().execute(DeleteDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse startAnalytics(String id) {
        StartDataFrameAnalyticsAction.Request request = new StartDataFrameAnalyticsAction.Request(id);
        return client().execute(StartDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected StopDataFrameAnalyticsAction.Response stopAnalytics(String id) {
        StopDataFrameAnalyticsAction.Request request = new StopDataFrameAnalyticsAction.Request(id);
        return client().execute(StopDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected void waitUntilAnalyticsIsStopped(String id) throws Exception {
        waitUntilAnalyticsIsStopped(id, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilAnalyticsIsStopped(String id, TimeValue waitTime) throws Exception {
        assertBusy(() -> assertThat(getAnalyticsStats(id).get(0).getState(), equalTo(DataFrameAnalyticsState.STOPPED)),
                waitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    protected List<DataFrameAnalyticsConfig> getAnalytics(String id) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request(id);
        return client().execute(GetDataFrameAnalyticsAction.INSTANCE, request).actionGet().getResources().results();
    }

    protected List<GetDataFrameAnalyticsStatsAction.Response.Stats> getAnalyticsStats(String id) {
        GetDataFrameAnalyticsStatsAction.Request request = new GetDataFrameAnalyticsStatsAction.Request(id);
        GetDataFrameAnalyticsStatsAction.Response response = client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE, request)
            .actionGet();
        return response.getResponse().results();
    }

    protected static DataFrameAnalyticsConfig buildOutlierDetectionAnalytics(String id, String[] sourceIndex, String destIndex,
                                                                             @Nullable String resultsField) {
        DataFrameAnalyticsConfig.Builder configBuilder = new DataFrameAnalyticsConfig.Builder();
        configBuilder.setId(id);
        configBuilder.setSource(new DataFrameAnalyticsSource(sourceIndex, null));
        configBuilder.setDest(new DataFrameAnalyticsDest(destIndex, resultsField));
        configBuilder.setAnalysis(new OutlierDetection());
        return configBuilder.build();
    }

    protected void assertState(String id, DataFrameAnalyticsState state) {
        List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = getAnalyticsStats(id);
        assertThat(stats.size(), equalTo(1));
        assertThat(stats.get(0).getId(), equalTo(id));
        assertThat(stats.get(0).getState(), equalTo(state));
    }

    protected void assertProgress(String id, int reindexing, int loadingData, int analyzing, int writingResults) {
        List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = getAnalyticsStats(id);
        List<PhaseProgress> progress = stats.get(0).getProgress();
        assertThat(stats.size(), equalTo(1));
        assertThat(stats.get(0).getId(), equalTo(id));
        assertThat(progress.size(), equalTo(4));
        assertThat(progress.get(0).getPhase(), equalTo("reindexing"));
        assertThat(progress.get(1).getPhase(), equalTo("loading_data"));
        assertThat(progress.get(2).getPhase(), equalTo("analyzing"));
        assertThat(progress.get(3).getPhase(), equalTo("writing_results"));
        assertThat(progress.get(0).getProgressPercent(), equalTo(reindexing));
        assertThat(progress.get(1).getProgressPercent(), equalTo(loadingData));
        assertThat(progress.get(2).getProgressPercent(), equalTo(analyzing));
        assertThat(progress.get(3).getProgressPercent(), equalTo(writingResults));
    }

    protected SearchResponse searchStoredProgress(String id) {
        return client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setQuery(QueryBuilders.idsQuery().addIds(DataFrameAnalyticsTask.progressDocId(id)))
            .get();
    }

    protected static DataFrameAnalyticsConfig buildRegressionAnalytics(String id, String[] sourceIndex, String destIndex,
                                                                       @Nullable String resultsField, Regression regression) {
        DataFrameAnalyticsConfig.Builder configBuilder = new DataFrameAnalyticsConfig.Builder();
        configBuilder.setId(id);
        configBuilder.setSource(new DataFrameAnalyticsSource(sourceIndex, null));
        configBuilder.setDest(new DataFrameAnalyticsDest(destIndex, resultsField));
        configBuilder.setAnalysis(regression);
        return configBuilder.build();
    }
}
