/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalysisConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsFields;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcess;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessConfig;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessFactory;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsManagerIT extends BaseMlIntegTestCase {

    private volatile boolean finished;
    private DataFrameAnalyticsConfigProvider provider;
    private static double EXPECTED_OUTLIER_SCORE = 42.0;
    @Before
    public void fieldSetup() {
        provider = new DataFrameAnalyticsConfigProvider(client());
        finished = false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateMachineLearning.class, CommonAnalysisPlugin.class,
            ReindexPlugin.class, PainlessPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class, Netty4Plugin.class, PainlessPlugin.class, ReindexPlugin.class);
    }

    public void testTaskContinuationFromReindexState() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
        String sourceIndex = "test-outlier-detection-from-reindex-state";
        createIndexForAnalysis(sourceIndex);
        String id = "test_outlier_detection_from_reindex_state";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex);
        putDataFrameAnalyticsConfig(config);
        List<AnalyticsResult> results = buildExpectedResults(sourceIndex);

        DataFrameAnalyticsManager manager = createManager(results);

        DataFrameAnalyticsTask task = buildMockedTask(config.getId());
        manager.execute(task, DataFrameAnalyticsState.REINDEXING);

        // wait for markAsCompleted() or markAsFailed() to be called;
        assertBusy(() -> assertTrue(isCompleted()), 120, TimeUnit.SECONDS);

        // Check we've got all docs
        SearchResponse searchResponse = client().prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).get();
        Assert.assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 5));
        for(SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> src = hit.getSourceAsMap();
            assertNotNull(src.get("outlier_score"));
            assertThat(src.get("outlier_score"), equalTo(EXPECTED_OUTLIER_SCORE));
        }

        verify(task, never()).markAsFailed(any(Exception.class));
        verify(task, times(1)).markAsCompleted();
    }

    public void testTaskContinuationFromReindexStateWithPreviousResultsIndex() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
        String sourceIndex = "test-outlier-detection-from-reindex-state-with-results";
        createIndexForAnalysis(sourceIndex);
        String id = "test_outlier_detection_from_reindex_state_with_results";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex);
        putDataFrameAnalyticsConfig(config);

        // Create the "results" index, as if we ran reindex already in the process, but did not transition from the state properly
        createAnalysesResultsIndex(config.getDest().getIndex(), false);
        List<AnalyticsResult> results = buildExpectedResults(sourceIndex);

        DataFrameAnalyticsManager manager = createManager(results);

        DataFrameAnalyticsTask task = buildMockedTask(config.getId());
        manager.execute(task, DataFrameAnalyticsState.REINDEXING);

        // wait for markAsCompleted() or markAsFailed() to be called;
        assertBusy(() -> assertTrue(isCompleted()), 120, TimeUnit.SECONDS);

        // Check we've got all docs
        SearchResponse searchResponse = client().prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).get();
        Assert.assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 5));
        for(SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> src = hit.getSourceAsMap();
            assertNotNull(src.get("outlier_score"));
            assertThat(src.get("outlier_score"), equalTo(EXPECTED_OUTLIER_SCORE));
        }

        verify(task, never()).markAsFailed(any(Exception.class));
        verify(task, times(1)).markAsCompleted();
    }

    public void testTaskContinuationFromAnalyzeState() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
        String sourceIndex = "test-outlier-detection-from-analyze-state";
        createIndexForAnalysis(sourceIndex);
        String id = "test_outlier_detection_from_analyze_state";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex);
        putDataFrameAnalyticsConfig(config);
        // Create the "results" index to simulate running reindex already and having partially ran analysis
        createAnalysesResultsIndex(config.getDest().getIndex(), true);
        List<AnalyticsResult> results = buildExpectedResults(sourceIndex);

        DataFrameAnalyticsManager manager = createManager(results);

        DataFrameAnalyticsTask task = buildMockedTask(config.getId());
        manager.execute(task, DataFrameAnalyticsState.ANALYZING);

        // wait for markAsCompleted() or markAsFailed() to be called;
        assertBusy(() -> assertTrue(isCompleted()), 120, TimeUnit.SECONDS);

        // Check we've got all docs
        SearchResponse searchResponse = client().prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).get();
        Assert.assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 5));
        for(SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> src = hit.getSourceAsMap();
            assertNotNull(src.get("outlier_score"));
            assertThat(src.get("outlier_score"), equalTo(EXPECTED_OUTLIER_SCORE));
        }

        verify(task, never()).markAsFailed(any(Exception.class));
        verify(task, times(1)).markAsCompleted();
        // Need to verify that we did not reindex again, as we already had the full destination index
        verify(task, never()).setReindexingTaskId(anyLong());
    }

    private synchronized void completed() {
        finished = true;
    }

    private synchronized boolean isCompleted() {
        return finished;
    }

    private static DataFrameAnalyticsConfig buildOutlierDetectionAnalytics(String id, String sourceIndex) {
        DataFrameAnalyticsConfig.Builder configBuilder = new DataFrameAnalyticsConfig.Builder(id);
        configBuilder.setSource(new DataFrameAnalyticsSource(sourceIndex, null));
        configBuilder.setDest(new DataFrameAnalyticsDest(sourceIndex + "-results"));
        Map<String, Object> analysisConfig = new HashMap<>();
        analysisConfig.put("outlier_detection", Collections.emptyMap());
        configBuilder.setAnalyses(Collections.singletonList(new DataFrameAnalysisConfig(analysisConfig)));
        return configBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private void putDataFrameAnalyticsConfig(DataFrameAnalyticsConfig config) throws Exception {
        PlainActionFuture future = new PlainActionFuture();
        provider.put(config, Collections.emptyMap(), future);
        future.get();
    }

    private void createIndexForAnalysis(String indexName) {
        client().admin().indices().prepareCreate(indexName)
            .addMapping("_doc", "numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0, "numeric_2", 1.0, "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            Assert.fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private void createAnalysesResultsIndex(String indexName, boolean includeOutlierScore) {
        client().admin().indices().prepareCreate(indexName)
            .addMapping("_doc",
                "numeric_1", "type=double",
                "numeric_2", "type=float",
                "categorical_1", "type=keyword",
                DataFrameAnalyticsFields.ID, "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            if (includeOutlierScore) {
                indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0,
                    "numeric_2", 1.0,
                    "categorical_1", "foo_" + i,
                    DataFrameAnalyticsFields.ID, docId,
                    "outlier_score", 10.0); // simply needs to be a score different than expected
            } else {
                indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0,
                    "numeric_2", 1.0,
                    "categorical_1", "foo_" + i,
                    DataFrameAnalyticsFields.ID, docId);
            }
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            Assert.fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private List<AnalyticsResult> buildExpectedResults(String index) throws Exception {
        SearchHit[] hits = client().search(new SearchRequest(index)).get().getHits().getHits();
        Arrays.sort(hits, Comparator.comparing(SearchHit::getId));
        List<AnalyticsResult> results = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            String[] fields = new String[2];
            Map<String, Object> src = hit.getSourceAsMap();
            fields[0] = src.get("numeric_1").toString();
            fields[1] = src.get("numeric_2").toString();
            results.add(new AnalyticsResult(new RowResults(Arrays.hashCode(fields),
                Collections.singletonMap("outlier_score", EXPECTED_OUTLIER_SCORE)),null));
        }
        return results;
    }

    private DataFrameAnalyticsManager createManager(List<AnalyticsResult> expectedResults) {
        AnalyticsProcessFactory factory = new MockedAnalyticsFactory(expectedResults);
        AnalyticsProcessManager processManager = new AnalyticsProcessManager(client(),
            TestEnvironment.newEnvironment(internalCluster().getDefaultSettings()),
            client().threadPool(),
            factory);
        return new DataFrameAnalyticsManager(clusterService(), (NodeClient)internalCluster().dataNodeClient(), provider, processManager);
    }

    @SuppressWarnings("unchecked")
    private DataFrameAnalyticsTask buildMockedTask(String id) {
        StartDataFrameAnalyticsAction.TaskParams params = new StartDataFrameAnalyticsAction.TaskParams(id);
        DataFrameAnalyticsTask task = mock(DataFrameAnalyticsTask.class);
        when(task.getParams()).thenReturn(params);
        when(task.getAllocationId()).thenReturn(1L);
        doAnswer(invoked -> {
            client().threadPool().executor("listener").execute(() -> {
                ActionListener listener = (ActionListener) invoked.getArguments()[1];
                final PersistentTasksCustomMetaData.PersistentTask<?> resp = new PersistentTasksCustomMetaData.PersistentTask<>(id,
                    MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                    params,
                    1,
                    new PersistentTasksCustomMetaData.Assignment(null, "none"));
                listener.onResponse(resp);
            });
            return null;
        }).when(task).updatePersistentTaskState(any(DataFrameAnalyticsTaskState.class), any(ActionListener.class));
        doNothing().when(task).setReindexingTaskId(anyLong());
        doAnswer(invoked -> {
            completed();
            return null;
        }).when(task).markAsCompleted();
        doAnswer(invoked -> {
            completed();
            Exception e = (Exception)invoked.getArguments()[0];
            fail(e.getMessage());
            return null;
        }).when(task).markAsFailed(any(Exception.class));
        return task;
    }

    class MockedAnalyticsFactory implements AnalyticsProcessFactory {
        final List<AnalyticsResult> results;

        MockedAnalyticsFactory(List<AnalyticsResult> resultsToSupply) {
            this.results = resultsToSupply;
        }
        @Override
        public AnalyticsProcess createAnalyticsProcess(String jobId,
                                                       AnalyticsProcessConfig analyticsProcessConfig,
                                                       ExecutorService executorService) {
            return new MockedAnalyticsProcess(results);
        }
    }

    class MockedAnalyticsProcess implements AnalyticsProcess {

        final List<AnalyticsResult> results;
        final ZonedDateTime start;
        MockedAnalyticsProcess(List<AnalyticsResult> resultsToSupply) {
            results = resultsToSupply;
            start = ZonedDateTime.now();
        }

        @Override
        public void writeEndOfDataMessage() throws IOException { }

        @Override
        public Iterator<AnalyticsResult> readAnalyticsResults() {
            return results.iterator();
        }

        @Override
        public void consumeAndCloseOutputStream() { }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void writeRecord(String[] record) throws IOException { }

        @Override
        public void persistState() throws IOException { }

        @Override
        public void flushStream() throws IOException { }

        @Override
        public void kill() throws IOException { }

        @Override
        public ZonedDateTime getProcessStartTime() {
            return start;
        }

        @Override
        public boolean isProcessAlive() {
            return true;
        }

        @Override
        public boolean isProcessAliveAfterWaiting() {
            return false;
        }

        @Override
        public String readError() {
            return null;
        }

        @Override
        public void close() throws IOException { }
    }
}
