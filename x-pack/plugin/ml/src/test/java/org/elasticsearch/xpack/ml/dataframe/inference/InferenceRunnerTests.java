/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RegressionTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.ml.dataframe.stats.DataCountsTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.SourceField;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceRunnerTests extends ESTestCase {

    private Client client;
    private ResultsPersisterService resultsPersisterService;
    private ModelLoadingService modelLoadingService;
    private DataFrameAnalyticsConfig config;
    private ProgressTracker progressTracker;
    private TaskId parentTaskId;

    @Before
    public void setupTests() {
        client = mock(Client.class);
        resultsPersisterService = mock(ResultsPersisterService.class);
        config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setAnalysis(RegressionTests.createRandom())
            .setSource(new DataFrameAnalyticsSource(new String[] {"source_index"}, null, null, null))
            .setDest(new DataFrameAnalyticsDest("dest_index", "test_results_field"))
            .build();
        progressTracker = ProgressTracker.fromZeroes(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference());
        parentTaskId = new TaskId(randomAlphaOfLength(10), randomLong());
        modelLoadingService = mock(ModelLoadingService.class);
    }

    public void testInferTestDocs() {
        ExtractedFields extractedFields = new ExtractedFields(
            Collections.singletonList(new SourceField("key", Collections.singleton("integer"))),
            Collections.emptyList(),
            Collections.emptyMap());

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("key", 1);
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("key", 2);
        TestDocsIterator testDocsIterator = mock(TestDocsIterator.class);
        when(testDocsIterator.hasNext()).thenReturn(true, false);
        when(testDocsIterator.next()).thenReturn(buildSearchHits(Arrays.asList(doc1, doc2)));
        when(testDocsIterator.getTotalHits()).thenReturn(2L);
        InferenceConfig config = ClassificationConfig.EMPTY_PARAMS;

        LocalModel localModel = localModelInferences(new ClassificationInferenceResults(1.0,
                "foo",
                Collections.emptyList(),
                Collections.emptyList(),
                config,
                1.0,
                1.0),
            new ClassificationInferenceResults(0.0,
                "bar",
                Collections.emptyList(),
                Collections.emptyList(),
                config,
                .5,
                .7));

        InferenceRunner inferenceRunner = createInferenceRunner(extractedFields);

        inferenceRunner.inferTestDocs(localModel, testDocsIterator, 0L);

        ArgumentCaptor<BulkRequest> argumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

        verify(resultsPersisterService).bulkIndexWithHeadersWithRetry(any(), argumentCaptor.capture(), any(), any(), any());
        assertThat(progressTracker.getInferenceProgressPercent(), equalTo(100));

        BulkRequest bulkRequest = argumentCaptor.getAllValues().get(0);
        List<DocWriteRequest<?>> indexRequests = bulkRequest.requests();
        Map<String, Object> doc1Source = ((IndexRequest)indexRequests.get(0)).sourceAsMap();
        Map<String, Object> doc2Source = ((IndexRequest)indexRequests.get(1)).sourceAsMap();

        assertThat(doc1Source.get("test_results_field"),
            equalTo(new HashMap<>(){{
                put("predicted_value", "foo");
                put("prediction_probability", 1.0);
                put("prediction_score", 1.0);
                put("predicted_value", "foo");
                put("is_training", false);
        }}));
        assertThat(doc2Source.get("test_results_field"),
            equalTo(new HashMap<>(){{
                put("predicted_value", "bar");
                put("prediction_probability", 0.5);
                put("prediction_score", .7);
                put("is_training", false);
            }}));
    }

    public void testInferTestDocs_GivenCancelWasCalled() {
        ExtractedFields extractedFields = mock(ExtractedFields.class);
        LocalModel localModel = mock(LocalModel.class);

        TestDocsIterator infiniteDocsIterator = mock(TestDocsIterator.class);
        when(infiniteDocsIterator.hasNext()).thenReturn(true);

        InferenceRunner inferenceRunner = createInferenceRunner(extractedFields);
        inferenceRunner.cancel();

        inferenceRunner.inferTestDocs(localModel, infiniteDocsIterator, 0L);

        Mockito.verifyNoMoreInteractions(localModel, resultsPersisterService);
        assertThat(progressTracker.getInferenceProgressPercent(), equalTo(0));
    }

    private static Deque<SearchHit> buildSearchHits(List<Map<String, Object>> vals) {
        return vals.stream()
            .map(InferenceRunnerTests::fromMap)
            .map(reference -> SearchHit.createFromMap(Collections.singletonMap("_source", reference)))
            .collect(Collectors.toCollection(ArrayDeque::new));
    }

    private static BytesReference fromMap(Map<String, Object> map) {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(map)) {
            return BytesReference.bytes(xContentBuilder);
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
    }

    private LocalModel localModelInferences(InferenceResults first, InferenceResults... rest) {
        LocalModel localModel = mock(LocalModel.class);
        when(localModel.inferNoStats(any())).thenReturn(first, rest);
        return localModel;
    }

    private InferenceRunner createInferenceRunner(ExtractedFields extractedFields) {
        return new InferenceRunner(Settings.EMPTY, client, modelLoadingService,  resultsPersisterService, parentTaskId, config,
            extractedFields, progressTracker, new DataCountsTracker(new DataCounts(config.getId())));
    }
}
