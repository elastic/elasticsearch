/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RegressionTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
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
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
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
        client = mockClient();
        resultsPersisterService = mock(ResultsPersisterService.class);
        config = new DataFrameAnalyticsConfig.Builder().setId("test")
            .setAnalysis(RegressionTests.createRandom())
            .setSource(new DataFrameAnalyticsSource(new String[] { "source_index" }, null, null, null))
            .setDest(new DataFrameAnalyticsDest("dest_index", "test_results_field"))
            .build();
        progressTracker = ProgressTracker.fromZeroes(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference());
        parentTaskId = new TaskId(randomAlphaOfLength(10), randomLong());
        modelLoadingService = mock(ModelLoadingService.class);
    }

    public void testInferTestDocs() {
        var extractedFields = new ExtractedFields(List.of(new SourceField("key", Set.of("integer"))), List.of(), Map.of());

        var testDocsIterator = mock(TestDocsIterator.class);
        when(testDocsIterator.hasNext()).thenReturn(true, false);
        when(testDocsIterator.next()).thenReturn(buildSearchHits(List.of(Map.of("key", 1), Map.of("key", 2))));
        when(testDocsIterator.getTotalHits()).thenReturn(2L);
        var config = ClassificationConfig.EMPTY_PARAMS;

        var localModel = localModelInferences(
            new ClassificationInferenceResults(1.0, "foo", List.of(), List.of(), config, 1.0, 1.0),
            new ClassificationInferenceResults(0.0, "bar", List.of(), List.of(), config, .5, .7)
        );

        doAnswer(ans -> {
            ActionListener<LocalModel> responseListener = ans.getArgument(1);
            responseListener.onResponse(localModel);
            return null;
        }).when(modelLoadingService).getModelForInternalInference(anyString(), any());

        run(createInferenceRunner(extractedFields, testDocsIterator)).assertSuccess();

        var argumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

        verify(resultsPersisterService).bulkIndexWithHeadersWithRetry(any(), argumentCaptor.capture(), any(), any(), any());
        assertThat(progressTracker.getInferenceProgressPercent(), equalTo(100));

        BulkRequest bulkRequest = argumentCaptor.getAllValues().get(0);
        List<DocWriteRequest<?>> indexRequests = bulkRequest.requests();
        Map<String, Object> doc1Source = ((IndexRequest) indexRequests.get(0)).sourceAsMap();
        Map<String, Object> doc2Source = ((IndexRequest) indexRequests.get(1)).sourceAsMap();

        assertThat(
            doc1Source.get("test_results_field"),
            equalTo(Map.of("predicted_value", "foo", "prediction_probability", 1.0, "prediction_score", 1.0, "is_training", false))
        );
        assertThat(
            doc2Source.get("test_results_field"),
            equalTo(Map.of("predicted_value", "bar", "prediction_probability", 0.5, "prediction_score", .7, "is_training", false))
        );
    }

    public void testInferTestDocs_GivenCancelWasCalled() {
        ExtractedFields extractedFields = mock(ExtractedFields.class);
        LocalModel localModel = mock(LocalModel.class);

        TestDocsIterator infiniteDocsIterator = mock(TestDocsIterator.class);
        when(infiniteDocsIterator.hasNext()).thenReturn(true);

        InferenceRunner inferenceRunner = createInferenceRunner(extractedFields, infiniteDocsIterator);
        inferenceRunner.cancel();
        run(inferenceRunner).assertSuccess();

        Mockito.verifyNoMoreInteractions(localModel, resultsPersisterService);
        assertThat(progressTracker.getInferenceProgressPercent(), equalTo(0));
    }

    private static Deque<SearchHit> buildSearchHits(List<Map<String, Object>> vals) {
        return vals.stream().map(InferenceRunnerTests::fromMap).map(reference -> {
            var pooled = SearchResponseUtils.searchHitFromMap(Map.of("_source", reference));
            try {
                return pooled.asUnpooled();
            } finally {
                pooled.decRef();
            }
        }).collect(Collectors.toCollection(ArrayDeque::new));
    }

    private static BytesReference fromMap(Map<String, Object> map) {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(map)) {
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
        return createInferenceRunner(extractedFields, mock(TestDocsIterator.class));
    }

    private InferenceRunner createInferenceRunner(ExtractedFields extractedFields, TestDocsIterator testDocsIterator) {
        var threadpool = mock(ThreadPool.class);
        when(threadpool.executor(any())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadpool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        return new InferenceRunner(
            Settings.EMPTY,
            client,
            modelLoadingService,
            resultsPersisterService,
            parentTaskId,
            config,
            extractedFields,
            progressTracker,
            new DataCountsTracker(new DataCounts(config.getId())),
            id -> testDocsIterator,
            threadpool
        );
    }

    private TestListener run(InferenceRunner inferenceRunner) {
        var listener = new TestListener();
        inferenceRunner.run("id", listener);
        return listener;
    }

    /**
     * When an exception is returned in a chained listener's onFailure call
     * Then InferenceRunner should wrap it in an ElasticsearchException
     */
    public void testModelLoadingServiceResponseWithAnException() {
        var expectedCause = new IllegalArgumentException("this is a test");
        doAnswer(ans -> {
            ActionListener<LocalModel> responseListener = ans.getArgument(1);
            responseListener.onFailure(expectedCause);
            return null;
        }).when(modelLoadingService).getModelForInternalInference(anyString(), any());

        var actualException = run(createInferenceRunner(mock(ExtractedFields.class))).assertFailure();
        inferenceRunnerHandledException(actualException, expectedCause);
    }

    /**
     * When an exception is thrown within InferenceRunner
     * Then InferenceRunner should wrap it in an ElasticsearchException
     */
    public void testExceptionCallingModelLoadingService() {
        var expectedCause = new IllegalArgumentException("this is a test");

        doThrow(expectedCause).when(modelLoadingService).getModelForInternalInference(anyString(), any());

        var actualException = run(createInferenceRunner(mock(ExtractedFields.class))).assertFailure();
        inferenceRunnerHandledException(actualException, expectedCause);
    }

    private void inferenceRunnerHandledException(Exception actual, Exception expectedCause) {
        assertThat(actual, instanceOf(ElasticsearchException.class));
        assertThat(actual.getCause(), is(expectedCause));
        assertThat(actual.getMessage(), equalTo("[test] failed running inference on model [id]; cause was [this is a test]"));
    }

    private Client mockClient() {
        var client = mock(Client.class);
        var threadpool = mock(ThreadPool.class);
        when(threadpool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadpool);

        Supplier<SearchResponse> withHits = () -> SearchResponseUtils.response(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
        )
            .aggregations(InternalAggregations.from(List.of(new Max(DestinationIndex.INCREMENTAL_ID, 1, DocValueFormat.RAW, Map.of()))))
            .build();
        Supplier<SearchResponse> withNoHits = () -> SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);

        when(client.search(any())).thenReturn(response(withHits)).thenReturn(response(withNoHits));
        return client;
    }

    // we only expect to call actionGet, calling other API will hang indefinitely
    private static ActionFuture<SearchResponse> response(Supplier<SearchResponse> searchResponse) {
        return new PlainActionFuture<>() {
            @Override
            public SearchResponse actionGet() {
                return searchResponse.get();
            }
        };
    }

    private static class TestListener implements ActionListener<Void> {
        private final AtomicBoolean success = new AtomicBoolean(false);
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        @Override
        public void onResponse(Void t) {
            success.set(true);
        }

        @Override
        public void onFailure(Exception e) {
            failure.set(e);
        }

        public void assertSuccess() {
            assertTrue(success.get());
        }

        public Exception assertFailure() {
            assertNotNull(failure.get());
            return failure.get();
        }
    }
}
