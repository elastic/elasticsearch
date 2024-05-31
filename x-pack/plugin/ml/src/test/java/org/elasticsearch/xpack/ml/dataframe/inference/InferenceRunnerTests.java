/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
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
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
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

        createInferenceRunner(extractedFields, testDocsIterator).run("model id");

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

        inferenceRunner.run("model id");

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

    private InferenceRunner createInferenceRunner(ExtractedFields extractedFields, TestDocsIterator testDocsIterator) {
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
            id -> testDocsIterator
        );
    }

    private Client mockClient() {
        var client = mock(Client.class);
        var threadpool = mock(ThreadPool.class);
        when(threadpool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadpool);

        Supplier<SearchResponse> withHits = () -> new SearchResponse(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
            InternalAggregations.from(List.of(new Max(DestinationIndex.INCREMENTAL_ID, 1, DocValueFormat.RAW, Map.of()))),
            new Suggest(new ArrayList<>()),
            false,
            false,
            new SearchProfileResults(Map.of()),
            1,
            "",
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        Supplier<SearchResponse> withNoHits = () -> new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            // Simulate completely null aggs
            null,
            new Suggest(new ArrayList<>()),
            false,
            false,
            new SearchProfileResults(Map.of()),
            1,
            "",
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

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
}
