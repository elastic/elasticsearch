/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ResultsPersisterServiceTests extends ESTestCase {

    private static final int MAX_FAILURE_RETRIES = 10;
    private static final String JOB_ID = "results_persister_test_job";

    private Client client;
    private OriginSettingClient originSettingClient;
    private ResultsPersisterService resultsPersisterService;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        resultsPersisterService = buildResultsPersisterService(originSettingClient);
    }

    public void testSearchWithRetries_ImmediateSuccess() {
        SearchRequest searchRequest = new SearchRequest("my-index");
        SearchResponse searchResponseOk = new SearchResponse(null, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        doAnswer(withResponse(searchResponseOk))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());

        List<String> messages = new ArrayList<>();
        SearchResponse searchResponse = resultsPersisterService.searchWithRetry(searchRequest, JOB_ID, () -> true, messages::add);
        assertThat(searchResponse, is(searchResponseOk));
        assertThat(messages, is(empty()));

        verify(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());
    }

    public void testSearchWithRetries_SuccessAfterRetry() {
        SearchRequest searchRequest = new SearchRequest("my-index");
        SearchResponse searchResponseOk = new SearchResponse(null, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
        SearchResponse searchResponseFailure = new SearchResponse(null, null, 1, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        doAnswerWithResponses(searchResponseFailure, searchResponseOk)
            .when(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());

        List<String> messages = new ArrayList<>();
        SearchResponse searchResponse = resultsPersisterService.searchWithRetry(searchRequest, JOB_ID, () -> true, messages::add);
        assertThat(searchResponse, is(searchResponseOk));
        assertThat(messages, hasSize(1));

        verify(client, times(2)).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());
    }

    public void testSearchWithRetries_FailureAfterTooManyRetries() {
        SearchRequest searchRequest = new SearchRequest("my-index");
        SearchResponse searchResponseFailure = new SearchResponse(null, null, 1, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        doAnswer(withResponse(searchResponseFailure))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());

        List<String> messages = new ArrayList<>();
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(searchRequest, JOB_ID, () -> true, messages::add));
        assertThat(e.getMessage(), containsString("failed to search after [" + (MAX_FAILURE_RETRIES + 1) + "] attempts."));
        assertThat(messages, hasSize(MAX_FAILURE_RETRIES));

        verify(client, times(MAX_FAILURE_RETRIES + 1)).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());
    }

    public void testSearchWithRetries_Failure_ShouldNotRetry() {
        SearchRequest searchRequest = new SearchRequest("my-index");
        SearchResponse searchResponseFailure = new SearchResponse(null, null, 1, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        doAnswer(withResponse(searchResponseFailure))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());

        List<String> messages = new ArrayList<>();
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(searchRequest, JOB_ID, () -> false, messages::add));
        assertThat(e.getMessage(), containsString("should not retry search after [1] attempts. SERVICE_UNAVAILABLE"));
        assertThat(messages, empty());

        verify(client, times(1)).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());
    }

    public void testSearchWithRetries_Failure_ShouldNotRetryAfterRandomNumberOfRetries() {
        SearchRequest searchRequest = new SearchRequest("my-index");
        SearchResponse searchResponseFailure = new SearchResponse(null, null, 1, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        doAnswer(withResponse(searchResponseFailure))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());

        List<String> messages = new ArrayList<>();
        int maxRetries = randomIntBetween(1, MAX_FAILURE_RETRIES);
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(searchRequest, JOB_ID, shouldRetryUntil(maxRetries), messages::add));
        assertThat(e.getMessage(), containsString("should not retry search after [" + maxRetries + "] attempts. SERVICE_UNAVAILABLE"));
        assertThat(messages, hasSize(maxRetries - 1));

        verify(client, times(maxRetries)).execute(eq(SearchAction.INSTANCE), eq(searchRequest), any());
    }

    private static Supplier<Boolean> shouldRetryUntil(int maxRetries) {
        return new Supplier<>() {
            int retries = 0;
            @Override
            public Boolean get() {
                return ++retries < maxRetries;
            }
        };
    }

    public void testBulkRequestChangeOnFailures() {
        IndexRequest indexRequestSuccess = new IndexRequest("my-index").id("success").source(Collections.singletonMap("data", "success"));
        IndexRequest indexRequestFail = new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
        BulkItemResponse successItem = new BulkItemResponse(1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared", "uuid", 1),
                indexRequestSuccess.id(),
                0,
                0,
                1,
                true));
        BulkItemResponse failureItem = new BulkItemResponse(2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, lastMessage::set);

        ArgumentCaptor<BulkRequest> captor =  ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, times(2)).execute(eq(BulkAction.INSTANCE), captor.capture(), any());

        List<BulkRequest> requests = captor.getAllValues();

        assertThat(requests.get(0).numberOfActions(), equalTo(2));
        assertThat(requests.get(1).numberOfActions(), equalTo(1));
        assertThat(lastMessage.get(), containsString("failed to index after [1] attempts. Will attempt again in"));
    }

    public void testBulkRequestDoesNotRetryWhenSupplierIsFalse() {
        IndexRequest indexRequestSuccess = new IndexRequest("my-index").id("success").source(Collections.singletonMap("data", "success"));
        IndexRequest indexRequestFail = new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
        BulkItemResponse successItem = new BulkItemResponse(1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared", "uuid", 1),
                indexRequestSuccess.id(),
                0,
                0,
                1,
                true));
        BulkItemResponse failureItem = new BulkItemResponse(2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> false, lastMessage::set));
        verify(client, times(1)).execute(eq(BulkAction.INSTANCE), any(), any());

        assertThat(lastMessage.get(), is(nullValue()));
    }

    public void testBulkRequestRetriesConfiguredAttemptNumber() {
        IndexRequest indexRequestFail = new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
        BulkItemResponse failureItem = new BulkItemResponse(2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{ failureItem }, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, lastMessage::set));
        verify(client, times(11)).execute(eq(BulkAction.INSTANCE), any(), any());

        assertThat(lastMessage.get(), containsString("failed to index after [10] attempts. Will attempt again in"));
    }

    public void testBulkRequestRetriesMsgHandlerIsCalled() {
        IndexRequest indexRequestSuccess = new IndexRequest("my-index").id("success").source(Collections.singletonMap("data", "success"));
        IndexRequest indexRequestFail = new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
        BulkItemResponse successItem = new BulkItemResponse(1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared", "uuid", 1),
                indexRequestSuccess.id(),
                0,
                0,
                1,
                true));
        BulkItemResponse failureItem = new BulkItemResponse(2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, lastMessage::set);

        ArgumentCaptor<BulkRequest> captor =  ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, times(2)).execute(eq(BulkAction.INSTANCE), captor.capture(), any());

        List<BulkRequest> requests = captor.getAllValues();

        assertThat(requests.get(0).numberOfActions(), equalTo(2));
        assertThat(requests.get(1).numberOfActions(), equalTo(1));
        assertThat(lastMessage.get(), containsString("failed to index after [1] attempts. Will attempt again in"));
    }

    private static <Response> Stubber doAnswerWithResponses(Response response1, Response response2) {
        return doAnswer(withResponse(response1)).doAnswer(withResponse(response2));
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static ResultsPersisterService buildResultsPersisterService(OriginSettingClient client) {
        CheckedConsumer<Integer, InterruptedException> sleeper = millis -> {};
        ThreadPool tp = mock(ThreadPool.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_META_DATA,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, tp);

        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(sleeper, client, clusterService, Settings.EMPTY);
        resultsPersisterService.setMaxFailureRetries(MAX_FAILURE_RETRIES);
        return resultsPersisterService;
    }
}
