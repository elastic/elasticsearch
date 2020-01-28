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
import org.elasticsearch.index.IndexNotFoundException;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ResultsPersisterServiceTests extends ESTestCase {

    // Common constants
    private static final String JOB_ID = "results_persister_test_job";

    // Constants for searchWithRetry tests
    private static final SearchRequest SEARCH_REQUEST = new SearchRequest("my-index");
    private static final SearchResponse SEARCH_RESPONSE_SUCCESS =
        new SearchResponse(null, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
    private static final SearchResponse SEARCH_RESPONSE_FAILURE =
        new SearchResponse(null, null, 1, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

    // Constants for bulkIndexWithRetry tests
    private static final IndexRequest INDEX_REQUEST_SUCCESS =
        new IndexRequest("my-index").id("success").source(Collections.singletonMap("data", "success"));
    private static final IndexRequest INDEX_REQUEST_FAILURE =
        new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
    private static final BulkItemResponse BULK_ITEM_RESPONSE_SUCCESS =
        new BulkItemResponse(
            1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared", "uuid", 1),
                INDEX_REQUEST_SUCCESS.id(),
                0,
                0,
                1,
                true));
    private static final BulkItemResponse BULK_ITEM_RESPONSE_FAILURE =
        new BulkItemResponse(
            2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));

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
        doAnswer(withResponse(SEARCH_RESPONSE_SUCCESS))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        List<String> messages = new ArrayList<>();
        SearchResponse searchResponse = resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, () -> true, messages::add);
        assertThat(searchResponse, is(SEARCH_RESPONSE_SUCCESS));
        assertThat(messages, is(empty()));

        verify(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    public void testSearchWithRetries_SuccessAfterRetry() {
        doAnswerWithResponses(SEARCH_RESPONSE_FAILURE, SEARCH_RESPONSE_SUCCESS)
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        List<String> messages = new ArrayList<>();
        SearchResponse searchResponse = resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, () -> true, messages::add);
        assertThat(searchResponse, is(SEARCH_RESPONSE_SUCCESS));
        assertThat(messages, hasSize(1));

        verify(client, times(2)).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    public void testSearchWithRetries_SuccessAfterRetryDueToException() {
        doThrow(new IndexNotFoundException("my-index")).doAnswer(withResponse(SEARCH_RESPONSE_SUCCESS))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        List<String> messages = new ArrayList<>();
        SearchResponse searchResponse = resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, () -> true, messages::add);
        assertThat(searchResponse, is(SEARCH_RESPONSE_SUCCESS));
        assertThat(messages, hasSize(1));

        verify(client, times(2)).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    private void testSearchWithRetries_FailureAfterTooManyRetries(int maxFailureRetries) {
        resultsPersisterService.setMaxFailureRetries(maxFailureRetries);

        doAnswer(withResponse(SEARCH_RESPONSE_FAILURE))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        List<String> messages = new ArrayList<>();
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, () -> true, messages::add));
        assertThat(e.getMessage(), containsString("failed to search after [" + (maxFailureRetries + 1) + "] attempts."));
        assertThat(messages, hasSize(maxFailureRetries));

        verify(client, times(maxFailureRetries + 1)).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    public void testSearchWithRetries_FailureAfterTooManyRetries_0() {
        testSearchWithRetries_FailureAfterTooManyRetries(0);
    }

    public void testSearchWithRetries_FailureAfterTooManyRetries_1() {
        testSearchWithRetries_FailureAfterTooManyRetries(1);
    }

    public void testSearchWithRetries_FailureAfterTooManyRetries_10() {
        testSearchWithRetries_FailureAfterTooManyRetries(10);
    }

    public void testSearchWithRetries_Failure_ShouldNotRetryFromTheBeginning() {
        doAnswer(withResponse(SEARCH_RESPONSE_FAILURE))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        List<String> messages = new ArrayList<>();
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, () -> false, messages::add));
        assertThat(e.getMessage(), containsString("should not retry search after [1] attempts. SERVICE_UNAVAILABLE"));
        assertThat(messages, empty());

        verify(client, times(1)).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    public void testSearchWithRetries_Failure_ShouldNotRetryAfterRandomNumberOfRetries() {
        int maxFailureRetries = 10;
        resultsPersisterService.setMaxFailureRetries(maxFailureRetries);

        doAnswer(withResponse(SEARCH_RESPONSE_FAILURE))
            .when(client).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());

        int maxRetries = randomIntBetween(1, maxFailureRetries);
        List<String> messages = new ArrayList<>();
        ElasticsearchException e =
            expectThrows(
                ElasticsearchException.class,
                () -> resultsPersisterService.searchWithRetry(SEARCH_REQUEST, JOB_ID, shouldRetryUntil(maxRetries), messages::add));
        assertThat(
            e.getMessage(), containsString("should not retry search after [" + (maxRetries + 1) + "] attempts. SERVICE_UNAVAILABLE"));
        assertThat(messages, hasSize(maxRetries));

        verify(client, times(maxRetries + 1)).execute(eq(SearchAction.INSTANCE), eq(SEARCH_REQUEST), any());
    }

    private static Supplier<Boolean> shouldRetryUntil(int maxRetries) {
        return new Supplier<>() {
            int retries = 0;
            @Override
            public Boolean get() {
                return ++retries <= maxRetries;
            }
        };
    }

    public void testBulkRequestChangeOnFailures() {
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{BULK_ITEM_RESPONSE_FAILURE, BULK_ITEM_RESPONSE_SUCCESS}, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(INDEX_REQUEST_FAILURE);
        bulkRequest.add(INDEX_REQUEST_SUCCESS);

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
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{BULK_ITEM_RESPONSE_FAILURE, BULK_ITEM_RESPONSE_SUCCESS}, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(INDEX_REQUEST_FAILURE);
        bulkRequest.add(INDEX_REQUEST_SUCCESS);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> false, lastMessage::set));
        verify(client, times(1)).execute(eq(BulkAction.INSTANCE), any(), any());

        assertThat(lastMessage.get(), is(nullValue()));
    }

    public void testBulkRequestRetriesConfiguredAttemptNumber() {
        int maxFailureRetries = 10;
        resultsPersisterService.setMaxFailureRetries(maxFailureRetries);

        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{BULK_ITEM_RESPONSE_FAILURE}, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(INDEX_REQUEST_FAILURE);

        AtomicReference<String> lastMessage = new AtomicReference<>();

        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, lastMessage::set));
        verify(client, times(maxFailureRetries + 1)).execute(eq(BulkAction.INSTANCE), any(), any());

        assertThat(lastMessage.get(), containsString("failed to index after [10] attempts. Will attempt again in"));
    }

    public void testBulkRequestRetriesMsgHandlerIsCalled() {
        doAnswerWithResponses(
                new BulkResponse(new BulkItemResponse[]{BULK_ITEM_RESPONSE_FAILURE, BULK_ITEM_RESPONSE_SUCCESS}, 0L),
                new BulkResponse(new BulkItemResponse[0], 0L))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(INDEX_REQUEST_FAILURE);
        bulkRequest.add(INDEX_REQUEST_SUCCESS);

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

        return new ResultsPersisterService(sleeper, client, clusterService, Settings.EMPTY);
    }
}
