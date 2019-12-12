/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResultsPersisterServiceTests extends ESTestCase {

    private final String JOB_ID = "results_persister_test_job";
    private final Consumer<String> NULL_MSG_HANDLER = (msg) -> {};

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
        BulkResponse withFailure = new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L);
        Client client = mockClientWithResponse(withFailure, new BulkResponse(new BulkItemResponse[0], 0L));

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        ResultsPersisterService resultsPersisterService = buildResultsPersisterService(client);

        resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, NULL_MSG_HANDLER);

        ArgumentCaptor<BulkRequest> captor =  ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, times(2)).bulk(captor.capture());

        List<BulkRequest> requests = captor.getAllValues();

        assertThat(requests.get(0).numberOfActions(), equalTo(2));
        assertThat(requests.get(1).numberOfActions(), equalTo(1));
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
        BulkResponse withFailure = new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L);
        Client client = mockClientWithResponse(withFailure, new BulkResponse(new BulkItemResponse[0], 0L));

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        ResultsPersisterService resultsPersisterService = buildResultsPersisterService(client);

        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> false, NULL_MSG_HANDLER));
    }

    public void testBulkRequestRetriesConfiguredAttemptNumber() {
        IndexRequest indexRequestFail = new IndexRequest("my-index").id("fail").source(Collections.singletonMap("data", "fail"));
        BulkItemResponse failureItem = new BulkItemResponse(2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", "fail", new Exception("boom")));
        BulkResponse withFailure = new BulkResponse(new BulkItemResponse[]{ failureItem }, 0L);
        Client client = mockClientWithResponse(withFailure);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);

        ResultsPersisterService resultsPersisterService = buildResultsPersisterService(client);

        resultsPersisterService.setMaxFailureRetries(1);
        expectThrows(ElasticsearchException.class,
            () -> resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, NULL_MSG_HANDLER));
        verify(client, times(2)).bulk(any(BulkRequest.class));
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
        BulkResponse withFailure = new BulkResponse(new BulkItemResponse[]{ failureItem, successItem }, 0L);
        Client client = mockClientWithResponse(withFailure, new BulkResponse(new BulkItemResponse[0], 0L));

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequestFail);
        bulkRequest.add(indexRequestSuccess);

        ResultsPersisterService resultsPersisterService = buildResultsPersisterService(client);
        AtomicReference<String> msgHolder = new AtomicReference<>("not_called");

        resultsPersisterService.bulkIndexWithRetry(bulkRequest, JOB_ID, () -> true, msgHolder::set);

        ArgumentCaptor<BulkRequest> captor =  ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, times(2)).bulk(captor.capture());

        List<BulkRequest> requests = captor.getAllValues();

        assertThat(requests.get(0).numberOfActions(), equalTo(2));
        assertThat(requests.get(1).numberOfActions(), equalTo(1));
        assertThat(msgHolder.get(), containsString("failed to index after [1] attempts. Will attempt again in"));
    }

    @SuppressWarnings("unchecked")
    private Client mockClientWithResponse(BulkResponse... responses) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        List<ActionFuture<BulkResponse>> futures = new ArrayList<>(responses.length - 1);
        ActionFuture<BulkResponse> future1 = makeFuture(responses[0]);
        for (int i = 1; i < responses.length; i++) {
            futures.add(makeFuture(responses[i]));
        }
        when(client.bulk(any(BulkRequest.class))).thenReturn(future1, futures.toArray(ActionFuture[]::new));
        return client;
    }

    @SuppressWarnings("unchecked")
    private static ActionFuture<BulkResponse> makeFuture(BulkResponse response) {
        ActionFuture<BulkResponse> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        return future;
    }

    private ResultsPersisterService buildResultsPersisterService(Client client) {
        ThreadPool tp = mock(ThreadPool.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_META_DATA,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, tp);

        return new ResultsPersisterService(client, clusterService, Settings.EMPTY);
    }
}
