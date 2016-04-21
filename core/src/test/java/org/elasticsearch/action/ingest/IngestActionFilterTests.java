/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineExecutionService;
import org.elasticsearch.ingest.PipelineStore;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IngestActionFilterTests extends ESTestCase {

    private IngestActionFilter filter;
    private PipelineExecutionService executionService;

    @Before
    public void setup() {
        executionService = mock(PipelineExecutionService.class);
        IngestService ingestService = mock(IngestService.class);
        when(ingestService.getPipelineExecutionService()).thenReturn(executionService);
        NodeService nodeService = mock(NodeService.class);
        when(nodeService.getIngestService()).thenReturn(ingestService);
        filter = new IngestActionFilter(Settings.EMPTY, nodeService);
    }

    public void testApplyNoPipelineId() throws Exception {
        IndexRequest indexRequest = new IndexRequest();
        Task task = mock(Task.class);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed(task, IndexAction.NAME, indexRequest, actionListener);
        verifyZeroInteractions(executionService, actionFilterChain);
    }

    public void testApplyBulkNoPipelineId() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest());
        Task task = mock(Task.class);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed(task, BulkAction.NAME, bulkRequest, actionListener);
        verifyZeroInteractions(executionService, actionFilterChain);
    }

    @SuppressWarnings("unchecked")
    public void testApplyIngestIdViaRequestParam() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline("_id");
        indexRequest.source("field", "value");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);

        verify(executionService).executeIndexRequest(same(indexRequest), any(Consumer.class), any(Consumer.class));
        verifyZeroInteractions(actionFilterChain);
    }

    @SuppressWarnings("unchecked")
    public void testApplyExecuted() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline("_id");
        indexRequest.source("field", "value");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        Answer answer = invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Boolean> listener = (Consumer) invocationOnMock.getArguments()[2];
            listener.accept(true);
            return null;
        };
        doAnswer(answer).when(executionService).executeIndexRequest(any(IndexRequest.class), any(Consumer.class), any(Consumer.class));
        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);

        verify(executionService).executeIndexRequest(same(indexRequest), any(Consumer.class), any(Consumer.class));
        verify(actionFilterChain).proceed(task, IndexAction.NAME, indexRequest, actionListener);
        verifyZeroInteractions(actionListener);
    }

    @SuppressWarnings("unchecked")
    public void testApplyFailed() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline("_id");
        indexRequest.source("field", "value");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        RuntimeException exception = new RuntimeException();
        Answer answer = invocationOnMock -> {
            Consumer<Throwable> handler = (Consumer) invocationOnMock.getArguments()[1];
            handler.accept(exception);
            return null;
        };
        doAnswer(answer).when(executionService).executeIndexRequest(same(indexRequest), any(Consumer.class), any(Consumer.class));
        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);

        verify(executionService).executeIndexRequest(same(indexRequest), any(Consumer.class), any(Consumer.class));
        verify(actionListener).onFailure(exception);
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyWithBulkRequest() throws Exception {
        Task task = mock(Task.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(Runnable::run);
        PipelineStore store = mock(PipelineStore.class);

        Processor processor = new TestProcessor(ingestDocument -> ingestDocument.setFieldValue("field2", "value2"));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", new CompoundProcessor(processor)));
        executionService = new PipelineExecutionService(store, threadPool);
        IngestService ingestService = mock(IngestService.class);
        when(ingestService.getPipelineExecutionService()).thenReturn(executionService);
        NodeService nodeService = mock(NodeService.class);
        when(nodeService.getIngestService()).thenReturn(ingestService);
        filter = new IngestActionFilter(Settings.EMPTY, nodeService);

        BulkRequest bulkRequest = new BulkRequest();
        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            if (rarely()) {
                ActionRequest request;
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_type", "_id");
                } else {
                    request = new UpdateRequest("_index", "_type", "_id");
                }
                bulkRequest.add(request);
            } else {
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline("_id");
                indexRequest.source("field1", "value1");
                bulkRequest.add(indexRequest);
            }
        }

        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, actionListener, actionFilterChain);
        verify(actionFilterChain).proceed(eq(task), eq(BulkAction.NAME), eq(bulkRequest), any());
        verifyZeroInteractions(actionListener);

        int assertedRequests = 0;
        for (ActionRequest actionRequest : bulkRequest.requests()) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                assertThat(indexRequest.sourceAsMap().size(), equalTo(2));
                assertThat(indexRequest.sourceAsMap().get("field1"), equalTo("value1"));
                assertThat(indexRequest.sourceAsMap().get("field2"), equalTo("value2"));
            }
            assertedRequests++;
        }
        assertThat(assertedRequests, equalTo(numRequest));
    }

    @SuppressWarnings("unchecked")
    public void testIndexApiSinglePipelineExecution() {
        Answer answer = invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Boolean> listener = (Consumer) invocationOnMock.getArguments()[2];
            listener.accept(true);
            return null;
        };
        doAnswer(answer).when(executionService).executeIndexRequest(any(IndexRequest.class), any(Consumer.class), any(Consumer.class));

        Task task = mock(Task.class);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline("_id").source("field", "value");
        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);
        assertThat(indexRequest.getPipeline(), nullValue());
        filter.apply(task, IndexAction.NAME, indexRequest, actionListener, actionFilterChain);
        verify(executionService, times(1)).executeIndexRequest(same(indexRequest), any(Consumer.class), any(Consumer.class));
        verify(actionFilterChain, times(2)).proceed(task, IndexAction.NAME, indexRequest, actionListener);
    }
}
