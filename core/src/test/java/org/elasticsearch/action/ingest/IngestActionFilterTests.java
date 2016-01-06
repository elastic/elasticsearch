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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestBootstrapper;
import org.elasticsearch.ingest.PipelineExecutionService;
import org.elasticsearch.ingest.PipelineStore;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.Matchers;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.action.ingest.IngestActionFilter.BulkRequestModifier;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IngestActionFilterTests extends ESTestCase {

    private IngestActionFilter filter;
    private PipelineExecutionService executionService;

    @Before
    public void setup() {
        executionService = mock(PipelineExecutionService.class);
        IngestBootstrapper bootstrapper = mock(IngestBootstrapper.class);
        when(bootstrapper.getPipelineExecutionService()).thenReturn(executionService);
        filter = new IngestActionFilter(Settings.EMPTY, bootstrapper);
    }

    public void testApplyNoIngestId() throws Exception {
        IndexRequest indexRequest = new IndexRequest();
        Task task = mock(Task.class);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed(task, "_action", indexRequest, actionListener);
        verifyZeroInteractions(executionService, actionFilterChain);
    }

    public void testApplyIngestIdViaRequestParam() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(Matchers.any(IndexRequest.class), Matchers.eq("_id"), Matchers.any(Consumer.class), Matchers.any(Consumer.class));
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyIngestIdViaContext() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putInContext(ConfigurationUtils.PIPELINE_ID_PARAM_CONTEXT_KEY, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(Matchers.any(IndexRequest.class), Matchers.eq("_id"), Matchers.any(Consumer.class), Matchers.any(Consumer.class));
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyAlreadyProcessed() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
        indexRequest.putHeader(ConfigurationUtils.PIPELINE_ALREADY_PROCESSED, true);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed(task, "_action", indexRequest, actionListener);
        verifyZeroInteractions(executionService, actionListener);
    }

    public void testApplyExecuted() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        Answer answer = invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Boolean> listener = (Consumer) invocationOnMock.getArguments()[3];
            listener.accept(true);
            return null;
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(Consumer.class), any(Consumer.class));
        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IndexRequest.class), eq("_id"), any(Consumer.class), any(Consumer.class));
        verify(actionFilterChain).proceed(task, "_action", indexRequest, actionListener);
        verifyZeroInteractions(actionListener);
    }

    public void testApplyFailed() throws Exception {
        Task task = mock(Task.class);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        RuntimeException exception = new RuntimeException();
        Answer answer = invocationOnMock -> {
            Consumer<Throwable> handler = (Consumer) invocationOnMock.getArguments()[2];
            handler.accept(exception);
            return null;
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(Consumer.class), any(Consumer.class));
        filter.apply(task, "_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(Matchers.any(IndexRequest.class), Matchers.eq("_id"), Matchers.any(Consumer.class), Matchers.any(Consumer.class));
        verify(actionListener).onFailure(exception);
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyWithBulkRequest() throws Exception {
        Task task = mock(Task.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(Runnable::run);
        PipelineStore store = mock(PipelineStore.class);

        Processor processor = new Processor() {
            @Override
            public void execute(IngestDocument ingestDocument) {
                ingestDocument.setFieldValue("field2", "value2");
            }

            @Override
            public String getType() {
                return null;
            }
        };
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", new CompoundProcessor(processor)));
        executionService = new PipelineExecutionService(store, threadPool);
        IngestBootstrapper bootstrapper = mock(IngestBootstrapper.class);
        when(bootstrapper.getPipelineExecutionService()).thenReturn(executionService);
        filter = new IngestActionFilter(Settings.EMPTY, bootstrapper);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
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
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
                indexRequest.source("field1", "value1");
                bulkRequest.add(indexRequest);
            }
        }

        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply(task, "_action", bulkRequest, actionListener, actionFilterChain);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                verify(actionFilterChain).proceed(task, "_action", bulkRequest, actionListener);
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
        });
    }

    public void testBulkRequestModifier() {
        int numRequests = scaledRandomIntBetween(8, 64);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.add(new IndexRequest("_index", "_type", String.valueOf(i)).source("{}"));
        }
        CaptureActionListener actionListener = new CaptureActionListener();
        BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(bulkRequest);

        int i = 0;
        Set<Integer> failedSlots = new HashSet<>();
        while (bulkRequestModifier.hasNext()) {
            bulkRequestModifier.next();
            if (randomBoolean()) {
                bulkRequestModifier.markCurrentItemAsFailed(new RuntimeException());
                failedSlots.add(i);
            }
            i++;
        }

        assertThat(bulkRequestModifier.getBulkRequest().requests().size(), equalTo(numRequests - failedSlots.size()));
        // simulate that we actually executed the modified bulk request:
        ActionListener<BulkResponse> result = bulkRequestModifier.wrapActionListenerIfNeeded(actionListener);
        result.onResponse(new BulkResponse(new BulkItemResponse[numRequests - failedSlots.size()], 0));

        BulkResponse bulkResponse = actionListener.getResponse();
        for (int j = 0; j < bulkResponse.getItems().length; j++) {
            if (failedSlots.contains(j)) {
                BulkItemResponse item =  bulkResponse.getItems()[j];
                assertThat(item.isFailed(), is(true));
                assertThat(item.getFailure().getIndex(), equalTo("_index"));
                assertThat(item.getFailure().getType(), equalTo("_type"));
                assertThat(item.getFailure().getId(), equalTo(String.valueOf(j)));
                assertThat(item.getFailure().getMessage(), equalTo("java.lang.RuntimeException"));
            } else {
                assertThat(bulkResponse.getItems()[j], nullValue());
            }
        }
    }

    private final static class CaptureActionListener implements ActionListener<BulkResponse> {

        private BulkResponse response;

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            this.response = bulkItemResponses ;
        }

        @Override
        public void onFailure(Throwable e) {
        }

        public BulkResponse getResponse() {
            return response;
        }
    }

}
