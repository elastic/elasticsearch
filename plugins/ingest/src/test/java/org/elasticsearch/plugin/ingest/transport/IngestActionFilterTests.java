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

package org.elasticsearch.plugin.ingest.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.plugin.ingest.transport.IngestActionFilter.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class IngestActionFilterTests extends ESTestCase {

    private IngestActionFilter filter;
    private PipelineExecutionService executionService;

    @Before
    public void setup() {
        executionService = mock(PipelineExecutionService.class);
        filter = new IngestActionFilter(Settings.EMPTY, executionService);
    }

    public void testApplyNoIngestId() throws Exception {
        IndexRequest indexRequest = new IndexRequest();
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed("_action", indexRequest, actionListener);
        verifyZeroInteractions(executionService, actionFilterChain);
    }

    public void testApplyIngestIdViaRequestParam() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyIngestIdViaContext() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putInContext(IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyAlreadyProcessed() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ALREADY_PROCESSED, true);
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(actionFilterChain).proceed("_action", indexRequest, actionListener);
        verifyZeroInteractions(executionService, actionListener);
    }

    public void testApplyExecuted() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        Answer answer = invocationOnMock -> {
            PipelineExecutionService.Listener listener = (PipelineExecutionService.Listener) invocationOnMock.getArguments()[2];
            listener.executed(new IngestDocument(indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.sourceAsMap()));
            return null;
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verify(actionFilterChain).proceed("_action", indexRequest, actionListener);
        verifyZeroInteractions(actionListener);
    }

    public void testApplyFailed() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        RuntimeException exception = new RuntimeException();
        Answer answer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                PipelineExecutionService.Listener listener = (PipelineExecutionService.Listener) invocationOnMock.getArguments()[2];
                listener.failed(exception);
                return null;
            }
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verify(actionListener).onFailure(exception);
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyWithBulkRequest() throws Exception {
        ThreadPool threadPool = new ThreadPool(
                Settings.builder()
                        .put("name", "_name")
                        .put(PipelineExecutionService.additionalSettings(Settings.EMPTY))
                        .build()
        );
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
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));
        executionService = new PipelineExecutionService(store, threadPool);
        filter = new IngestActionFilter(Settings.EMPTY, executionService);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
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

        filter.apply("_action", bulkRequest, actionListener, actionFilterChain);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                verify(actionFilterChain).proceed("_action", bulkRequest, actionListener);
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

        threadPool.shutdown();
    }


    public void testApplyWithBulkRequestWithFailureAllFailed() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        int numRequest = scaledRandomIntBetween(0, 8);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
            indexRequest.source("field1", "value1");
            bulkRequest.add(indexRequest);
        }

        RuntimeException exception = new RuntimeException();
        Answer answer = (invocationOnMock) -> {
            PipelineExecutionService.Listener listener = (PipelineExecutionService.Listener) invocationOnMock.getArguments()[2];
            listener.failed(exception);
            return null;
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));

        CaptureActionListener actionListener = new CaptureActionListener();
        RecordRequestAFC actionFilterChain = new RecordRequestAFC();

        filter.apply("_action", bulkRequest, actionListener, actionFilterChain);

        assertThat(actionFilterChain.request, nullValue());
        ActionResponse response = actionListener.response;
        assertThat(response, instanceOf(BulkResponse.class));
        BulkResponse bulkResponse = (BulkResponse) response;
        assertThat(bulkResponse.getItems().length, equalTo(numRequest));
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            assertThat(bulkItemResponse.isFailed(), equalTo(true));
        }
    }

    public void testApplyWithBulkRequestWithFailure() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        int numRequest = scaledRandomIntBetween(8, 64);
        int numNonIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            ActionRequest request;
            if (randomBoolean()) {
                numNonIndexRequests++;
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_type", "_id");
                } else {
                    request = new UpdateRequest("_index", "_type", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
                indexRequest.source("field1", "value1");
                request = indexRequest;
            }
            bulkRequest.add(request);
        }

        RuntimeException exception = new RuntimeException();
        Answer answer = (invocationOnMock) -> {
            PipelineExecutionService.Listener listener = (PipelineExecutionService.Listener) invocationOnMock.getArguments()[2];
            listener.failed(exception);
            return null;
        };
        doAnswer(answer).when(executionService).execute(any(IndexRequest.class), eq("_id"), any(PipelineExecutionService.Listener.class));

        ActionListener actionListener = mock(ActionListener.class);
        RecordRequestAFC actionFilterChain = new RecordRequestAFC();

        filter.apply("_action", bulkRequest, actionListener, actionFilterChain);

        BulkRequest interceptedRequests = actionFilterChain.getRequest();
        assertThat(interceptedRequests.requests().size(), equalTo(numNonIndexRequests));

        verifyZeroInteractions(actionListener);
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

    private final static class RecordRequestAFC implements ActionFilterChain {

        private ActionRequest request;

        @Override
        public void proceed(String action, ActionRequest request, ActionListener listener) {
            this.request = request;
        }

        @Override
        public void proceed(String action, ActionResponse response, ActionListener listener) {

        }

        @SuppressWarnings("unchecked")
        public <T extends ActionRequest<T>> T getRequest() {
            return (T) request;
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
