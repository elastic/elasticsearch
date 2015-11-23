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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.mutate.MutateProcessor;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
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

        verify(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApplyIngestIdViaContext() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putInContext(IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
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

    public void testApply_executed() throws Exception {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
        indexRequest.source("field", "value");
        indexRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        ActionListener actionListener = mock(ActionListener.class);
        ActionFilterChain actionFilterChain = mock(ActionFilterChain.class);

        Answer answer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
                PipelineExecutionService.Listener listener = (PipelineExecutionService.Listener) invocationOnMock.getArguments()[2];
                listener.executed(ingestDocument);
                return null;
            }
        };
        doAnswer(answer).when(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verify(actionFilterChain).proceed("_action", indexRequest, actionListener);
        verifyZeroInteractions(actionListener);
    }

    public void testApply_failed() throws Exception {
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
        doAnswer(answer).when(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        filter.apply("_action", indexRequest, actionListener, actionFilterChain);

        verify(executionService).execute(any(IngestDocument.class), eq("_id"), any(PipelineExecutionService.Listener.class));
        verify(actionListener).onFailure(exception);
        verifyZeroInteractions(actionFilterChain);
    }

    public void testApply_withBulkRequest() throws Exception {
        ThreadPool threadPool = new ThreadPool(
                Settings.builder()
                        .put("name", "_name")
                        .put(PipelineExecutionService.additionalSettings(Settings.EMPTY))
                        .build()
        );
        PipelineStore store = mock(PipelineStore.class);

        Map<String, Object> mutateConfig = new HashMap<>();
        Map<String, Object> update = new HashMap<>();
        update.put("field2", "value2");
        mutateConfig.put("update", update);

        Processor mutateProcessor = (new MutateProcessor.Factory()).create(mutateConfig);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Arrays.asList(mutateProcessor)));
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

}
