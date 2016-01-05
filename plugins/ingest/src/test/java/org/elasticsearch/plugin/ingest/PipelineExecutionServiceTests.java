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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineExecutionServiceTests extends ESTestCase {

    private PipelineStore store;
    private PipelineExecutionService executionService;

    @Before
    public void setup() {
        store = mock(PipelineStore.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(Runnable::run);
        executionService = new PipelineExecutionService(store, threadPool);
    }

    public void testExecutePipelineDoesNotExist() {
        when(store.get("_id")).thenReturn(null);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        try {
            executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        }
        verify(failureHandler, never()).accept(any(Throwable.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteSuccess() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", processor));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        //TODO we remove metadata, this check is not valid anymore, what do we replace it with?
        //verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.MetaData metaData : IngestDocument.MetaData.values()) {
                if (metaData == IngestDocument.MetaData.TTL) {
                    ingestDocument.setFieldValue(IngestDocument.MetaData.TTL.getFieldName(), "5w");
                } else {
                    ingestDocument.setFieldValue(metaData.getFieldName(), "update" + metaData.getFieldName());
                }

            }
            return null;
        }).when(processor).execute(any());
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", processor));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        verify(processor).execute(any());
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);

        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.parent(), equalTo("update_parent"));
        assertThat(indexRequest.timestamp(), equalTo("update_timestamp"));
        assertThat(indexRequest.ttl(), equalTo(new TimeValue(3024000000L)));
    }

    public void testExecuteFailure() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", processor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        Processor onFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(Collections.singletonList(processor), Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        //TODO we remove metadata, this check is not valid anymore, what do we replace it with?
        //verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, never()).accept(any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(true);
    }

    public void testExecuteFailureWithOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        Processor onFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(Collections.singletonList(processor), Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(onFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        Processor onFailureProcessor = mock(Processor.class);
        Processor onFailureOnFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(Collections.singletonList(processor),
            Collections.singletonList(new CompoundProcessor(Arrays.asList(onFailureProcessor),Arrays.asList(onFailureOnFailureProcessor))));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(onFailureOnFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(onFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    @SuppressWarnings("unchecked")
    public void testExecuteTTL() throws Exception {
        // test with valid ttl
        SetProcessor.Factory metaProcessorFactory = new SetProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_ttl");
        config.put("value", "5d");
        Processor processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", new CompoundProcessor(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        Consumer<Throwable> failureHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);

        assertThat(indexRequest.ttl(), equalTo(TimeValue.parseTimeValue("5d", null, "ttl")));
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);

        // test with invalid ttl
        metaProcessorFactory = new SetProcessor.Factory(TestTemplateService.instance());
        config = new HashMap<>();
        config.put("field", "_ttl");
        config.put("value", "abc");
        processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", new CompoundProcessor(processor)));

        indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        failureHandler = mock(Consumer.class);
        completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);
        verify(failureHandler, times(1)).accept(any(ElasticsearchParseException.class));
        verify(completionHandler, never()).accept(anyBoolean());

        // test with provided ttl
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", mock(CompoundProcessor.class)));

        indexRequest = new IndexRequest("_index", "_type", "_id")
                .source(Collections.emptyMap())
                .ttl(1000L);
        failureHandler = mock(Consumer.class);
        completionHandler = mock(Consumer.class);
        executionService.execute(indexRequest, "_id", failureHandler, completionHandler);

        assertThat(indexRequest.ttl(), equalTo(new TimeValue(1000L)));
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);
    }

    public void testBulkRequestExecutionWithFailures() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();

        int numRequest = scaledRandomIntBetween(8, 64);
        int numIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            ActionRequest request;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_type", "_id");
                } else {
                    request = new UpdateRequest("_index", "_type", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
                indexRequest.source("field1", "value1");
                request = indexRequest;
                numIndexRequests++;
            }
            bulkRequest.add(request);
        }

        String pipelineId = "_id";

        CompoundProcessor processor = mock(CompoundProcessor.class);
        Exception error = new RuntimeException();
        doThrow(error).when(processor).execute(any());
        when(store.get(pipelineId)).thenReturn(new Pipeline(pipelineId, null, processor));

        Consumer<Throwable> requestItemErrorHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(bulkRequest.requests(), pipelineId, requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, times(numIndexRequests)).accept(error);
        verify(completionHandler, times(1)).accept(true);
    }

    public void testBulkRequestExecution() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();

        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id");
            indexRequest.source("field1", "value1");
            bulkRequest.add(indexRequest);
        }

        String pipelineId = "_id";
        when(store.get(pipelineId)).thenReturn(new Pipeline(pipelineId, null, new CompoundProcessor()));

        Consumer<Throwable> requestItemErrorHandler = mock(Consumer.class);
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.execute(bulkRequest.requests(), pipelineId, requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);
    }

    private IngestDocument eqID(String index, String type, String id, Map<String, Object> source) {
        return Matchers.argThat(new IngestDocumentMatcher(index, type, id, source));
    }

    private class IngestDocumentMatcher extends ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        public IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, null, null, null, source);
        }

        @Override
        public boolean matches(Object o) {
            if (o.getClass() == IngestDocument.class) {
                IngestDocument otherIngestDocument = (IngestDocument) o;
                //ingest metadata will not be the same (timestamp differs every time)
                return Objects.equals(ingestDocument.getSourceAndMetadata(), otherIngestDocument.getSourceAndMetadata());
            }
            return false;
        }
    }
}
