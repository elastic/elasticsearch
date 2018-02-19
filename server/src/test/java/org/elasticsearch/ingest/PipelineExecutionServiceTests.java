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

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineExecutionServiceTests extends ESTestCase {

    private final Integer version = randomBoolean() ? randomInt() : null;
    private PipelineStore store;
    private PipelineExecutionService executionService;

    @Before
    public void setup() {
        store = mock(PipelineStore.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        executionService = new PipelineExecutionService(store, threadPool);
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        try {
            executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        }
        verify(failureHandler, never()).accept(any(Exception.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteBulkPipelineDoesNotExist() {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest1 = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 =
                new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("does_not_exist");
        bulkRequest.add(indexRequest2);
        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(bulkRequest.requests(), failureHandler, completionHandler);
        verify(failureHandler, times(1)).accept(
            argThat(new CustomTypeSafeMatcher<IndexRequest>("failure handler was not called with the expected arguments") {
                @Override
                protected boolean matchesSafely(IndexRequest item) {
                    return item == indexRequest2;
                }

            }),
            argThat(new CustomTypeSafeMatcher<IllegalArgumentException>("failure handler was not called with the expected arguments") {
                @Override
                protected boolean matchesSafely(IllegalArgumentException iae) {
                    return "pipeline with id [does_not_exist] does not exist".equals(iae.getMessage());
                }
            })
        );
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteSuccess() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);
    }

    public void testExecuteEmptyPipeline() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        when(processor.getProcessors()).thenReturn(Collections.emptyList());

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(processor, never()).execute(any());
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.MetaData metaData : IngestDocument.MetaData.values()) {
                ingestDocument.setFieldValue(metaData.getFieldName(), "update" + metaData.getFieldName());
            }
            return null;
        }).when(processor).execute(any());
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(processor).execute(any());
        verify(failureHandler, never()).accept(any());
        verify(completionHandler, times(1)).accept(true);

        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.parent(), equalTo("update_parent"));
    }

    public void testExecuteFailure() throws Exception {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock_processor_type");
        when(processor.getTag()).thenReturn("mock_processor_tag");
        Processor onFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor),
                Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(failureHandler, never()).accept(any(ElasticsearchException.class));
        verify(completionHandler, times(1)).accept(true);
    }

    public void testExecuteFailureWithOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        Processor onFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor),
                Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(onFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        Processor processor = mock(Processor.class);
        Processor onFailureProcessor = mock(Processor.class);
        Processor onFailureOnFailureProcessor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor),
            Collections.singletonList(new CompoundProcessor(false, Collections.singletonList(onFailureProcessor),
                    Collections.singletonList(onFailureOnFailureProcessor))));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(onFailureOnFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(onFailureProcessor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(any(RuntimeException.class));
        verify(completionHandler, never()).accept(anyBoolean());
    }

    public void testBulkRequestExecutionWithFailures() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        int numIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            DocWriteRequest request;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_type", "_id");
                } else {
                    request = new UpdateRequest("_index", "_type", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline(pipelineId);
                indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
                request = indexRequest;
                numIndexRequests++;
            }
            bulkRequest.add(request);
        }

        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        Exception error = new RuntimeException();
        doThrow(error).when(processor).execute(any());
        when(store.get(pipelineId)).thenReturn(new Pipeline(pipelineId, null, version, processor));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, times(numIndexRequests)).accept(any(IndexRequest.class), eq(error));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testBulkRequestExecution() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline(pipelineId);
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
            bulkRequest.add(indexRequest);
        }

        when(store.get(pipelineId)).thenReturn(new Pipeline(pipelineId, null, version, new CompoundProcessor()));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testStats() throws Exception {
        IngestStats ingestStats = executionService.stats();
        assertThat(ingestStats.getStatsPerPipeline().size(), equalTo(0));
        assertThat(ingestStats.getTotalStats().getIngestCount(), equalTo(0L));
        assertThat(ingestStats.getTotalStats().getIngestCurrent(), equalTo(0L));
        assertThat(ingestStats.getTotalStats().getIngestFailedCount(), equalTo(0L));
        assertThat(ingestStats.getTotalStats().getIngestTimeInMillis(), equalTo(0L));

        when(store.get("_id1")).thenReturn(new Pipeline("_id1", null, version, new CompoundProcessor(mock(Processor.class))));
        when(store.get("_id2")).thenReturn(new Pipeline("_id2", null, null, new CompoundProcessor(mock(Processor.class))));

        Map<String, PipelineConfiguration> configurationMap = new HashMap<>();
        configurationMap.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{}"), XContentType.JSON));
        configurationMap.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{}"), XContentType.JSON));
        executionService.updatePipelineStats(new IngestMetadata(configurationMap));

        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> completionHandler = mock(Consumer.class);

        IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1");
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        ingestStats = executionService.stats();
        assertThat(ingestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(ingestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(ingestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(0L));
        assertThat(ingestStats.getTotalStats().getIngestCount(), equalTo(1L));

        indexRequest.setPipeline("_id2");
        executionService.executeIndexRequest(indexRequest, failureHandler, completionHandler);
        ingestStats = executionService.stats();
        assertThat(ingestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(ingestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(ingestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(1L));
        assertThat(ingestStats.getTotalStats().getIngestCount(), equalTo(2L));
    }

    // issue: https://github.com/elastic/elasticsearch/issues/18126
    public void testUpdatingStatsWhenRemovingPipelineWorks() throws Exception {
        Map<String, PipelineConfiguration> configurationMap = new HashMap<>();
        configurationMap.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{}"), XContentType.JSON));
        configurationMap.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{}"), XContentType.JSON));
        executionService.updatePipelineStats(new IngestMetadata(configurationMap));
        assertThat(executionService.stats().getStatsPerPipeline(), hasKey("_id1"));
        assertThat(executionService.stats().getStatsPerPipeline(), hasKey("_id2"));

        configurationMap = new HashMap<>();
        configurationMap.put("_id3", new PipelineConfiguration("_id3", new BytesArray("{}"), XContentType.JSON));
        executionService.updatePipelineStats(new IngestMetadata(configurationMap));
        assertThat(executionService.stats().getStatsPerPipeline(), not(hasKey("_id1")));
        assertThat(executionService.stats().getStatsPerPipeline(), not(hasKey("_id2")));
    }

    private IngestDocument eqID(String index, String type, String id, Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher(index, type, id, source));
    }

    private class IngestDocumentMatcher extends ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, null, source);
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
