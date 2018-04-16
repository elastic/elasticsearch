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

import org.apache.lucene.util.SetOnce;
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
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
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
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");

        final SetOnce<Boolean> failure = new SetOnce<>();
        final BiConsumer<IndexRequest, Exception> failureHandler = (request, e) -> {
            failure.set(true);
            assertThat(request, sameInstance(indexRequest));
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        };

        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteIndexPipelineExistsButFailedParsing() {
        when(store.get("_id")).thenReturn(new Pipeline("_id", "stub", null,
                new CompoundProcessor(new AbstractProcessor("mock") {
            @Override
            public void execute(IngestDocument ingestDocument) {
                throw new IllegalStateException("error");
            }

            @Override
            public String getType() {
                return null;
            }
        })));

        final SetOnce<Boolean> failure = new SetOnce<>();
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        final BiConsumer<IndexRequest, Exception> failureHandler = (request, e) -> {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getCause().getCause(), instanceOf(IllegalStateException.class));
            assertThat(e.getCause().getCause().getMessage(), equalTo("error"));
            failure.set(true);
        };

        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(null);
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

    public void testExecuteSuccess() {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteEmptyPipeline() throws Exception {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        when(processor.getProcessors()).thenReturn(Collections.emptyList());

        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(processor, never()).execute(any());
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        final long newVersion = randomLong();
        final String versionType = randomFrom("internal", "external", "external_gt", "external_gte");
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.MetaData metaData : IngestDocument.MetaData.values()) {
                if (metaData == IngestDocument.MetaData.VERSION) {
                    ingestDocument.setFieldValue(metaData.getFieldName(), newVersion);
                } else if (metaData == IngestDocument.MetaData.VERSION_TYPE) {
                    ingestDocument.setFieldValue(metaData.getFieldName(), versionType);
                } else {
                    ingestDocument.setFieldValue(metaData.getFieldName(), "update" + metaData.getFieldName());
                }
            }
            return null;
        }).when(processor).execute(any());
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));

        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(processor).execute(any());
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.version(), equalTo(newVersion));
        assertThat(indexRequest.versionType(), equalTo(VersionType.fromString(versionType)));
    }

    public void testExecuteFailure() throws Exception {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, processor));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
                .when(processor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(eq(indexRequest), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock_processor_type");
        when(processor.getTag()).thenReturn("mock_processor_tag");
        final Processor onFailureProcessor = mock(Processor.class);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
                false, Collections.singletonList(processor), Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqIndexTypeId(Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(failureHandler, never()).accept(eq(indexRequest), any(ElasticsearchException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteFailureWithOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor onFailureProcessor = mock(Processor.class);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
                false, Collections.singletonList(processor), Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
                .when(processor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        doThrow(new RuntimeException())
                .when(onFailureProcessor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(eq(indexRequest), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor onFailureProcessor = mock(Processor.class);
        final Processor onFailureOnFailureProcessor = mock(Processor.class);
        final List<Processor> processors = Collections.singletonList(onFailureProcessor);
        final List<Processor> onFailureProcessors = Collections.singletonList(onFailureOnFailureProcessor);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
                false,
                Collections.singletonList(processor),
                Collections.singletonList(new CompoundProcessor(false, processors, onFailureProcessors)));
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", version, compoundProcessor));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
                .when(onFailureOnFailureProcessor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        doThrow(new RuntimeException())
                .when(onFailureProcessor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        doThrow(new RuntimeException())
                .when(processor)
                .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Collections.emptyMap()));
        verify(failureHandler, times(1)).accept(eq(indexRequest), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(null);
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

    public void testBulkRequestExecution() {
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

    public void testStats() {
        final IngestStats initialStats = executionService.stats();
        assertThat(initialStats.getStatsPerPipeline().size(), equalTo(0));
        assertThat(initialStats.getTotalStats().getIngestCount(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestCurrent(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestFailedCount(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestTimeInMillis(), equalTo(0L));

        when(store.get("_id1")).thenReturn(new Pipeline("_id1", null, version, new CompoundProcessor(mock(Processor.class))));
        when(store.get("_id2")).thenReturn(new Pipeline("_id2", null, null, new CompoundProcessor(mock(Processor.class))));

        final Map<String, PipelineConfiguration> configurationMap = new HashMap<>();
        configurationMap.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{}"), XContentType.JSON));
        configurationMap.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{}"), XContentType.JSON));
        executionService.updatePipelineStats(new IngestMetadata(configurationMap));

        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1");
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        final IngestStats afterFirstRequestStats = executionService.stats();
        assertThat(afterFirstRequestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(afterFirstRequestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(afterFirstRequestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(0L));
        assertThat(afterFirstRequestStats.getTotalStats().getIngestCount(), equalTo(1L));

        indexRequest.setPipeline("_id2");
        executionService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        final IngestStats afterSecondRequestStats = executionService.stats();
        assertThat(afterSecondRequestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(afterSecondRequestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(afterSecondRequestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(1L));
        assertThat(afterSecondRequestStats.getTotalStats().getIngestCount(), equalTo(2L));
    }

    // issue: https://github.com/elastic/elasticsearch/issues/18126
    public void testUpdatingStatsWhenRemovingPipelineWorks() {
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

    private IngestDocument eqIndexTypeId(final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", source));
    }

    private IngestDocument eqIndexTypeId(final Long version, final VersionType versionType, final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", version, versionType, source));
    }

    private class IngestDocumentMatcher extends ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, null, null, source);
        }

        IngestDocumentMatcher(String index, String type, String id, Long version, VersionType versionType, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, version, versionType, source);
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
