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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.set.SetProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
        @SuppressWarnings("unchecked")
        ActionListener<Void> listener = (ActionListener<Void>)mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(listener).onFailure(any(IllegalArgumentException.class));
        verify(listener, times(0)).onResponse(any());
    }

    public void testExecuteSuccess() throws Exception {
        Processor processor = mock(Processor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        @SuppressWarnings("unchecked")
        ActionListener<Void> listener = (ActionListener<Void>)mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);
        //TODO we remove metadata, this check is not valid anymore, what do we replace it with?
        //verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener).onResponse(null);
        verify(listener, times(0)).onFailure(any(Exception.class));
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        Processor processor = mock(Processor.class);
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
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        @SuppressWarnings("unchecked")
        ActionListener<Void> listener = (ActionListener<Void>)mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(processor).execute(any());
        verify(listener).onResponse(any());
        verify(listener, times(0)).onFailure(any(Exception.class));

        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.parent(), equalTo("update_parent"));
        assertThat(indexRequest.timestamp(), equalTo("update_timestamp"));
        assertThat(indexRequest.ttl(), equalTo(new TimeValue(3024000000L)));
    }

    public void testExecuteFailure() throws Exception {
        Processor processor = mock(Processor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        @SuppressWarnings("unchecked")
        ActionListener<Void> listener = (ActionListener<Void>)mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener, times(0)).onResponse(null);
        verify(listener).onFailure(any(RuntimeException.class));
    }

    @SuppressWarnings("unchecked")
    public void testExecuteTTL() throws Exception {
        // test with valid ttl
        SetProcessor.Factory metaProcessorFactory = new SetProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_ttl");
        config.put("value", "5d");
        Processor processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        ActionListener<Void> listener = (ActionListener<Void>)mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);

        assertThat(indexRequest.ttl(), equalTo(TimeValue.parseTimeValue("5d", null, "ttl")));
        verify(listener, times(1)).onResponse(any());
        verify(listener, never()).onFailure(any());

        // test with invalid ttl
        metaProcessorFactory = new SetProcessor.Factory(TestTemplateService.instance());
        config = new HashMap<>();
        config.put("field", "_ttl");
        config.put("value", "abc");
        processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        listener = mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);

        verify(listener, never()).onResponse(any());
        verify(listener, times(1)).onFailure(any(ElasticsearchParseException.class));

        // test with provided ttl
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.emptyList()));

        indexRequest = new IndexRequest("_index", "_type", "_id")
                .source(Collections.emptyMap())
                .ttl(1000L);
        listener = mock(ActionListener.class);
        executionService.execute(indexRequest, "_id", listener);

        assertThat(indexRequest.ttl(), equalTo(new TimeValue(1000L)));
        verify(listener, times(1)).onResponse(any());
        verify(listener, never()).onFailure(any(Throwable.class));
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
