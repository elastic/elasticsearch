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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.meta.MetaDataProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.*;
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

    public void testExecute_pipelineDoesNotExist() {
        when(store.get("_id")).thenReturn(null);
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        PipelineExecutionService.Listener listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(listener).failed(any(IllegalArgumentException.class));
        verify(listener, times(0)).executed(any());
    }

    public void testExecuteSuccess() throws Exception {
        Processor processor = mock(Processor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Arrays.asList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        PipelineExecutionService.Listener listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener).executed(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener, times(0)).failed(any(Exception.class));
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        Processor processor = mock(Processor.class);
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.MetaData metaData : IngestDocument.MetaData.values()) {
                if (metaData == IngestDocument.MetaData.TTL) {
                    ingestDocument.setMetaData(IngestDocument.MetaData.TTL, "5w");
                } else {
                    ingestDocument.setMetaData(metaData, "update" + metaData.getFieldName());
                }

            }
            return null;
        }).when(processor).execute(any());
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Arrays.asList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        PipelineExecutionService.Listener listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(processor).execute(any());
        verify(listener).executed(any());
        verify(listener, times(0)).failed(any(Exception.class));

        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.parent(), equalTo("update_parent"));
        assertThat(indexRequest.timestamp(), equalTo("update_timestamp"));
        assertThat(indexRequest.ttl(), equalTo(3024000000l));
    }

    public void testExecute_failure() throws Exception {
        Processor processor = mock(Processor.class);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Arrays.asList(processor)));
        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        doThrow(new RuntimeException()).when(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        PipelineExecutionService.Listener listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);
        verify(processor).execute(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener, times(0)).executed(eqID("_index", "_type", "_id", Collections.emptyMap()));
        verify(listener).failed(any(RuntimeException.class));
    }

    public void testExecuteTTL() throws Exception {
        // test with valid ttl
        MetaDataProcessor.Factory metaProcessorFactory = new MetaDataProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("_ttl", "5d");
        MetaDataProcessor processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        PipelineExecutionService.Listener listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);

        assertThat(indexRequest.ttl(), equalTo(TimeValue.parseTimeValue("5d", null, "ttl").millis()));
        verify(listener, times(1)).executed(any());
        verify(listener, never()).failed(any());

        // test with invalid ttl
        metaProcessorFactory = new MetaDataProcessor.Factory();
        config = new HashMap<>();
        config.put("_ttl", "abc");
        processor = metaProcessorFactory.create(config);
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.singletonList(processor)));

        indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap());
        listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);

        verify(listener, never()).executed(any());
        verify(listener, times(1)).failed(any(ElasticsearchParseException.class));

        // test with provided ttl
        when(store.get("_id")).thenReturn(new Pipeline("_id", "_description", Collections.emptyList()));

        indexRequest = new IndexRequest("_index", "_type", "_id")
                .source(Collections.emptyMap())
                .ttl(1000l);
        listener = mock(PipelineExecutionService.Listener.class);
        executionService.execute(indexRequest, "_id", listener);

        assertThat(indexRequest.ttl(), equalTo(1000l));
        verify(listener, times(1)).executed(any());
        verify(listener, never()).failed(any(Throwable.class));
    }

    private IngestDocument eqID(String index, String type, String id, Map<String, Object> source) {
        return Matchers.argThat(new IngestDocumentMatcher(index, type, id, source));
    }

    private class IngestDocumentMatcher extends ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        public IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, source);
        }

        @Override
        public boolean matches(Object o) {
            return Objects.equals(ingestDocument, o);
        }
    }

}
