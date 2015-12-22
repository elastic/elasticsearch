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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.CompoundProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimulateExecutionServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private SimulateExecutionService executionService;
    private Pipeline pipeline;
    private CompoundProcessor processor;
    private IngestDocument ingestDocument;

    @Before
    public void setup() {
        threadPool = new ThreadPool(
                Settings.builder()
                        .put("name", getClass().getName())
                        .build()
        );
        executionService = new SimulateExecutionService(threadPool);
        processor = mock(CompoundProcessor.class);
        when(processor.getType()).thenReturn("mock");
        pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor, processor));
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
    }

    @After
    public void destroy() {
        threadPool.shutdown();
    }

    public void testExecuteVerboseItem() throws Exception {
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        verify(processor, times(2)).execute(ingestDocument);
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorId(), equalTo("processor[mock]-0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorId(), equalTo("processor[mock]-1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument().getSourceAndMetadata(),
            not(sameInstance(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testExecuteItem() throws Exception {
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        verify(processor, times(2)).execute(ingestDocument);
        assertThat(actualItemResponse, instanceOf(SimulateDocumentSimpleResult.class));
        SimulateDocumentSimpleResult simulateDocumentSimpleResult = (SimulateDocumentSimpleResult) actualItemResponse;
        assertThat(simulateDocumentSimpleResult.getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentSimpleResult.getFailure(), nullValue());
    }

    public void testExecuteVerboseItemWithFailure() throws Exception {
        Exception e = new RuntimeException("processor failed");
        doThrow(e).doNothing().when(processor).execute(ingestDocument);
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        verify(processor, times(2)).execute(ingestDocument);
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorId(), equalTo("processor[mock]-0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorId(), equalTo("processor[mock]-1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
        runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
    }

    public void testExecuteItemWithFailure() throws Exception {
        Exception e = new RuntimeException("processor failed");
        doThrow(e).when(processor).execute(ingestDocument);
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        verify(processor, times(1)).execute(ingestDocument);
        assertThat(actualItemResponse, instanceOf(SimulateDocumentSimpleResult.class));
        SimulateDocumentSimpleResult simulateDocumentSimpleResult = (SimulateDocumentSimpleResult) actualItemResponse;
        assertThat(simulateDocumentSimpleResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentSimpleResult.getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentSimpleResult.getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
    }
}
