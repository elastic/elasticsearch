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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SimulateExecutionServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private SimulateExecutionService executionService;
    private Pipeline pipeline;
    private Processor processor;
    private IngestDocument ingestDocument;

    @Before
    public void setup() {
        threadPool = new ThreadPool(
                Settings.builder()
                        .put("node.name", getClass().getName())
                        .build()
        );
        executionService = new SimulateExecutionService(threadPool);
        processor = new TestProcessor("id", "mock", ingestDocument -> {});
        pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor, processor));
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
    }

    @After
    public void destroy() {
        threadPool.shutdown();
    }

    public void testExecuteVerboseDocumentSimple() throws Exception {
        List<SimulateProcessorResult> processorResultList = new ArrayList<>();
        executionService.executeVerboseDocument(processor, ingestDocument, processorResultList);
        SimulateProcessorResult result = new SimulateProcessorResult("id", ingestDocument);
        assertThat(processorResultList.size(), equalTo(1));
        assertThat(processorResultList.get(0).getProcessorTag(), equalTo(result.getProcessorTag()));
        assertThat(processorResultList.get(0).getIngestDocument(), equalTo(result.getIngestDocument()));
        assertThat(processorResultList.get(0).getFailure(), nullValue());
    }

    public void testExecuteVerboseDocumentSimpleException() throws Exception {
        RuntimeException exception = new RuntimeException("mock_exception");
        TestProcessor processor = new TestProcessor("id", "mock", ingestDocument -> { throw exception; });
        List<SimulateProcessorResult> processorResultList = new ArrayList<>();
        try {
            executionService.executeVerboseDocument(processor, ingestDocument, processorResultList);
            fail("should throw exception");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), equalTo("mock_exception"));
        }
        SimulateProcessorResult result = new SimulateProcessorResult("id", exception);
        assertThat(processorResultList.size(), equalTo(1));
        assertThat(processorResultList.get(0).getProcessorTag(), equalTo(result.getProcessorTag()));
        assertThat(processorResultList.get(0).getFailure(), equalTo(result.getFailure()));
    }

    public void testExecuteVerboseDocumentCompoundSuccess() throws Exception {
        TestProcessor processor1 = new TestProcessor("p1", "mock", ingestDocument -> { });
        TestProcessor processor2 = new TestProcessor("p2", "mock", ingestDocument -> { });

        Processor compoundProcessor = new CompoundProcessor(processor1, processor2);
        List<SimulateProcessorResult> processorResultList = new ArrayList<>();
        executionService.executeVerboseDocument(compoundProcessor, ingestDocument, processorResultList);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(processorResultList.size(), equalTo(2));
        assertThat(processorResultList.get(0).getProcessorTag(), equalTo("p1"));
        assertThat(processorResultList.get(0).getIngestDocument(), equalTo(ingestDocument));
        assertThat(processorResultList.get(0).getFailure(), nullValue());
        assertThat(processorResultList.get(1).getProcessorTag(), equalTo("p2"));
        assertThat(processorResultList.get(1).getIngestDocument(), equalTo(ingestDocument));
        assertThat(processorResultList.get(1).getFailure(), nullValue());
    }

    public void testExecuteVerboseDocumentCompoundOnFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("p1", "mock", ingestDocument -> { });
        TestProcessor processor2 = new TestProcessor("p2", "mock", ingestDocument -> { throw new RuntimeException("p2_exception"); });
        TestProcessor onFailureProcessor1 = new TestProcessor("fail_p1", "mock", ingestDocument -> { });
        TestProcessor onFailureProcessor2 = new TestProcessor("fail_p2", "mock", ingestDocument -> { throw new RuntimeException("fail_p2_exception"); });
        TestProcessor onFailureProcessor3 = new TestProcessor("fail_p3", "mock", ingestDocument -> { });
        CompoundProcessor onFailureCompoundProcessor = new CompoundProcessor(Collections.singletonList(onFailureProcessor2), Collections.singletonList(onFailureProcessor3));

        Processor compoundProcessor = new CompoundProcessor(Arrays.asList(processor1, processor2), Arrays.asList(onFailureProcessor1, onFailureCompoundProcessor));
        List<SimulateProcessorResult> processorResultList = new ArrayList<>();
        executionService.executeVerboseDocument(compoundProcessor, ingestDocument, processorResultList);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(onFailureProcessor1.getInvokedCounter(), equalTo(1));
        assertThat(onFailureProcessor2.getInvokedCounter(), equalTo(1));
        assertThat(onFailureProcessor3.getInvokedCounter(), equalTo(1));
        assertThat(processorResultList.size(), equalTo(5));
        assertThat(processorResultList.get(0).getProcessorTag(), equalTo("p1"));
        assertThat(processorResultList.get(1).getProcessorTag(), equalTo("p2"));
        assertThat(processorResultList.get(2).getProcessorTag(), equalTo("fail_p1"));
        assertThat(processorResultList.get(3).getProcessorTag(), equalTo("fail_p2"));
        assertThat(processorResultList.get(4).getProcessorTag(), equalTo("fail_p3"));
    }

    public void testExecuteVerboseItem() throws Exception {
        TestProcessor processor = new TestProcessor("test-id", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("test-id"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("test-id"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument().getSourceAndMetadata(),
            not(sameInstance(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testExecuteItem() throws Exception {
        TestProcessor processor = new TestProcessor("processor_0", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());
    }

    public void testExecuteVerboseItemWithFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("processor_0", "mock", ingestDocument -> { throw new RuntimeException("processor failed"); });
        TestProcessor processor2 = new TestProcessor("processor_1", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(Collections.singletonList(processor1), Collections.singletonList(processor2)));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("processor_1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testExecuteItemWithFailure() throws Exception {
        TestProcessor processor = new TestProcessor(ingestDocument -> { throw new RuntimeException("processor failed"); });
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentBaseResult.getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentBaseResult.getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
    }
}
