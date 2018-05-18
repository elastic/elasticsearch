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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SimulateExecutionServiceTests extends ESTestCase {

    private final Integer version = randomBoolean() ? randomInt() : null;

    private ThreadPool threadPool;
    private SimulateExecutionService executionService;
    private IngestDocument ingestDocument;

    @Before
    public void setup() {
        threadPool = new ThreadPool(
                Settings.builder()
                        .put("node.name", getClass().getName())
                        .build()
        );
        executionService = new SimulateExecutionService(threadPool);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
    }

    @After
    public void destroy() {
        threadPool.shutdown();
    }

    public void testExecuteVerboseItem() throws Exception {
        TestProcessor processor = new TestProcessor("test-id", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("test-id"));
        IngestDocument firstProcessorIngestDocument = simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument();
        assertThat(firstProcessorIngestDocument, not(sameInstance(this.ingestDocument)));
        assertIngestDocument(firstProcessorIngestDocument, this.ingestDocument);
        assertThat(firstProcessorIngestDocument.getSourceAndMetadata(), not(sameInstance(this.ingestDocument.getSourceAndMetadata())));

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("test-id"));
        IngestDocument secondProcessorIngestDocument = simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument();
        assertThat(secondProcessorIngestDocument, not(sameInstance(this.ingestDocument)));
        assertIngestDocument(secondProcessorIngestDocument, this.ingestDocument);
        assertThat(secondProcessorIngestDocument.getSourceAndMetadata(), not(sameInstance(this.ingestDocument.getSourceAndMetadata())));
        assertThat(secondProcessorIngestDocument.getSourceAndMetadata(),
            not(sameInstance(firstProcessorIngestDocument.getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testExecuteItem() throws Exception {
        TestProcessor processor = new TestProcessor("processor_0", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());
    }

    public void testExecuteVerboseItemExceptionWithoutOnFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("processor_0", "mock", ingestDocument -> {});
        TestProcessor processor2 = new TestProcessor("processor_1", "mock", ingestDocument -> { throw new RuntimeException("processor failed"); });
        TestProcessor processor3 = new TestProcessor("processor_2", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2, processor3));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(processor3.getInvokedCounter(), equalTo(0));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertIngestDocument(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("processor_1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
    }

    public void testExecuteVerboseItemWithOnFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("processor_0", "mock", ingestDocument -> { throw new RuntimeException("processor failed"); });
        TestProcessor processor2 = new TestProcessor("processor_1", "mock", ingestDocument -> {});
        TestProcessor processor3 = new TestProcessor("processor_2", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version,
                new CompoundProcessor(new CompoundProcessor(false, Collections.singletonList(processor1),
                                Collections.singletonList(processor2)), processor3));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(3));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("processor_1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), not(sameInstance(ingestDocument)));

        IngestDocument ingestDocumentWithOnFailureMetadata = new IngestDocument(ingestDocument);
        Map<String, Object> metadata = ingestDocumentWithOnFailureMetadata.getIngestMetadata();
        metadata.put(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD, "mock");
        metadata.put(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD, "processor_0");
        metadata.put(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD, "processor failed");
        assertIngestDocument(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), ingestDocumentWithOnFailureMetadata);

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(2).getProcessorTag(), equalTo("processor_2"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(2).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertIngestDocument(simulateDocumentVerboseResult.getProcessorResults().get(2).getIngestDocument(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(2).getFailure(), nullValue());
    }

    public void testExecuteVerboseItemExceptionWithIgnoreFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor("processor_0", "mock", ingestDocument -> { throw exception; });
        CompoundProcessor processor = new CompoundProcessor(true, Collections.singletonList(testProcessor), Collections.emptyList());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(1));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), sameInstance(exception));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertIngestDocument(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));
    }

    public void testExecuteVerboseItemWithoutExceptionAndWithIgnoreFailure() throws Exception {
        TestProcessor testProcessor = new TestProcessor("processor_0", "mock", ingestDocument -> { });
        CompoundProcessor processor = new CompoundProcessor(true, Collections.singletonList(testProcessor), Collections.emptyList());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, true);
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(1));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), not(sameInstance(ingestDocument)));
        assertIngestDocument(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata(), not(sameInstance(ingestDocument.getSourceAndMetadata())));
    }

    public void testExecuteItemWithFailure() throws Exception {
        TestProcessor processor = new TestProcessor(ingestDocument -> { throw new RuntimeException("processor failed"); });
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor, processor));
        SimulateDocumentResult actualItemResponse = executionService.executeDocument(pipeline, ingestDocument, false);
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentBaseResult.getFailure(), instanceOf(RuntimeException.class));
        Exception exception = simulateDocumentBaseResult.getFailure();
        assertThat(exception, instanceOf(ElasticsearchException.class));
        assertThat(exception.getMessage(), equalTo("java.lang.IllegalArgumentException: java.lang.RuntimeException: processor failed"));
    }
}
