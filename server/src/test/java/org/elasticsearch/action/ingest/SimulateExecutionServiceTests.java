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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.DropProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestProcessorException;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SimulateExecutionServiceTests extends ESTestCase {

    private final Integer version = randomBoolean() ? randomInt() : null;

    private TestThreadPool threadPool;
    private SimulateExecutionService executionService;
    private IngestDocument ingestDocument;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(SimulateExecutionServiceTests.class.getSimpleName());
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

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.getAcquire();
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("test-id"));
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(0), pipeline.getId(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("test-id"));
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(1), pipeline.getId(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument().getSourceAndMetadata(),
            not(sameInstance(simulateDocumentVerboseResult.getProcessorResults().get(0).getIngestDocument().getSourceAndMetadata())));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }
    public void testExecuteItem() throws Exception {
        TestProcessor processor = new TestProcessor("processor_0", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor, processor));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, false, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.getAcquire();
        assertThat(processor.getInvokedCounter(), equalTo(2));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), equalTo(ingestDocument));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());
    }

    public void testExecuteVerboseItemExceptionWithoutOnFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("processor_0", "mock", ingestDocument -> {});
        TestProcessor processor2 = new TestProcessor("processor_1", "mock", new RuntimeException("processor failed"));
        TestProcessor processor3 = new TestProcessor("processor_2", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2, processor3));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(processor3.getInvokedCounter(), equalTo(0));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(0), pipeline.getId(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getProcessorTag(), equalTo("processor_1"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getIngestDocument(), nullValue());
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure();
        assertThat(runtimeException.getMessage(), equalTo("processor failed"));
    }

    public void testExecuteVerboseItemWithOnFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor("processor_0", "mock", new RuntimeException("processor failed"));
        TestProcessor processor2 = new TestProcessor("processor_1", "mock", ingestDocument -> {});
        TestProcessor processor3 = new TestProcessor("processor_2", "mock", ingestDocument -> {});
        Pipeline pipeline = new Pipeline("_id", "_description", version,
                new CompoundProcessor(new CompoundProcessor(false, Collections.singletonList(processor1),
                                Collections.singletonList(processor2)), processor3));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
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
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(1), pipeline.getId(),
            ingestDocumentWithOnFailureMetadata);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(1).getFailure(), nullValue());

        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(2).getProcessorTag(), equalTo("processor_2"));
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(2), pipeline.getId(), ingestDocument);
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(2).getFailure(), nullValue());
    }

    public void testExecuteVerboseItemExceptionWithIgnoreFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor("processor_0", "mock", exception);
        CompoundProcessor processor = new CompoundProcessor(true, Collections.singletonList(testProcessor), Collections.emptyList());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(1));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), sameInstance(exception));
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(0), pipeline.getId(), ingestDocument);
    }

    public void testExecuteVerboseItemWithoutExceptionAndWithIgnoreFailure() throws Exception {
        TestProcessor testProcessor = new TestProcessor("processor_0", "mock", ingestDocument -> { });
        CompoundProcessor processor = new CompoundProcessor(true, Collections.singletonList(testProcessor), Collections.emptyList());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(1));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getProcessorTag(), equalTo("processor_0"));
        assertThat(simulateDocumentVerboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertVerboseResult(simulateDocumentVerboseResult.getProcessorResults().get(0), pipeline.getId(), ingestDocument);
    }

    public void testExecuteItemWithFailure() throws Exception {
        TestProcessor processor = new TestProcessor(ingestDocument -> { throw new RuntimeException("processor failed"); });
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor, processor));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, false, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentBaseResult.getFailure(), instanceOf(RuntimeException.class));
        Exception exception = simulateDocumentBaseResult.getFailure();
        assertThat(exception, instanceOf(IngestProcessorException.class));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: processor failed"));
    }

    public void testDropDocument() throws Exception {
        TestProcessor processor1 = new TestProcessor(ingestDocument -> ingestDocument.setFieldValue("field", "value"));
        Processor processor2 = new DropProcessor.Factory().create(Map.of(), null, Map.of());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, false, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) actualItemResponse;
        assertThat(simulateDocumentBaseResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());
    }

    public void testDropDocumentVerbose() throws Exception {
        TestProcessor processor1 = new TestProcessor(ingestDocument -> ingestDocument.setFieldValue("field", "value"));
        Processor processor2 = new DropProcessor.Factory().create(Map.of(), null, Map.of());
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult verboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(verboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(verboseResult.getProcessorResults().get(0).getIngestDocument(), notNullValue());
        assertThat(verboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(verboseResult.getProcessorResults().get(1).getIngestDocument(), nullValue());
        assertThat(verboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testDropDocumentVerboseExtraProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor(ingestDocument -> ingestDocument.setFieldValue("field1", "value"));
        Processor processor2 = new DropProcessor.Factory().create(Map.of(), null, Map.of());
        TestProcessor processor3 = new TestProcessor(ingestDocument -> ingestDocument.setFieldValue("field2", "value"));
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2, processor3));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SimulateDocumentResult> holder = new AtomicReference<>();
        executionService.executeDocument(pipeline, ingestDocument, true, (r, e) -> {
            holder.set(r);
            latch.countDown();
        });
        latch.await();
        SimulateDocumentResult actualItemResponse = holder.get();
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor3.getInvokedCounter(), equalTo(0));
        assertThat(actualItemResponse, instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult verboseResult = (SimulateDocumentVerboseResult) actualItemResponse;
        assertThat(verboseResult.getProcessorResults().size(), equalTo(2));
        assertThat(verboseResult.getProcessorResults().get(0).getIngestDocument(), notNullValue());
        assertThat(verboseResult.getProcessorResults().get(0).getFailure(), nullValue());
        assertThat(verboseResult.getProcessorResults().get(1).getIngestDocument(), nullValue());
        assertThat(verboseResult.getProcessorResults().get(1).getFailure(), nullValue());
    }

    public void testAsyncSimulation() throws Exception {
        int numDocs = randomIntBetween(1, 64);
        List<IngestDocument> documents = new ArrayList<>(numDocs);
        for (int id = 0; id < numDocs; id++) {
            documents.add(new IngestDocument("_index", Integer.toString(id), null, 0L, VersionType.INTERNAL, new HashMap<>()));
        }
        Processor processor1 = new AbstractProcessor(null) {

            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    ingestDocument.setFieldValue("processed", true);
                    handler.accept(ingestDocument, null);
                });
            }

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getType() {
                return "none-of-your-business";
            }
        };
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1));
        SimulatePipelineRequest.Parsed request = new SimulatePipelineRequest.Parsed(pipeline, documents, false);

        AtomicReference<SimulatePipelineResponse> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        executionService.execute(request, ActionListener.wrap(response -> {
            responseHolder.set(response);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }));
        latch.await(1, TimeUnit.MINUTES);
        assertThat(errorHolder.get(), nullValue());
        SimulatePipelineResponse response = responseHolder.get();
        assertThat(response, notNullValue());
        assertThat(response.getResults().size(), equalTo(numDocs));

        for (int id = 0; id < numDocs; id++) {
            SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) response.getResults().get(id);
            assertThat(result.getIngestDocument().getMetadata().get(IngestDocument.Metadata.ID), equalTo(Integer.toString(id)));
            assertThat(result.getIngestDocument().getSourceAndMetadata().get("processed"), is(true));
        }
    }

    private static void assertVerboseResult(SimulateProcessorResult result,
                                            String expectedPipelineId,
                                            IngestDocument expectedIngestDocument) {
        IngestDocument simulateVerboseIngestDocument = result.getIngestDocument();
        // Remove and compare pipeline key. It is always in the verbose result,
        // since that is a snapshot of how the ingest doc looks during pipeline execution, but not in the final ingestDocument.
        // The key gets added and removed during pipeline execution.
        String actualPipelineId = (String) simulateVerboseIngestDocument.getIngestMetadata().remove("pipeline");
        assertThat(actualPipelineId, equalTo(expectedPipelineId));

        assertThat(simulateVerboseIngestDocument, not(sameInstance(expectedIngestDocument)));
        assertIngestDocument(simulateVerboseIngestDocument, expectedIngestDocument);
        assertThat(simulateVerboseIngestDocument.getSourceAndMetadata(), not(sameInstance(expectedIngestDocument.getSourceAndMetadata())));
    }

}
