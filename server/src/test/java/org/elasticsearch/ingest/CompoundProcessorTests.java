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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompoundProcessorTests extends ESTestCase {
    private IngestDocument ingestDocument;

    @Before
    public void init() {
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
    }

    public void testEmpty() throws Exception {
        CompoundProcessor processor = new CompoundProcessor();
        assertThat(processor.getProcessors().isEmpty(), is(true));
        assertThat(processor.getOnFailureProcessors().isEmpty(), is(true));
        processor.execute(ingestDocument, (result, e) -> {});
    }

    public void testSingleProcessor() throws Exception {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        TestProcessor processor = new TestProcessor(ingestDocument ->{
            assertStats(0, ingestDocument.getFieldValue("compoundProcessor", CompoundProcessor.class), 1, 0, 0, 0);
        });
        CompoundProcessor compoundProcessor = new CompoundProcessor(relativeTimeProvider, processor);
        ingestDocument.setFieldValue("compoundProcessor", compoundProcessor); //ugly hack to assert current count = 1
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 0, 1);

    }

    public void testSingleProcessorWithException() throws Exception {
        TestProcessor processor = new TestProcessor(new RuntimeException("error"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(relativeTimeProvider, processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        Exception[] holder = new Exception[1];
        compoundProcessor.execute(ingestDocument, (result, e) -> holder[0] = e);
        assertThat(((ElasticsearchException) holder[0]).getRootCause().getMessage(), equalTo("error"));
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);

    }

    public void testIgnoreFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor(new RuntimeException("error"));
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue("field", "value");});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor =
            new CompoundProcessor(true, Arrays.asList(processor1, processor2), Collections.emptyList(), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertStats(1, compoundProcessor, 0, 1, 0, 0);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("value"));
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", new RuntimeException("error"));
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor1),
            Collections.singletonList(processor2), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();

        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 1);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithOnFailureDropProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", new RuntimeException("error"));
        Processor processor2 = new Processor() {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                //Simulates the drop processor
                return null;
            }

            @Override
            public String getType() {
                return "drop";
            }

            @Override
            public String getTag() {
                return null;
            }
        };

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor1),
            Collections.singletonList(processor2), relativeTimeProvider);
        IngestDocument[] result = new IngestDocument[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> result[0] = r);
        assertThat(result[0], nullValue());
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        TestProcessor processor = new TestProcessor("id", "first", new RuntimeException("error"));
        TestProcessor processorToFail = new TestProcessor("id2", "second", (Consumer<IngestDocument>) ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
            throw new RuntimeException("error");
        });
        TestProcessor lastProcessor = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("second"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id2"));
        });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(false, Collections.singletonList(processorToFail),
            Collections.singletonList(lastProcessor), relativeTimeProvider);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor),
            Collections.singletonList(compoundOnFailProcessor), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(processorToFail.getInvokedCounter(), equalTo(1));
        assertThat(lastProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFailWithoutOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", new RuntimeException("error"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id1"));
        });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(relativeTimeProvider, firstProcessor);

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFail() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", new RuntimeException("error"));
        TestProcessor failProcessor =
            new TestProcessor("tag_fail", "fail", new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(false, Collections.singletonList(firstProcessor),
            Collections.singletonList(failProcessor), relativeTimeProvider);

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFailInOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", new RuntimeException("error"));
        TestProcessor failProcessor =
            new TestProcessor("tag_fail", "fail", new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(false, Collections.singletonList(firstProcessor),
            Collections.singletonList(new CompoundProcessor(relativeTimeProvider, failProcessor)));

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor), relativeTimeProvider);
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testBreakOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", new RuntimeException("error1"));
        TestProcessor secondProcessor = new TestProcessor("id2", "second", new RuntimeException("error2"));
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", ingestDocument -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor pipeline = new CompoundProcessor(false, Arrays.asList(firstProcessor, secondProcessor),
            Collections.singletonList(onFailureProcessor), relativeTimeProvider);
        pipeline.execute(ingestDocument, (result, e) -> {});
        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(0));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));
        assertStats(pipeline, 1, 1, 0);
    }

    public void testFailureProcessorIsInvokedOnFailure() {
        TestProcessor onFailureProcessor = new TestProcessor(null, "on_failure", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(5));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("failure!"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test-processor"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), nullValue());
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PIPELINE_FIELD), equalTo("2"));
            assertThat(ingestMetadata.get("pipeline"), equalTo("1"));
        });

        Pipeline pipeline2 = new Pipeline("2", null, null, new CompoundProcessor(new TestProcessor(new RuntimeException("failure!"))));
        Pipeline pipeline1 = new Pipeline("1", null, null, new CompoundProcessor(false, List.of(new AbstractProcessor(null) {
            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                ingestDocument.executePipeline(pipeline2, handler);
            }

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                throw new AssertionError();
            }

            @Override
            public String getType() {
                return "pipeline";
            }
        }), List.of(onFailureProcessor)));

        ingestDocument.executePipeline(pipeline1, (document, e) -> {
            assertThat(document, notNullValue());
            assertThat(e, nullValue());
        });
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testNewCompoundProcessorException() {
        TestProcessor processor = new TestProcessor("my_tag", "my_type", new RuntimeException());
        IngestProcessorException ingestProcessorException1 =
            CompoundProcessor.newCompoundProcessorException(new RuntimeException(), processor, ingestDocument);
        assertThat(ingestProcessorException1.getHeader("processor_tag"), equalTo(List.of("my_tag")));
        assertThat(ingestProcessorException1.getHeader("processor_type"), equalTo(List.of("my_type")));
        assertThat(ingestProcessorException1.getHeader("pipeline_origin"), nullValue());

        IngestProcessorException ingestProcessorException2 =
            CompoundProcessor.newCompoundProcessorException(ingestProcessorException1, processor, ingestDocument);
        assertThat(ingestProcessorException2, sameInstance(ingestProcessorException1));
    }

    public void testNewCompoundProcessorExceptionPipelineOrigin() {
        Pipeline pipeline2 = new Pipeline("2", null, null,
            new CompoundProcessor(new TestProcessor("my_tag", "my_type", new RuntimeException())));
        Pipeline pipeline1 = new Pipeline("1", null, null, new CompoundProcessor(new AbstractProcessor(null) {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                 throw new UnsupportedOperationException();
            }

            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                ingestDocument.executePipeline(pipeline2, handler);
            }

            @Override
            public String getType() {
                return "my_type2";
            }
        }));

        Exception[] holder = new Exception[1];
        ingestDocument.executePipeline(pipeline1, (document, e) -> holder[0] = e);
        IngestProcessorException ingestProcessorException = (IngestProcessorException) holder[0];
        assertThat(ingestProcessorException.getHeader("processor_tag"), equalTo(List.of("my_tag")));
        assertThat(ingestProcessorException.getHeader("processor_type"), equalTo(List.of("my_type")));
        assertThat(ingestProcessorException.getHeader("pipeline_origin"), equalTo(List.of("2", "1")));
    }

    private void assertStats(CompoundProcessor compoundProcessor, long count,  long failed, long time) {
        assertStats(0, compoundProcessor, 0L, count, failed, time);
    }

    private void assertStats(int processor, CompoundProcessor compoundProcessor, long current, long count,  long failed, long time) {
        IngestStats.Stats stats = compoundProcessor.getProcessorsWithMetrics().get(processor).v2().createStats();
        assertThat(stats.getIngestCount(), equalTo(count));
        assertThat(stats.getIngestCurrent(), equalTo(current));
        assertThat(stats.getIngestFailedCount(), equalTo(failed));
        assertThat(stats.getIngestTimeInMillis(), equalTo(time));
    }
}
