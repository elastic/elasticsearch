/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
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
        ingestDocument = TestIngestDocument.emptyIngestDocument();
    }

    // need to (randomly?) mix sync and async processors
    // verify that sync execute method throws

    public void testEmpty() throws Exception {
        CompoundProcessor processor = new CompoundProcessor();
        assertThat(processor.getProcessors().isEmpty(), is(true));
        assertThat(processor.getOnFailureProcessors().isEmpty(), is(true));
        executeCompound(processor, ingestDocument, (result, e) -> {});
    }

    public void testSingleProcessor() throws Exception {
        boolean isAsync = randomBoolean();
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        TestProcessor processor = new TestProcessor(ingestDocument -> {
            assertStats(0, ingestDocument.getFieldValue("compoundProcessor", CompoundProcessor.class), 1, 0, 0, 0);
        }) {
            @Override
            public boolean isAsync() {
                return isAsync;
            }
        };
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, List.of(processor), List.of(), relativeTimeProvider);
        ingestDocument.setFieldValue("compoundProcessor", compoundProcessor); // ugly hack to assert current count = 1
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 0, 1);
    }

    public void testSingleProcessorWithException() throws Exception {
        TestProcessor processor = new TestProcessor(new RuntimeException("error"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, List.of(processor), List.of(), relativeTimeProvider);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        Exception[] holder = new Exception[1];
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> holder[0] = e);
        assertThat(((ElasticsearchException) holder[0]).getRootCause().getMessage(), equalTo("error"));
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testIgnoreFailure() throws Exception {
        TestProcessor processor1 = getTestProcessor(null, randomBoolean(), true);
        TestProcessor processor2 = new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue("field", "value"); });
        TestProcessor processor3 = new TestProcessor(ingestDocument -> fail("ignoreFailure is true, processor shouldn't be called"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            true,
            List.of(processor1, processor2),
            List.of(processor3), // when ignoreFailure is true, onFailureProcessors are not called (regardless of whether a failure occurs)
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertStats(1, compoundProcessor, 0, 1, 0, 0);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("value"));
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", null, new RuntimeException("error"));
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, List.of(processor1), List.of(processor2), relativeTimeProvider);
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();

        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 1);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithOnFailureDropProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", null, new RuntimeException("error"));
        Processor processor2 = new Processor() {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                // Simulates the drop processor
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

            @Override
            public String getDescription() {
                return null;
            }
        };

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, List.of(processor1), List.of(processor2), relativeTimeProvider);
        IngestDocument[] result = new IngestDocument[1];
        executeCompound(compoundProcessor, ingestDocument, (r, e) -> result[0] = r);
        assertThat(result[0], nullValue());
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        TestProcessor processor = new TestProcessor("id", "first", null, new RuntimeException("error"));
        TestProcessor processorToFail = new TestProcessor("id2", "second", null, (Consumer<IngestDocument>) ingestDocument -> {
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
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(
            false,
            List.of(processorToFail),
            List.of(lastProcessor),
            relativeTimeProvider
        );
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor),
            List.of(compoundOnFailProcessor),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});

        assertThat(processorToFail.getInvokedCounter(), equalTo(1));
        assertThat(lastProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testNestedOnFailureHandlers() {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        TestProcessor firstFailingProcessor = new TestProcessor("id1", "first", null, new RuntimeException("first failure"));
        TestProcessor onFailure1 = new TestProcessor("id2", "second", null, ingestDocument -> {
            ingestDocument.setFieldValue("foofield", "exists");
        });
        TestProcessor onFailure2 = new TestProcessor("id3", "third", null, new RuntimeException("onfailure2"));
        TestProcessor onFailure2onFailure = new TestProcessor("id4", "4th", null, ingestDocument -> {
            ingestDocument.setFieldValue("foofield2", "ran");
        });
        CompoundProcessor of2 = new CompoundProcessor(false, List.of(onFailure2), List.of(onFailure2onFailure), relativeTimeProvider);
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(false, List.of(onFailure1, of2), List.of(), relativeTimeProvider);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(firstFailingProcessor),
            List.of(compoundOnFailProcessor),
            relativeTimeProvider
        );
        IngestDocument[] docHolder = new IngestDocument[1];
        Exception[] exHolder = new Exception[1];
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {
            docHolder[0] = result;
            exHolder[0] = e;
        });

        assertThat(onFailure1.getInvokedCounter(), equalTo(1));
        assertThat(onFailure2.getInvokedCounter(), equalTo(1));
        assertThat(onFailure2onFailure.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
        assertThat(docHolder[0], notNullValue());
        assertThat(exHolder[0], nullValue());
        assertThat(docHolder[0].getFieldValue("foofield", String.class), equalTo("exists"));
        assertThat(docHolder[0].getFieldValue("foofield2", String.class), equalTo("ran"));
    }

    public void testCompoundProcessorExceptionFailWithoutOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id1"));
        });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(false, List.of(firstProcessor), List.of(), relativeTimeProvider);

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(failCompoundProcessor),
            List.of(secondProcessor),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFail() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor failProcessor = new TestProcessor("tag_fail", "fail", null, new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(
            false,
            List.of(firstProcessor),
            List.of(failProcessor),
            relativeTimeProvider
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(failCompoundProcessor),
            List.of(secondProcessor),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFailInOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor failProcessor = new TestProcessor("tag_fail", "fail", null, new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(
            false,
            List.of(firstProcessor),
            List.of(new CompoundProcessor(false, List.of(failProcessor), List.of(), relativeTimeProvider))
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(failCompoundProcessor),
            List.of(secondProcessor),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testBreakOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error1"));
        TestProcessor secondProcessor = new TestProcessor("id2", "second", null, new RuntimeException("error2"));
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", null, ingestDocument -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor pipeline = new CompoundProcessor(
            false,
            List.of(firstProcessor, secondProcessor),
            List.of(onFailureProcessor),
            relativeTimeProvider
        );
        executeCompound(pipeline, ingestDocument, (result, e) -> {});
        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(0));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));
        assertStats(pipeline, 1, 1, 0);
    }

    public void testFailureProcessorIsInvokedOnFailure() {
        TestProcessor onFailureProcessor = new TestProcessor(null, "on_failure", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(5));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("failure!"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test-processor"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), nullValue());
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PIPELINE_FIELD), equalTo("2"));
            assertThat(ingestMetadata.get("pipeline"), equalTo("1"));
        });

        Pipeline pipeline2 = new Pipeline(
            "2",
            null,
            null,
            null,
            new CompoundProcessor(new TestProcessor(new RuntimeException("failure!")))
        );
        Pipeline pipeline1 = new Pipeline("1", null, null, null, new CompoundProcessor(false, List.of(new AbstractProcessor(null, null) {
            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                throw new AssertionError();
            }

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                IngestDocument[] result = new IngestDocument[1];
                Exception[] error = new Exception[1];

                ingestDocument.executePipeline(pipeline2, (document, e) -> {
                    result[0] = document;
                    error[0] = e;
                });
                if (error[0] != null) {
                    throw error[0];
                }
                return result[0];
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
        TestProcessor processor = new TestProcessor("my_tag", "my_type", null, new RuntimeException());
        IngestProcessorException ingestProcessorException1 = CompoundProcessor.newCompoundProcessorException(
            new RuntimeException(),
            processor,
            ingestDocument
        );
        assertThat(ingestProcessorException1.getHeader("processor_tag"), equalTo(List.of("my_tag")));
        assertThat(ingestProcessorException1.getHeader("processor_type"), equalTo(List.of("my_type")));
        assertThat(ingestProcessorException1.getHeader("pipeline_origin"), nullValue());

        IngestProcessorException ingestProcessorException2 = CompoundProcessor.newCompoundProcessorException(
            ingestProcessorException1,
            processor,
            ingestDocument
        );
        assertThat(ingestProcessorException2, sameInstance(ingestProcessorException1));
    }

    public void testNewCompoundProcessorExceptionPipelineOrigin() {
        Pipeline pipeline2 = new Pipeline(
            "2",
            null,
            null,
            null,
            new CompoundProcessor(new TestProcessor("my_tag", "my_type", null, new RuntimeException()))
        );
        Pipeline pipeline1 = new Pipeline("1", null, null, null, new CompoundProcessor(new AbstractProcessor(null, null) {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                IngestDocument[] result = new IngestDocument[1];
                Exception[] error = new Exception[1];

                ingestDocument.executePipeline(pipeline2, (document, e) -> {
                    result[0] = document;
                    error[0] = e;
                });
                if (error[0] != null) {
                    throw error[0];
                }
                return result[0];
            }

            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                throw new UnsupportedOperationException();
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

    public void testMultipleProcessors() {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        int processorsCount = 1000;
        TestProcessor[] processors = new TestProcessor[processorsCount];
        for (int i = 0; i < processorsCount; i++) {
            processors[i] = getTestProcessor(Integer.toString(i), randomBoolean(), randomBoolean());
        }
        CompoundProcessor compoundProcessor = new CompoundProcessor(true, List.of(processors), List.of(), relativeTimeProvider);
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {
            if (e != null) fail("CompoundProcessor threw exception despite ignoreFailure being true");
        });
        for (int i = 0; i < processors.length; i++) {
            assertThat(
                "Processor " + i + " ran " + processors[i].getInvokedCounter() + " times",
                processors[i].getInvokedCounter(),
                equalTo(1)
            );
        }
    }

    public void testMultipleProcessorsDoNotIgnoreFailures() {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        int goodProcessorsCount = 100;
        int totalProcessorsCount = goodProcessorsCount * 2 + 1;
        TestProcessor[] processors = new TestProcessor[totalProcessorsCount];
        for (int i = 0; i < goodProcessorsCount; i++) {
            processors[i] = getTestProcessor(Integer.toString(i), randomBoolean(), false);
        }
        processors[goodProcessorsCount] = getTestProcessor(Integer.toString(goodProcessorsCount), randomBoolean(), true);
        for (int i = goodProcessorsCount + 1; i < totalProcessorsCount; i++) {
            processors[i] = getTestProcessor(Integer.toString(i), randomBoolean(), false);
        }
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, List.of(processors), List.of(), relativeTimeProvider);
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        for (int i = 0; i < goodProcessorsCount + 1; i++) {
            assertThat(
                "Processor " + i + " ran " + processors[i].getInvokedCounter() + " times",
                processors[i].getInvokedCounter(),
                equalTo(1)
            );
        }
        for (int i = goodProcessorsCount + 1; i < totalProcessorsCount; i++) {
            assertThat(
                "Processor " + i + " ran " + processors[i].getInvokedCounter() + " times",
                processors[i].getInvokedCounter(),
                equalTo(0)
            );
        }
    }

    public void testSkipPipeline() {
        TestProcessor processor1 = new TestProcessor(doc -> doc.reroute("foo"));
        TestProcessor processor2 = new TestProcessor(new RuntimeException("this processor was expected to be skipped"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor1, processor2),
            List.of(),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 0, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(0));
        assertStats(1, compoundProcessor, 0, 0, 0, 0);
    }

    public void testSkipAsyncProcessor() {
        TestProcessor processor1 = new TestProcessor(doc -> doc.reroute("foo")) {
            @Override
            public boolean isAsync() {
                return true;
            }
        };
        TestProcessor processor2 = new TestProcessor(new RuntimeException("this processor was expected to be skipped"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor1, processor2),
            List.of(),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 0, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(0));
        assertStats(1, compoundProcessor, 0, 0, 0, 0);
    }

    public void testSkipProcessorIgnoreFailure() {
        TestProcessor processor1 = new TestProcessor(doc -> {
            doc.reroute("foo");
            throw new RuntimeException("simulate processor failure after calling skipCurrentPipeline()");
        });
        TestProcessor processor2 = new TestProcessor(doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(true, List.of(processor1, processor2), List.of(), relativeTimeProvider);
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(0));
        assertStats(1, compoundProcessor, 0, 0, 0, 0);
    }

    public void testDontSkipFailureProcessor() {
        TestProcessor processor = new TestProcessor(doc -> {
            doc.reroute("foo");
            throw new RuntimeException("simulate processor failure after calling skipCurrentPipeline()");
        });
        TestProcessor failureProcessor1 = new TestProcessor(doc -> {});
        TestProcessor failureProcessor2 = new TestProcessor(doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor),
            List.of(failureProcessor1, failureProcessor2),
            relativeTimeProvider
        );
        executeCompound(compoundProcessor, ingestDocument, (result, e) -> {});
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
        assertThat(failureProcessor1.getInvokedCounter(), equalTo(1));
        assertThat(failureProcessor2.getInvokedCounter(), equalTo(1));
    }

    private TestProcessor getTestProcessor(String tag, boolean isAsync, boolean shouldThrowException) {
        return new TestProcessor(tag, "test-processor", null, ingestDocument -> {
            if (shouldThrowException) throw new RuntimeException("Intentionally failing");
        }) {
            @Override
            public boolean isAsync() {
                return isAsync;
            }
        };
    }

    // delegates to appropriate sync or async method
    private static void executeCompound(CompoundProcessor cp, IngestDocument doc, BiConsumer<IngestDocument, Exception> handler) {
        if (cp.isAsync()) {
            cp.execute(doc, handler);
        } else {
            try {
                IngestDocument result = cp.execute(doc);
                handler.accept(result, null);
            } catch (Exception e) {
                handler.accept(null, e);
            }
        }
    }

    private void assertStats(CompoundProcessor compoundProcessor, long count, long failed, long time) {
        assertStats(0, compoundProcessor, 0L, count, failed, time);
    }

    private void assertStats(int processor, CompoundProcessor compoundProcessor, long current, long count, long failed, long time) {
        IngestStats.Stats stats = compoundProcessor.getProcessorsWithMetrics().get(processor).v2().createStats();
        assertThat(stats.ingestCount(), equalTo(count));
        assertThat(stats.ingestCurrent(), equalTo(current));
        assertThat(stats.ingestFailedCount(), equalTo(failed));
        assertThat(stats.ingestTimeInMillis(), equalTo(time));
    }
}
