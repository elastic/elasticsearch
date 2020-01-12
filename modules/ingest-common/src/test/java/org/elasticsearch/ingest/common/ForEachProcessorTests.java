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

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class ForEachProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private Consumer<Runnable> genericExecutor = (Consumer<Runnable>) mock(Consumer.class);
    private final ExecutorService direct = EsExecutors.newDirectExecutorService();

    @Before
    public void setup() {
        //execute runnable on same thread for simplicity. some tests will override this and actually run async
        doAnswer(invocationOnMock -> {
            direct.execute((Runnable) invocationOnMock.getArguments()[0]);
            return null;
        }).when(genericExecutor).accept(any(Runnable.class));
    }

    public void testExecute() throws Exception {
        ThreadPoolExecutor asyncExecutor =
            EsExecutors.newScaling(getClass().getName() + "/" + getTestName(), between(1, 2), between(3, 4), 10, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory("test"), new ThreadContext(Settings.EMPTY));
        doAnswer(invocationOnMock -> {
            asyncExecutor.execute((Runnable) invocationOnMock.getArguments()[0]);
            return null;
        }).when(genericExecutor).accept(any(Runnable.class));

        List<String> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add("baz");
        IntStream.range(0, ForEachProcessor.MAX_RECURSE_PER_THREAD).forEach(value -> values.add("a"));
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", "values", new UppercaseProcessor("_tag", "_ingest._value", false, "_ingest._value"),
            false, genericExecutor
        );
        processor.execute(ingestDocument, (result, e) -> {});

        assertBusy(() -> assertEquals(values.size() / ForEachProcessor.MAX_RECURSE_PER_THREAD, asyncExecutor.getCompletedTaskCount()));
        asyncExecutor.shutdown();
        asyncExecutor.awaitTermination(5, TimeUnit.SECONDS);

        @SuppressWarnings("unchecked")
        List<String> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.get(0), equalTo("FOO"));
        assertThat(result.get(1), equalTo("BAR"));
        assertThat(result.get(2), equalTo("BAZ"));
        IntStream.range(3, ForEachProcessor.MAX_RECURSE_PER_THREAD + 3).forEach(i -> assertThat(result.get(i), equalTo("A")));
        verify(genericExecutor, times(values.size() / ForEachProcessor.MAX_RECURSE_PER_THREAD)).accept(any(Runnable.class));
    }

    public void testExecuteWithAsyncProcessor() throws Exception {
        List<String> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add("baz");
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor("_tag", "values", new AsyncUpperCaseProcessor("_ingest._value"),
            false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {
        });

        assertBusy(() -> {
            @SuppressWarnings("unchecked")
            List<String> result = ingestDocument.getFieldValue("values", List.class);
            assertEquals(values.size(), result.size());
            assertThat(result.get(0), equalTo("FOO"));
            assertThat(result.get(1), equalTo("BAR"));
            assertThat(result.get(2), equalTo("BAZ"));
        });

        verifyZeroInteractions(genericExecutor);
    }

    public void testExecuteWithFailure() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.singletonMap("values", Arrays.asList("a", "b", "c"))
        );

        TestProcessor testProcessor = new TestProcessor(id -> {
            if ("c".equals(id.getFieldValue("_ingest._value", String.class))) {
                throw new RuntimeException("failure");
            }
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", "values", testProcessor, false, genericExecutor);
        Exception[] exceptions = new Exception[1];
        processor.execute(ingestDocument, (result, e) -> {exceptions[0] = e;});
        assertThat(exceptions[0].getMessage(), equalTo("failure"));
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(ingestDocument.getFieldValue("values", List.class), equalTo(Arrays.asList("a", "b", "c")));

        testProcessor = new TestProcessor(id -> {
            String value = id.getFieldValue("_ingest._value", String.class);
            if ("c".equals(value)) {
                throw new RuntimeException("failure");
            } else {
                id.setFieldValue("_ingest._value", value.toUpperCase(Locale.ROOT));
            }
        });
        Processor onFailureProcessor = new TestProcessor(ingestDocument1 -> {});
        processor = new ForEachProcessor(
            "_tag", "values", new CompoundProcessor(false, Arrays.asList(testProcessor), Arrays.asList(onFailureProcessor)),
            false, genericExecutor
        );
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(ingestDocument.getFieldValue("values", List.class), equalTo(Arrays.asList("A", "B", "c")));
    }

    public void testMetaDataAvailable() throws Exception {
        List<Map<String, Object>> values = new ArrayList<>();
        values.add(new HashMap<>());
        values.add(new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        TestProcessor innerProcessor = new TestProcessor(id -> {
            id.setFieldValue("_ingest._value.index", id.getSourceAndMetadata().get("_index"));
            id.setFieldValue("_ingest._value.id", id.getSourceAndMetadata().get("_id"));
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", "values", innerProcessor, false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});

        assertThat(innerProcessor.getInvokedCounter(), equalTo(2));
        assertThat(ingestDocument.getFieldValue("values.0.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.0.id", String.class), equalTo("_id"));
        assertThat(ingestDocument.getFieldValue("values.1.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.1.id", String.class), equalTo("_id"));
    }

    public void testRestOfTheDocumentIsAvailable() throws Exception {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Map<String, Object> object = new HashMap<>();
            object.put("field", "value");
            values.add(object);
        }
        Map<String, Object> document = new HashMap<>();
        document.put("values", values);
        document.put("flat_values", new ArrayList<>());
        document.put("other", "value");
        IngestDocument ingestDocument = new IngestDocument("_index", "_id", null, null, null, document);

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", "values", new SetProcessor("_tag",
            new TestTemplateService.MockTemplateScript.Factory("_ingest._value.new_field"),
            (model) -> model.get("other")), false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});

        assertThat(ingestDocument.getFieldValue("values.0.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.1.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.2.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.3.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.4.new_field", String.class), equalTo("value"));
    }

    public void testRandom() throws Exception {
        ThreadPoolExecutor asyncExecutor =
            EsExecutors.newScaling(getClass().getName() + "/" + getTestName(), between(1, 2), between(3, 4), 10, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory("test"), new ThreadContext(Settings.EMPTY));
        doAnswer(invocationOnMock -> {
            asyncExecutor.execute((Runnable) invocationOnMock.getArguments()[0]);
            return null;
        }).when(genericExecutor).accept(any(Runnable.class));
        Processor innerProcessor = new Processor() {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                    String existingValue = ingestDocument.getFieldValue("_ingest._value", String.class);
                    ingestDocument.setFieldValue("_ingest._value", existingValue + ".");
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }
        };
        int numValues = randomIntBetween(1, 10000);
        List<String> values = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            values.add("");
        }
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor("_tag", "values", innerProcessor, false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});

        assertBusy(() -> assertEquals(values.size() / ForEachProcessor.MAX_RECURSE_PER_THREAD, asyncExecutor.getCompletedTaskCount()));
        asyncExecutor.shutdown();
        asyncExecutor.awaitTermination(5, TimeUnit.SECONDS);

        @SuppressWarnings("unchecked")
        List<String> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.size(), equalTo(numValues));
        for (String r : result) {
            assertThat(r, equalTo("."));
        }
        verify(genericExecutor, times(values.size() / ForEachProcessor.MAX_RECURSE_PER_THREAD)).accept(any(Runnable.class));
    }

    public void testModifyFieldsOutsideArray() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add("string");
        values.add(1);
        values.add(null);
        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        TemplateScript.Factory template = new TestTemplateService.MockTemplateScript.Factory("errors");

        ForEachProcessor processor = new ForEachProcessor(
                "_tag", "values", new CompoundProcessor(false,
                Collections.singletonList(new UppercaseProcessor("_tag_upper", "_ingest._value", false, "_ingest._value")),
                Collections.singletonList(new AppendProcessor("_tag", template, (model) -> (Collections.singletonList("added"))))
        ), false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});

        List<?> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.get(0), equalTo("STRING"));
        assertThat(result.get(1), equalTo(1));
        assertThat(result.get(2), equalTo(null));

        List<?> errors = ingestDocument.getFieldValue("errors", List.class);
        assertThat(errors.size(), equalTo(2));
    }

    public void testScalarValueAllowsUnderscoreValueFieldToRemainAccessible() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add("please");
        values.add("change");
        values.add("me");
        Map<String, Object> source = new HashMap<>();
        source.put("_value", "new_value");
        source.put("values", values);
        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_id", null, null, null, source
        );

        TestProcessor processor = new TestProcessor(doc -> doc.setFieldValue("_ingest._value",
                doc.getFieldValue("_source._value", String.class)));
        ForEachProcessor forEachProcessor = new ForEachProcessor("_tag", "values", processor, false, genericExecutor);
        forEachProcessor.execute(ingestDocument, (result, e) -> {});

        List<?> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.get(0), equalTo("new_value"));
        assertThat(result.get(1), equalTo("new_value"));
        assertThat(result.get(2), equalTo("new_value"));
    }

    public void testNestedForEach() throws Exception {
        List<Map<String, Object>> values = new ArrayList<>();
        List<Object> innerValues = new ArrayList<>();
        innerValues.add("abc");
        innerValues.add("def");
        Map<String, Object> value = new HashMap<>();
        value.put("values2", innerValues);
        values.add(value);

        innerValues = new ArrayList<>();
        innerValues.add("ghi");
        innerValues.add("jkl");
        value = new HashMap<>();
        value.put("values2", innerValues);
        values.add(value);

        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_id", null, null, null, Collections.singletonMap("values1", values)
        );

        TestProcessor testProcessor = new TestProcessor(
                doc -> doc.setFieldValue("_ingest._value", doc.getFieldValue("_ingest._value", String.class).toUpperCase(Locale.ENGLISH))
        );
        ForEachProcessor processor = new ForEachProcessor(
                "_tag", "values1", new ForEachProcessor("_tag", "_ingest._value.values2", testProcessor, false, genericExecutor),
            false, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});

        List<?> result = ingestDocument.getFieldValue("values1.0.values2", List.class);
        assertThat(result.get(0), equalTo("ABC"));
        assertThat(result.get(1), equalTo("DEF"));

        List<?> result2 = ingestDocument.getFieldValue("values1.1.values2", List.class);
        assertThat(result2.get(0), equalTo("GHI"));
        assertThat(result2.get(1), equalTo("JKL"));
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = new IngestDocument(
            "_index", "_id", null, null, null, Collections.emptyMap()
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        TestProcessor testProcessor = new TestProcessor(doc -> {});
        ForEachProcessor processor = new ForEachProcessor("_tag", "_ingest._value", testProcessor, true, genericExecutor);
        processor.execute(ingestDocument, (result, e) -> {});
        assertIngestDocument(originalIngestDocument, ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(0));
    }

    private class AsyncUpperCaseProcessor implements Processor {

        private final String field;

        private AsyncUpperCaseProcessor(String field) {
            this.field = field;
        }

        @Override
        public void execute(IngestDocument document, BiConsumer<IngestDocument, Exception> handler) {
            new Thread(() -> {
                try {
                    String value = document.getFieldValue(field, String.class, false);
                    document.setFieldValue(field, value.toUpperCase(Locale.ROOT));
                    handler.accept(document, null);
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }).start();
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            throw new UnsupportedOperationException("this is an async processor, don't call this");
        }

        @Override
        public String getType() {
            return "uppercase-async";
        }

        @Override
        public String getTag() {
            return getType();
        }
    }

}
