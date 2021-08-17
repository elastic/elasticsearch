/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.core.Map;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class ForEachProcessorTests extends ESTestCase {

    public void testExecuteWithAsyncProcessor() throws Exception {
        List<String> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add("baz");
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor("_tag", null, "values", new AsyncUpperCaseProcessor("_ingest._value"),
            false);
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
    }

    public void testExecuteWithFailure() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, Collections.singletonMap("values", Arrays.asList("a", "b", "c"))
        );

        TestProcessor testProcessor = new TestProcessor(id -> {
            if ("c".equals(id.getFieldValue("_ingest._value", String.class))) {
                throw new RuntimeException("failure");
            }
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "values", testProcessor, false);
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
            "_tag", null, "values", new CompoundProcessor(false, Arrays.asList(testProcessor), Arrays.asList(onFailureProcessor)),
            false
        );
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(ingestDocument.getFieldValue("values", List.class), equalTo(Arrays.asList("A", "B", "c")));
    }

    public void testMetadataAvailable() throws Exception {
        List<java.util.Map<String, Object>> values = new ArrayList<>();
        values.add(new HashMap<>());
        values.add(new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        TestProcessor innerProcessor = new TestProcessor(id -> {
            id.setFieldValue("_ingest._value.index", id.getSourceAndMetadata().get("_index"));
            id.setFieldValue("_ingest._value.type", id.getSourceAndMetadata().get("_type"));
            id.setFieldValue("_ingest._value.id", id.getSourceAndMetadata().get("_id"));
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "values", innerProcessor, false);
        processor.execute(ingestDocument, (result, e) -> {});

        assertThat(innerProcessor.getInvokedCounter(), equalTo(2));
        assertThat(ingestDocument.getFieldValue("values.0.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.0.type", String.class), equalTo("_type"));
        assertThat(ingestDocument.getFieldValue("values.0.id", String.class), equalTo("_id"));
        assertThat(ingestDocument.getFieldValue("values.1.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.1.type", String.class), equalTo("_type"));
        assertThat(ingestDocument.getFieldValue("values.1.id", String.class), equalTo("_id"));
    }

    public void testRestOfTheDocumentIsAvailable() throws Exception {
        List<java.util.Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            java.util.Map<String, Object> object = new HashMap<>();
            object.put("field", "value");
            values.add(object);
        }
        java.util.Map<String, Object> document = new HashMap<>();
        document.put("values", values);
        document.put("flat_values", new ArrayList<>());
        document.put("other", "value");
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, document);

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", null, "values", new SetProcessor("_tag",
            null, new TestTemplateService.MockTemplateScript.Factory("_ingest._value.new_field"),
            (model) -> model.get("other"), null), false);
        processor.execute(ingestDocument, (result, e) -> {});

        assertThat(ingestDocument.getFieldValue("values.0.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.1.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.2.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.3.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.4.new_field", String.class), equalTo("value"));
    }

    public void testRandom() {
        Processor innerProcessor = new Processor() {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) {
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

            @Override
            public String getDescription() {
                return null;
            }
        };
        int numValues = randomIntBetween(1, 10000);
        List<String> values = IntStream.range(0, numValues).mapToObj(i->"").collect(Collectors.toList());

        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor("_tag", null, "values", innerProcessor, false);
        processor.execute(ingestDocument, (result, e) -> {});

        @SuppressWarnings("unchecked")
        List<String> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.size(), equalTo(numValues));
        result.forEach(r -> assertThat(r, equalTo(".")));
    }

    public void testModifyFieldsOutsideArray() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add("string");
        values.add(1);
        values.add(null);
        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_type", "_id", null, null, null, Collections.singletonMap("values", values)
        );

        TemplateScript.Factory template = new TestTemplateService.MockTemplateScript.Factory("errors");

        ForEachProcessor processor = new ForEachProcessor(
                "_tag", null, "values", new CompoundProcessor(false,
                org.elasticsearch.core.List.of(
                    new UppercaseProcessor("_tag_upper", null, "_ingest._value", false, "_ingest._value")),
                org.elasticsearch.core.List.of(
                    new AppendProcessor("_tag", null, template, (model) -> (Collections.singletonList("added")), true))
        ), false);
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
        java.util.Map<String, Object> source = new HashMap<>();
        source.put("_value", "new_value");
        source.put("values", values);
        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_type", "_id", null, null, null, source
        );

        TestProcessor processor = new TestProcessor(doc -> doc.setFieldValue("_ingest._value",
                doc.getFieldValue("_source._value", String.class)));
        ForEachProcessor forEachProcessor = new ForEachProcessor("_tag", null, "values", processor, false);
        forEachProcessor.execute(ingestDocument, (result, e) -> {});

        List<?> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.get(0), equalTo("new_value"));
        assertThat(result.get(1), equalTo("new_value"));
        assertThat(result.get(2), equalTo("new_value"));
    }

    public void testNestedForEach() throws Exception {
        List<java.util.Map<String, Object>> values = new ArrayList<>();
        List<Object> innerValues = new ArrayList<>();
        innerValues.add("abc");
        innerValues.add("def");
        java.util.Map<String, Object> value = new HashMap<>();
        value.put("values2", innerValues);
        values.add(value);

        innerValues = new ArrayList<>();
        innerValues.add("ghi");
        innerValues.add("jkl");
        value = new HashMap<>();
        value.put("values2", innerValues);
        values.add(value);

        IngestDocument ingestDocument = new IngestDocument(
                "_index", "_type", "_id", null, null, null, Collections.singletonMap("values1", values)
        );

        TestProcessor testProcessor = new TestProcessor(
                doc -> doc.setFieldValue("_ingest._value", doc.getFieldValue("_ingest._value", String.class).toUpperCase(Locale.ENGLISH))
        );
        ForEachProcessor processor = new ForEachProcessor(
                "_tag", null, "values1", new ForEachProcessor("_tag", null, "_ingest._value.values2", testProcessor, false),
            false);
        processor.execute(ingestDocument, (result, e) -> {});

        List<?> result = ingestDocument.getFieldValue("values1.0.values2", List.class);
        assertThat(result.get(0), equalTo("ABC"));
        assertThat(result.get(1), equalTo("DEF"));

        List<?> result2 = ingestDocument.getFieldValue("values1.1.values2", List.class);
        assertThat(result2.get(0), equalTo("GHI"));
        assertThat(result2.get(1), equalTo("JKL"));
    }

    public void testNestedForEachWithMapIteration() throws Exception {
        java.util.Map<String, Object> innerMap1 = Map.of("foo1", 1, "bar1", 2, "baz1", 3);
        java.util.Map<String, Object> innerMap2 = Map.of("foo2", 4, "bar2", 5, "baz2", 6);
        java.util.Map<String, Object> innerMap3 = Map.of("foo3", 7, "bar3", 8, "baz3", 9, "otherKey", 42);

        java.util.Map<String, Object> outerMap = Map.of("foo", innerMap1, "bar", innerMap2, "baz", innerMap3);
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, Map.of("field", outerMap));

        List<String> visitedKeys = new ArrayList<>();
        List<Object> visitedValues = new ArrayList<>();
        TestProcessor testProcessor = new TestProcessor(
            doc -> {
                String key = (String) doc.getIngestMetadata().get("_key");
                Object value = doc.getIngestMetadata().get("_value");
                visitedKeys.add(key);
                visitedValues.add(value);

                // change some of the keys
                if (key.startsWith("bar")) {
                    doc.setFieldValue("_ingest._key", "bar2");
                }
                // change some of the values
                if (key.startsWith("baz")) {
                    doc.setFieldValue("_ingest._value", ((Integer) value) * 2);
                }
            }
        );

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", null, "field", new ForEachProcessor("_tag", null, "_ingest._value", testProcessor, false),
            false);
        processor.execute(ingestDocument, (result, e) -> {});

        assertThat(testProcessor.getInvokedCounter(), equalTo(10));
        assertThat(
            visitedKeys.toArray(),
            arrayContainingInAnyOrder("foo1", "bar1", "baz1", "foo2", "bar2", "baz2", "foo3", "bar3", "baz3", "otherKey")
        );
        assertThat(visitedValues.toArray(), arrayContainingInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 42));
        assertThat(ingestDocument.getFieldValue("field", java.util.Map.class).entrySet().toArray(),
            arrayContainingInAnyOrder(
                Map.entry("foo", Map.of("foo1", 1, "bar2", 2, "baz1", 6)),
                Map.entry("bar", Map.of("foo2", 4, "bar2", 5, "baz2", 12)),
                Map.entry("baz", Map.of("foo3", 7, "bar2", 8, "baz3", 18, "otherKey", 42))
            )
        );
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, Collections.emptyMap()
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        TestProcessor testProcessor = new TestProcessor(doc -> {});
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "_ingest._value", testProcessor, true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertIngestDocument(originalIngestDocument, ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(0));
    }

    public void testAppendingToTheSameField() {
        java.util.Map<String, Object> source = Collections.singletonMap("field", Arrays.asList("a", "b"));
        IngestDocument originalIngestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, source);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        TestProcessor testProcessor = new TestProcessor(id->id.appendFieldValue("field", "a"));
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "field", testProcessor, true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(2));
        ingestDocument.removeField("_ingest._value");
        assertThat(ingestDocument, equalTo(originalIngestDocument));
    }

    public void testRemovingFromTheSameField() {
        java.util.Map<String, Object> source = Collections.singletonMap("field", Arrays.asList("a", "b"));
        IngestDocument originalIngestDocument = new IngestDocument("_index", "_id", "_type", null, null, null, source);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        TestProcessor testProcessor = new TestProcessor(id -> id.removeField("field.0"));
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "field", testProcessor, true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(2));
        ingestDocument.removeField("_ingest._value");
        assertThat(ingestDocument, equalTo(originalIngestDocument));
    }

    public void testMapIteration() {
        java.util.Map<String, Object> mapValue = Map.of("foo", 1, "bar", 2, "baz", 3);
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, Map.of("field", mapValue));

        List<String> encounteredKeys = new ArrayList<>();
        List<Object> encounteredValues = new ArrayList<>();
        TestProcessor testProcessor = new TestProcessor(id -> {
            String key = (String) id.getIngestMetadata().get("_key");
            Object value = id.getIngestMetadata().get("_value");
            encounteredKeys.add(key);
            encounteredValues.add(value);
            if (key.equals("bar")) {
                id.setFieldValue("_ingest._key", "bar2");
            }
            if (key.equals("baz")) {
                id.setFieldValue("_ingest._value", 33);
            }
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "field", testProcessor, true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(encounteredKeys.toArray(), arrayContainingInAnyOrder("foo", "bar", "baz"));
        assertThat(encounteredValues.toArray(), arrayContainingInAnyOrder(1, 2, 3));
        assertThat(ingestDocument.getFieldValue("field", java.util.Map.class).entrySet().toArray(),
            arrayContainingInAnyOrder(Map.entry("foo", 1), Map.entry("bar2", 2), Map.entry("baz", 33)));
    }

    public void testRemovalOfMapKey() {
        java.util.Map<String, Object> mapValue = Map.of("foo", 1, "bar", 2, "baz", 3);
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, Map.of("field", mapValue));

        List<String> encounteredKeys = new ArrayList<>();
        List<Object> encounteredValues = new ArrayList<>();
        TestProcessor testProcessor = new TestProcessor(id -> {
            String key = (String) id.getIngestMetadata().get("_key");
            encounteredKeys.add(key);
            encounteredValues.add(id.getIngestMetadata().get("_value"));
            if (key.equals("bar")) {
                id.setFieldValue("_ingest._key", "");
            }
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", null, "field", testProcessor, true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(encounteredKeys.toArray(), arrayContainingInAnyOrder("foo", "bar", "baz"));
        assertThat(encounteredValues.toArray(), arrayContainingInAnyOrder(1, 2, 3));
        assertThat(ingestDocument.getFieldValue("field", java.util.Map.class).entrySet().toArray(),
            arrayContainingInAnyOrder(Map.entry("foo", 1), Map.entry("baz", 3)));
    }

    public void testMapIterationWithAsyncProcessor() throws Exception {
        java.util.Map<String, Object> innerMap1 = Map.of("foo1", 1, "bar1", 2, "baz1", 3);
        java.util.Map<String, Object> innerMap2 = Map.of("foo2", 4, "bar2", 5, "baz2", 6);
        java.util.Map<String, Object> innerMap3 = Map.of("foo3", 7, "bar3", 8, "baz3", 9, "otherKey", 42);

        java.util.Map<String, Object> outerMap = Map.of("foo", innerMap1, "bar", innerMap2, "baz", innerMap3);
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, Map.of("field", outerMap));

        List<String> visitedKeys = new ArrayList<>();
        List<Object> visitedValues = new ArrayList<>();
        TestAsyncProcessor testProcessor = new TestAsyncProcessor(
            doc -> {
                String key = (String) doc.getIngestMetadata().get("_key");
                Object value = doc.getIngestMetadata().get("_value");
                visitedKeys.add(key);
                visitedValues.add(value);

                // change some of the keys
                if (key.startsWith("bar")) {
                    doc.setFieldValue("_ingest._key", "bar2");
                }
                // change some of the values
                if (key.startsWith("baz")) {
                    doc.setFieldValue("_ingest._value", ((Integer) value) * 2);
                }
            }
        );

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", null, "field", new ForEachProcessor("_tag", null, "_ingest._value", testProcessor, false),
            false);
        processor.execute(ingestDocument, (result, e) -> {});

        assertBusy(() -> {
            assertThat(testProcessor.getInvokedCounter(), equalTo(10));
            assertThat(
                visitedKeys.toArray(),
                arrayContainingInAnyOrder("foo1", "bar1", "baz1", "foo2", "bar2", "baz2", "foo3", "bar3", "baz3", "otherKey")
            );
            assertThat(visitedValues.toArray(), arrayContainingInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 42));
            assertThat(ingestDocument.getFieldValue("field", java.util.Map.class).entrySet().toArray(),
                arrayContainingInAnyOrder(
                    Map.entry("foo", Map.of("foo1", 1, "bar2", 2, "baz1", 6)),
                    Map.entry("bar", Map.of("foo2", 4, "bar2", 5, "baz2", 12)),
                    Map.entry("baz", Map.of("foo3", 7, "bar2", 8, "baz3", 18, "otherKey", 42))
                )
            );
        });
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

        @Override
        public String getDescription() {
            return "async uppercase processor description";
        }
    }

    private class TestAsyncProcessor implements Processor {

        private final Function<IngestDocument, IngestDocument> ingestDocumentMapper;
        private final AtomicInteger invokedCounter = new AtomicInteger();

        private TestAsyncProcessor(Consumer<IngestDocument> ingestDocumentConsumer) {
            this.ingestDocumentMapper = ingestDocument -> {
                ingestDocumentConsumer.accept(ingestDocument);
                return ingestDocument;
            };
        }

        @Override
        public void execute(IngestDocument document, BiConsumer<IngestDocument, Exception> handler) {
            new Thread(() -> {
                invokedCounter.incrementAndGet();
                try {
                    handler.accept(ingestDocumentMapper.apply(document), null);
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }).start();
        }

        public int getInvokedCounter() {
            return invokedCounter.get();
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            throw new UnsupportedOperationException("this is an async processor, don't call this");
        }

        @Override
        public String getType() {
            return "test-async-processor";
        }

        @Override
        public String getTag() {
            return getType();
        }

        @Override
        public String getDescription() {
            return "test async processor description";
        }
    }

}
