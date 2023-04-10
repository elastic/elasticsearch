/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.common.JsonProcessor.ConflictStrategy.MERGE;
import static org.elasticsearch.ingest.common.JsonProcessor.ConflictStrategy.REPLACE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class JsonProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testExecute() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        String randomField = randomAlphaOfLength(3);
        String randomTargetField = randomAlphaOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, randomField, randomTargetField, false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();

        Map<String, Object> randomJsonMap = RandomDocumentPicks.randomSource(random());
        XContentBuilder builder = JsonXContent.contentBuilder().map(randomJsonMap);
        String randomJson = XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        document.put(randomField, randomJson);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        Map<String, Object> jsonified = ingestDocument.getFieldValue(randomTargetField, Map.class);
        assertEquals(ingestDocument.getFieldValue(randomTargetField, Object.class), jsonified);
    }

    public void testInvalidValue() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", "blah blah");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Unrecognized token 'blah': " + "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')"
            )
        );
    }

    public void testByteArray() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", new byte[] { 0, 1 });
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(
            exception.getCause().getMessage(),
            containsString("Unrecognized token 'B': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')")
        );
    }

    public void testNull() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertNull(ingestDocument.getFieldValue("target_field", Object.class));
    }

    public void testBoolean() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        boolean value = true;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testInteger() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        int value = 3;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testDouble() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        double value = 3.0;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testString() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        String value = "hello world";
        document.put("field", "\"" + value + "\"");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testArray() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        List<Boolean> value = List.of(true, true, false);
        document.put("field", value.toString());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testFieldMissing() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public void testAddToRoot() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        String randomTargetField = randomAlphaOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, "a", randomTargetField, true, REPLACE, false);
        Map<String, Object> document = new HashMap<>();

        String json = "{\"a\": 1, \"b\": 2}";
        document.put("a", json);
        document.put("c", "see");

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertEquals(1, sourceAndMetadata.get("a"));
        assertEquals(2, sourceAndMetadata.get("b"));
        assertEquals("see", sourceAndMetadata.get("c"));
    }

    public void testDuplicateKeys() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        JsonProcessor lenientJsonProcessor = new JsonProcessor(processorTag, null, "a", null, true, REPLACE, true);

        Map<String, Object> document = new HashMap<>();
        String json = "{\"a\": 1, \"a\": 2}";
        document.put("a", json);
        document.put("c", "see");

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        lenientJsonProcessor.execute(ingestDocument);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertEquals(2, sourceAndMetadata.get("a"));
        assertEquals("see", sourceAndMetadata.get("c"));

        JsonProcessor strictJsonProcessor = new JsonProcessor(processorTag, null, "a", null, true, REPLACE, false);
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> strictJsonProcessor.execute(RandomDocumentPicks.randomIngestDocument(random(), document))
        );
        assertThat(exception.getMessage(), containsString("Duplicate field 'a'"));
    }

    public void testAddToRootRecursiveMerge() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, "json", null, true, MERGE, false);

        Map<String, Object> document = new HashMap<>();
        String json = """
            {"foo": {"bar": "baz"}}""";
        document.put("json", json);
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar", "override_me");
        inner.put("qux", "quux");
        document.put("foo", inner);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);

        assertEquals("baz", ingestDocument.getFieldValue("foo.bar", String.class));
        assertEquals("quux", ingestDocument.getFieldValue("foo.qux", String.class));
    }

    public void testAddToRootNonRecursiveMerge() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, "json", null, true, REPLACE, false);

        Map<String, Object> document = new HashMap<>();
        String json = """
            {"foo": {"bar": "baz"}}""";
        document.put("json", json);
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar", "override_me");
        inner.put("qux", "quux");
        document.put("foo", inner);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);

        assertEquals("baz", ingestDocument.getFieldValue("foo.bar", String.class));
        assertFalse(ingestDocument.hasField("foo.qux"));
    }

    public void testAddBoolToRoot() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", true, REPLACE, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", true);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("cannot add non-map fields to root of document"));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testApply() {
        {
            Object result = JsonProcessor.apply("{\"foo\":\"bar\"}", true, true);
            assertThat(result, instanceOf(Map.class));
            Map resultMap = (Map) result;
            assertThat(resultMap.size(), equalTo(1));
            assertThat(resultMap.get("foo"), equalTo("bar"));
        }
        {
            Object result = JsonProcessor.apply("\"foo\"", true, true);
            assertThat(result, instanceOf(String.class));
            assertThat(result, equalTo("foo"));
        }
        {
            boolean boolValue = randomBoolean();
            Object result = JsonProcessor.apply(Boolean.toString(boolValue), true, true);
            assertThat(result, instanceOf(Boolean.class));
            assertThat(result, equalTo(boolValue));
        }
        {
            double value = randomDouble();
            Object result = JsonProcessor.apply(Double.toString(value), true, true);
            assertThat(result, instanceOf(Double.class));
            assertThat((double) result, closeTo(value, .001));
        }
        {
            List<Double> list = randomList(10, ESTestCase::randomDouble);
            String value = list.stream().map(val -> Double.toString(val)).collect(Collectors.joining(",", "[", "]"));
            Object result = JsonProcessor.apply(value, true, true);
            assertThat(result, instanceOf(List.class));
            List<Double> resultList = (List<Double>) result;
            assertThat(resultList.size(), equalTo(list.size()));
            for (int i = 0; i < list.size(); i++) {
                assertThat(resultList.get(i), closeTo(list.get(i), .001));
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testApplyWithInvalidJson() {
        /*
         * The following fail whether strictJsonParsing is set to true or false. The reason is that even the first token cannot be parsed
         *  as JSON (since the first token is a not a primitive or an object -- just characters not in quotes).
         */
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("foo", true, true));
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("foo", true, false));
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("foo [360113.865822] wbrdg-0afe001ce", true, true));
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("foo [360113.865822] wbrdg-0afe001ce", true, false));

        /*
         * The following are examples of malformed json but the first part of each is valid json. Previously apply parsed just the first
         * token and ignored the rest, but it now throw an IllegalArgumentException unless strictJsonParsing is set to false. See
         * https://github.com/elastic/elasticsearch/issues/92898.
         */
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("123 foo", true, true));
        expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("45 this is {\"a\": \"json\"}", true, true));

        {
            expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("[360113.865822] wbrdg-0afe001ce", true, true));
            Object result = JsonProcessor.apply("[360113.865822] wbrdg-0afe001ce", true, false);
            assertThat(result, instanceOf(List.class));
            List<Double> resultList = (List<Double>) result;
            assertThat(resultList.size(), equalTo(1));
            assertThat(resultList.get(0), closeTo(360113.865822, .001));
        }
        {
            expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply("{\"foo\":\"bar\"} wbrdg-0afe00e", true, true));
            Object result = JsonProcessor.apply("{\"foo\":\"bar\"} wbrdg-0afe00e", true, false);
            assertThat(result, instanceOf(Map.class));
            Map resultMap = (Map) result;
            assertThat(resultMap.size(), equalTo(1));
            assertThat(resultMap.get("foo"), equalTo("bar"));
        }
        {
            expectThrows(IllegalArgumentException.class, () -> JsonProcessor.apply(" 1268 : TimeOut = 123 : a", true, true));
            Object result = JsonProcessor.apply(" 1268 : TimeOut = 123 : a", true, false);
            assertThat(result, instanceOf(Integer.class));
            assertThat(result, equalTo(1268));
        }
    }
}
