/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JsonProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testExecute() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        String randomField = randomAlphaOfLength(3);
        String randomTargetField = randomAlphaOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, randomField, randomTargetField, false);
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
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", "blah blah");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getCause().getMessage(), containsString("Unrecognized token 'blah': " +
            "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')"));
    }

    public void testByteArray() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", new byte[] { 0, 1 });
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Unrecognized token 'B': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')"
            )
        );
    }

    public void testNull() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertNull(ingestDocument.getFieldValue("target_field", Object.class));
    }

    public void testBoolean() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        boolean value = true;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testInteger() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        int value = 3;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testDouble() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        double value = 3.0;
        document.put("field", value);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testString() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        String value = "hello world";
        document.put("field", "\"" + value + "\"");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testArray() throws Exception {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        List<Boolean> value = Arrays.asList(true, true, false);
        document.put("field", value.toString());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(value));
    }

    public void testFieldMissing() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public void testAddToRoot() throws Exception {
        String processorTag = randomAlphaOfLength(3);
        String randomTargetField = randomAlphaOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, null, "a", randomTargetField, true);
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

    public void testAddBoolToRoot() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", null, "field", "target_field", true);
        Map<String, Object> document = new HashMap<>();
        document.put("field", true);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("cannot add non-map fields to root of document"));
    }
}
