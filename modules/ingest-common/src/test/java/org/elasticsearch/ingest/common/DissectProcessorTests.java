/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.dissect.DissectException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Basic tests for the {@link DissectProcessor}. See the {@link org.elasticsearch.dissect.DissectParser} test suite for a comprehensive
 * set of dissect tests.
 */
public class DissectProcessorTests extends ESTestCase {

    public void testMatch() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1, null, null, Map.of("message", "foo,bar,baz"));
        DissectProcessor dissectProcessor = new DissectProcessor("", null, "message", "%{a},%{b},%{c}", "", true);
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("b", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("c", String.class), equalTo("baz"));
    }

    public void testMatchOverwrite() {
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1,
            null,
            null,
            Map.of("message", "foo,bar,baz", "a", "willgetstompped")
        );
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("willgetstompped"));
        DissectProcessor dissectProcessor = new DissectProcessor("", null, "message", "%{a},%{b},%{c}", "", true);
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("b", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("c", String.class), equalTo("baz"));
    }

    public void testAdvancedMatch() {
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1,
            null,
            null,
            Map.of("message", "foo       bar,,,,,,,baz nope:notagain 😊 🐇 🙃")
        );
        DissectProcessor dissectProcessor = new DissectProcessor(
            "",
            null,
            "message",
            "%{a->} %{*b->},%{&b} %{}:%{?skipme} %{+smile/2} 🐇 %{+smile/1}",
            "::::",
            true
        );
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("bar", String.class), equalTo("baz"));
        expectThrows(IllegalArgumentException.class, () -> ingestDocument.getFieldValue("nope", String.class));
        expectThrows(IllegalArgumentException.class, () -> ingestDocument.getFieldValue("notagain", String.class));
        assertThat(ingestDocument.getFieldValue("smile", String.class), equalTo("🙃::::😊"));
    }

    public void testMiss() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1, null, null, Map.of("message", "foo:bar,baz"));
        DissectProcessor dissectProcessor = new DissectProcessor("", null, "message", "%{a},%{b},%{c}", "", true);
        DissectException e = expectThrows(DissectException.class, () -> dissectProcessor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("Unable to find match for dissect pattern"));
    }

    public void testNonStringValueWithIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", null, fieldName, "%{a},%{b},%{c}", "", true);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", null, fieldName, "%{a},%{b},%{c}", "", true);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap(fieldName, null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValueWithOutIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", null, fieldName, "%{a},%{b},%{c}", "", false);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap(fieldName, null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
    }
}
