/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SplitProcessorTests extends ESTestCase {

    public void testSplit() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(List.of("127", "0", "0", "1")));
    }

    public void testSplitFieldNotFound() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testSplitNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, "field", "\\.", false, false, "field");
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot split."));
        }
    }

    public void testSplitNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap(fieldName, null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", true, false, fieldName);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, "field", "\\.", true, false, "field");
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitNonStringValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast " + "to [java.lang.String]")
            );
        }
    }

    public void testSplitAppendable() throws Exception {
        Map<String, Object> splitConfig = new HashMap<>();
        splitConfig.put("field", "flags");
        splitConfig.put("separator", "\\|");
        Processor splitProcessor = (new SplitProcessor.Factory()).create(null, null, null, splitConfig);
        Map<String, Object> source = new HashMap<>();
        source.put("flags", "new|hot|super|fun|interesting");
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(source);
        splitProcessor.execute(ingestDocument);
        @SuppressWarnings("unchecked")
        List<String> flags = (List<String>) ingestDocument.getFieldValue("flags", List.class);
        assertThat(flags, equalTo(List.of("new", "hot", "super", "fun", "interesting")));
        ingestDocument.appendFieldValue("flags", "additional_flag");
        assertThat(
            ingestDocument.getFieldValue("flags", List.class),
            equalTo(List.of("new", "hot", "super", "fun", "interesting", "additional_flag"))
        );
    }

    public void testSplitWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
        String targetFieldName = fieldName + randomAlphaOfLength(5);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, List.class), equalTo(List.of("127", "0", "0", "1")));
    }

    public void testSplitWithPreserveTrailing() throws Exception {
        doTestSplitWithPreserveTrailing(true, "foo|bar|baz||", List.of("foo", "bar", "baz", "", ""));
    }

    public void testSplitWithoutPreserveTrailing() throws Exception {
        doTestSplitWithPreserveTrailing(false, "foo|bar|baz||", List.of("foo", "bar", "baz"));
    }

    private void doTestSplitWithPreserveTrailing(boolean preserveTrailing, String fieldValue, List<String> expected) throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\|", false, preserveTrailing, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expected));
    }
}
