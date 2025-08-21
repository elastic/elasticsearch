/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class RenameProcessorTests extends ESTestCase {

    public void testRename() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(fieldName, Object.class);
        String newFieldName;
        do {
            newFieldName = RandomDocumentPicks.randomFieldName(random());
        } while (RandomDocumentPicks.canAddField(newFieldName, ingestDocument) == false || newFieldName.equals(fieldName));
        Processor processor = createRenameProcessor(fieldName, newFieldName, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), equalTo(fieldValue));
    }

    public void testRenameArrayElement() throws Exception {
        Map<String, Object> document = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        list.add("item3");
        document.put("list", list);
        List<Map<String, String>> one = new ArrayList<>();
        one.add(Map.of("one", "one"));
        one.add(Map.of("two", "two"));
        document.put("one", one);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Processor processor = createRenameProcessor("list.0", "item", false, false);
        processor.execute(ingestDocument);
        Object actualObject = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(actualObject, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> actualList = (List<String>) actualObject;
        assertThat(actualList.size(), equalTo(2));
        assertThat(actualList.get(0), equalTo("item2"));
        assertThat(actualList.get(1), equalTo("item3"));
        actualObject = ingestDocument.getSourceAndMetadata().get("item");
        assertThat(actualObject, instanceOf(String.class));
        assertThat(actualObject, equalTo("item1"));

        processor = createRenameProcessor("list.0", "list.3", false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[3] is out of bounds for array with length [2] as part of path [list.3]"));
            assertThat(actualList.size(), equalTo(2));
            assertThat(actualList.get(0), equalTo("item2"));
            assertThat(actualList.get(1), equalTo("item3"));
        }
    }

    public void testRenameNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createRenameProcessor(fieldName, RandomDocumentPicks.randomFieldName(random()), false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] doesn't exist"));
        }
    }

    public void testRenameNonExistingFieldWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createRenameProcessor(fieldName, RandomDocumentPicks.randomFieldName(random()), true, false);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);

        Processor processor1 = createRenameProcessor("", RandomDocumentPicks.randomFieldName(random()), true, false);
        processor1.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testRenameNewFieldAlreadyExists() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Processor processor = createRenameProcessor(
            RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument),
            fieldName,
            false,
            false
        );
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] already exists"));
        }
    }

    public void testRenameExistingFieldNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, (Object) null);
        String newFieldName = randomValueOtherThanMany(ingestDocument::hasField, () -> RandomDocumentPicks.randomFieldName(random()));
        Processor processor = createRenameProcessor(fieldName, newFieldName, false, false);
        processor.execute(ingestDocument);
        if (newFieldName.startsWith(fieldName + '.')) {
            assertThat(ingestDocument.getFieldValue(fieldName, Object.class), instanceOf(Map.class));
        } else {
            assertThat(ingestDocument.hasField(fieldName), equalTo(false));
        }
        assertThat(ingestDocument.hasField(newFieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), nullValue());
    }

    public void testRenameAtomicOperationSetFails() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_index", "foobar");

        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(metadata);
        Processor processor = createRenameProcessor("_index", "_version_type", false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            // the set failed, the old field has not been removed
            assertThat(
                e.getMessage(),
                equalTo(
                    "_version_type must be a null or one of [internal, external, external_gte] "
                        + "but was [foobar] with type [java.lang.String]"
                )
            );
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("_index"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("_version_type"), equalTo(false));
        }
    }

    public void testRenameAtomicOperationRemoveFails() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");

        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(metadata);
        Processor processor = createRenameProcessor("_version", "new_field", false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            // the remove failed, the old field has not been removed
            assertThat(e.getMessage(), equalTo("_version cannot be removed"));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("_version"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(false));
        }
    }

    public void testRenameLeafIntoBranch() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(source);
        Processor processor1 = createRenameProcessor("foo", "foo.bar", false, false);
        processor1.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo", Map.class), equalTo(Map.of("bar", "bar")));
        assertThat(ingestDocument.getFieldValue("foo.bar", String.class), equalTo("bar"));

        Processor processor2 = createRenameProcessor("foo.bar", "foo.bar.baz", false, false);
        processor2.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo", Map.class), equalTo(Map.of("bar", Map.of("baz", "bar"))));
        assertThat(ingestDocument.getFieldValue("foo.bar", Map.class), equalTo(Map.of("baz", "bar")));
        assertThat(ingestDocument.getFieldValue("foo.bar.baz", String.class), equalTo("bar"));

        // try to restore it (will fail, not allowed without the override flag)
        Processor processor3 = createRenameProcessor("foo.bar.baz", "foo", false, false);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor3.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [foo] already exists"));
    }

    public void testRenameOverride() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("event.original", "existing_message");
        source.put("message", "new_message");
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(source);
        Processor processor1 = createRenameProcessor("message", "event.original", false, true);
        processor1.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("event.original", String.class), equalTo("new_message"));
    }

    private RenameProcessor createRenameProcessor(String field, String targetField, boolean ignoreMissing, boolean overrideEnabled) {
        return new RenameProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(field),
            new TestTemplateService.MockTemplateScript.Factory(targetField),
            ignoreMissing,
            overrideEnabled
        );
    }
}
