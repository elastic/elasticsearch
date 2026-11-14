/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
        Object actualObject = ingestDocument.getSource().get("list");
        assertThat(actualObject, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> actualList = (List<String>) actualObject;
        assertThat(actualList.size(), equalTo(2));
        assertThat(actualList.get(0), equalTo("item2"));
        assertThat(actualList.get(1), equalTo("item3"));
        actualObject = ingestDocument.getSource().get("item");
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
            assertThat(ingestDocument.getMetadata().containsKey("_index"), equalTo(true));
            assertThat(ingestDocument.getMetadata().containsKey("_version_type"), equalTo(false));
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
            assertThat(ingestDocument.getMetadata().containsKey("_version"), equalTo(true));
            assertThat(ingestDocument.getSource().containsKey("new_field"), equalTo(false));
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

    /**
     * An empty field is treated as one that is missing rather than as an invalid path. That matters for a template that renders as
     * nothing when the document doesn't have something that it refers to, combined with ignore_missing: the processor does nothing if
     * ignore_missing is set, and otherwise it fails because the field doesn't exist. A field that is empty for every document is
     * treated the same way. This is the same for the kv, rename and remove processors.
     */
    public void testEmptyFieldIsTreatedAsMissing() throws Exception {
        TemplateScript.Factory constantEmpty = ConfigurationUtils.compileTemplate(
            RenameProcessor.TYPE,
            "tag",
            "field",
            "",
            TestTemplateService.instance()
        );
        // a constant template, which is empty for every document, and one that really is rendered for every document
        for (TemplateScript.Factory emptyField : List.of(constantEmpty, new TestTemplateService.MockTemplateScript.Factory(""))) {
            TemplateScript.Factory target = new TestTemplateService.MockTemplateScript.Factory("target");

            IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
            ingestDocument.setFieldValue("foo", "bar");
            new RenameProcessor("tag", null, emptyField, target, true, false).execute(ingestDocument);
            assertThat(ingestDocument.getSource(), equalTo(Map.of("foo", "bar")));

            RenameProcessor notIgnoringMissing = new RenameProcessor("tag", null, emptyField, target, false, false);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> notIgnoringMissing.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("field [] doesn't exist"));
        }
    }

    /**
     * Unlike an empty field, an empty target field is not treated as a missing one, so it is an error whether or not ignore_missing is
     * set (which is only about the field that is being renamed). It is reported before the field that is being renamed is removed, so
     * that the document is left as it was.
     */
    public void testEmptyTargetFieldIsInvalid() throws Exception {
        TemplateScript.Factory constantEmpty = ConfigurationUtils.compileTemplate(
            RenameProcessor.TYPE,
            "tag",
            "target_field",
            "",
            TestTemplateService.instance()
        );
        // a constant template, which is empty for every document, and one that really is rendered for every document
        for (TemplateScript.Factory emptyTarget : List.of(constantEmpty, new TestTemplateService.MockTemplateScript.Factory(""))) {
            for (boolean ignoreMissing : new boolean[] { true, false }) {
                IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
                ingestDocument.setFieldValue("foo", "bar");
                RenameProcessor processor = new RenameProcessor(
                    "tag",
                    null,
                    new TestTemplateService.MockTemplateScript.Factory("foo"),
                    emptyTarget,
                    ignoreMissing,
                    randomBoolean()
                );
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
                assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
                assertThat(ingestDocument.getSource(), equalTo(Map.of("foo", "bar")));
            }
        }
    }

    /**
     * A target field that can't be used is reported before the field that is being renamed is removed, so that field is not lost. That
     * matters most for an element of an array: removing it shifts the elements after it down, so (unlike for a field of an object)
     * putting the value back where it came from is not the same as never having removed it.
     */
    public void testMalformedTargetFieldDoesNotLoseTheField() throws Exception {
        // a field of an object
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("foo", "bar");
        Processor processor = createRenameProcessor("foo", "a..b", false, randomBoolean());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
        assertThat(ingestDocument.getSource(), equalTo(Map.of("foo", "bar")));

        // an element of an array
        IngestDocument arrayDocument = TestIngestDocument.emptyIngestDocument();
        arrayDocument.setFieldValue("list", new ArrayList<>(List.of("item1", "item2", "item3")));
        Processor arrayProcessor = createRenameProcessor("list.0", "a..b", false, randomBoolean());
        e = expectThrows(IllegalArgumentException.class, () -> arrayProcessor.execute(arrayDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
        assertThat(arrayDocument.getFieldValue("list", List.class), equalTo(List.of("item1", "item2", "item3")));
    }

    /**
     * The field and the target field are templates, which are rendered as often as they have to be and no more: once each for every
     * document (however many times the processor looks at them), and the target not at all if there is nothing to rename.
     */
    public void testTemplatedFieldsAreRenderedOncePerExecute() throws Exception {
        AtomicInteger fieldRenders = new AtomicInteger();
        AtomicInteger targetRenders = new AtomicInteger();
        TemplateScript.Factory field = countingTemplate("source", fieldRenders);
        TemplateScript.Factory target = countingTemplate("target", targetRenders);

        for (boolean overrideEnabled : new boolean[] { true, false }) {
            // something to rename
            IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
            ingestDocument.setFieldValue("source", "value");
            fieldRenders.set(0);
            targetRenders.set(0);
            new RenameProcessor("tag", null, field, target, false, overrideEnabled).execute(ingestDocument);
            assertThat(ingestDocument.getSource(), equalTo(Map.of("target", "value")));
            assertThat("override " + overrideEnabled, fieldRenders.get(), equalTo(1));
            assertThat("override " + overrideEnabled, targetRenders.get(), equalTo(1));

            // nothing to rename
            ingestDocument = TestIngestDocument.emptyIngestDocument();
            fieldRenders.set(0);
            targetRenders.set(0);
            new RenameProcessor("tag", null, field, target, true, overrideEnabled).execute(ingestDocument);
            assertThat(ingestDocument.getSource(), equalTo(Map.of()));
            assertThat("override " + overrideEnabled, fieldRenders.get(), equalTo(1));
            assertThat("override " + overrideEnabled, targetRenders.get(), equalTo(0));
        }
    }

    private static TemplateScript.Factory countingTemplate(String rendersAs, AtomicInteger renders) {
        return params -> new TemplateScript(params) {
            @Override
            public String execute() {
                renders.incrementAndGet();
                return rendersAs;
            }
        };
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
