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
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.ingest.IngestDocument.Metadata;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class SetProcessorTests extends ESTestCase {

    public void testSetExistingFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        while (Metadata.isMetadata(fieldName)) {
            fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        }
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, null, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetNewFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        // used to verify that there are no conflicts between subsequent fields going to be added
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), testIngestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, fieldValue, null, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetFieldsTypeMismatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue("field", "value");
        Processor processor = createSetProcessor("field.inner", "value", null, true, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(
            exception.getMessage(),
            equalTo("cannot set [inner] with parent object of type [java.lang.String] as part of path [field.inner]")
        );
    }

    public void testSetNewFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, null, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Object fieldValue = "foo";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, "bar", null, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingNullFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Object fieldValue = null;
        Object newValue = "bar";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, newValue, null, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(newValue));
    }

    public void testSetMetadataExceptVersion() throws Exception {
        Metadata randomMetadata = randomFrom(Metadata.INDEX, Metadata.ID, Metadata.ROUTING);
        Processor processor = createSetProcessor(randomMetadata.getFieldName(), "_value", null, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(randomMetadata.getFieldName(), String.class), equalTo("_value"));
    }

    public void testSetMetadataVersion() throws Exception {
        long version = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.VERSION.getFieldName(), version, null, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION.getFieldName(), Long.class), equalTo(version));
    }

    public void testSetMetadataVersionType() throws Exception {
        String versionType = randomFrom("internal", "external", "external_gte");
        Processor processor = createSetProcessor(Metadata.VERSION_TYPE.getFieldName(), versionType, null, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION_TYPE.getFieldName(), String.class), equalTo(versionType));
    }

    public void testSetMetadataIfSeqNo() throws Exception {
        long ifSeqNo = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_SEQ_NO.getFieldName(), ifSeqNo, null, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_SEQ_NO.getFieldName(), Long.class), equalTo(ifSeqNo));
    }

    public void testSetMetadataIfPrimaryTerm() throws Exception {
        long ifPrimaryTerm = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_PRIMARY_TERM.getFieldName(), ifPrimaryTerm, null, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_PRIMARY_TERM.getFieldName(), Long.class), equalTo(ifPrimaryTerm));
    }

    public void testSetDynamicTemplates() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int iters = between(1, 3);
        for (int i = 0; i < iters; i++) {
            Map<String, String> dynamicTemplates = IntStream.range(0, between(0, 3))
                .boxed()
                .collect(Collectors.toMap(n -> "field-" + n, n -> randomFrom("int", "geo_point", "keyword")));
            Processor processor = createSetProcessor(Metadata.DYNAMIC_TEMPLATES.getFieldName(), dynamicTemplates, null, true, false);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getFieldValue(Metadata.DYNAMIC_TEMPLATES.getFieldName(), Map.class), equalTo(dynamicTemplates));
        }
    }

    public void testCopyFromOtherField() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        document.put("field", fieldValue);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        String fieldName;
        if (document.size() > 1) {
            // select an existing field as target if one exists other than the copy_from field
            do {
                fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
            } while (fieldName.equals("field") || fieldName.startsWith("field."));
        } else {
            // otherwise make up a new target field
            fieldName = randomAlphaOfLength(6);
        }

        Processor processor = createSetProcessor(fieldName, null, "field", true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        Object copiedValue = ingestDocument.getFieldValue(fieldName, Object.class);
        if (fieldValue instanceof Map) {
            assertMapEquals(copiedValue, fieldValue);
        } else {
            assertThat(copiedValue, equalTo(fieldValue));
        }
    }

    public void testCopyFromDeepCopiesNonPrimitiveMutableTypes() throws Exception {
        final String originalField = "originalField";
        final String targetField = "targetField";
        Processor processor = createSetProcessor(targetField, null, originalField, true, false);

        // map types
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> originalMap = new HashMap<>();
        originalMap.put("foo", "bar");
        document.put(originalField, originalMap);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IngestDocument output = processor.execute(ingestDocument);
        originalMap.put("foo", "not-bar");
        Map<?, ?> outputMap = output.getFieldValue(targetField, Map.class);
        assertThat(outputMap.get("foo"), equalTo("bar"));

        // set types
        document = new HashMap<>();
        Set<String> originalSet = randomUnique(() -> randomAlphaOfLength(5), 5);
        Set<String> preservedSet = new HashSet<>(originalSet);
        document.put(originalField, originalSet);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        originalSet.add(randomValueOtherThanMany(originalSet::contains, () -> randomAlphaOfLength(5)));
        assertThat(ingestDocument.getFieldValue(targetField, Object.class), equalTo(preservedSet));

        // list types
        document = new HashMap<>();
        List<String> originalList = randomList(1, 5, () -> randomAlphaOfLength(5));
        List<String> preservedList = new ArrayList<>(originalList);
        document.put(originalField, originalList);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        originalList.add(randomValueOtherThanMany(originalList::contains, () -> randomAlphaOfLength(5)));
        assertThat(ingestDocument.getFieldValue(targetField, Object.class), equalTo(preservedList));

        // byte[] types
        document = new HashMap<>();
        byte[] originalBytes = randomByteArrayOfLength(10);
        byte[] preservedBytes = new byte[originalBytes.length];
        System.arraycopy(originalBytes, 0, preservedBytes, 0, originalBytes.length);
        document.put(originalField, originalBytes);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        originalBytes[0] = originalBytes[0] == 0 ? (byte) 1 : (byte) 0;
        assertThat(ingestDocument.getFieldValue(targetField, Object.class), equalTo(preservedBytes));

        // Date types
        document = new HashMap<>();
        Date originalDate = new Date();
        Date preservedDate = new Date(originalDate.getTime());
        document.put(originalField, originalDate);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        originalDate.setTime(originalDate.getTime() + 1);
        assertThat(ingestDocument.getFieldValue(targetField, Object.class), equalTo(preservedDate));

        // String[] types — e.g. the result of String#split() in a script processor
        document = new HashMap<>();
        String[] originalArray = "foo.bar.baz".split("\\.");
        document.put(originalField, originalArray);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        String[] copiedArray = ingestDocument.getFieldValue(targetField, String[].class);
        assertThat(copiedArray, arrayContainingInAnyOrder("foo", "bar", "baz"));
        assertNotSame(originalArray, copiedArray);
        originalArray[0] = "mutated";
        assertThat(ingestDocument.getFieldValue(targetField, String[].class), arrayContainingInAnyOrder("foo", "bar", "baz"));
    }

    public void testSetEmptyField() {
        // edge case: it's valid (according to the current validation) to *create* a set processor that has an empty string as its 'field',
        // but it will fail at ingest execution time.
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Processor processor = createSetProcessor("", "some_value", null, false, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path cannot be null nor empty"));
    }

    public void testSetWithConstantFieldTemplate() throws Exception {
        // unlike the mock template that the other tests use, a template that really is constant is one that can be pre-parsed
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("foo.bar.baz", "value", null, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo.bar.baz", String.class), equalTo("value"));
    }

    public void testSetWithConstantFieldTemplateAndOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("foo.bar", "existing");
        Processor processor = createSetProcessorWithCompiledField("foo.bar", "new", null, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo.bar", String.class), equalTo("existing"));
        processor = createSetProcessorWithCompiledField("foo.baz", "new", null, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo.baz", String.class), equalTo("new"));
    }

    public void testCopyFromWithConstantFieldTemplate() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("source.field", List.of("a", "b"));
        Processor processor = createSetProcessorWithCompiledField("target.field", null, "source.field", true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.field", List.class), equalTo(List.of("a", "b")));
    }

    public void testCopyFromMissingFieldWithIgnoreEmptyValue() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("target.field", null, "source.missing", true, true);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target.field"), equalTo(false));
        Processor strict = createSetProcessorWithCompiledField("target.field", null, "source.missing", true, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> strict.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source] not present as part of path [source.missing]"));
    }

    /**
     * Pipelines are stored, so one that has a malformed path in it has to still be loadable (that is, creating the processor must not
     * throw), and then it has to fail, as it always has, when a document is run through it.
     */
    public void testMalformedConstantFieldFailsAtExecutionTime() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("a..b", "value", null, true, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testMalformedCopyFromFailsAtExecutionTime() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("target", null, "a..b", true, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path [a..b] is not valid"));
    }

    /**
     * Where a value is written to is only looked at when there turns out to be something to write (which, with ignore_empty_value, it
     * might not be, for example because the field that it is copied from is missing). That matters for a field that is a template, as
     * it can render as nothing (or as something that is not a path) for some documents and not for others.
     */
    public void testFieldIsNotLookedAtIfThereIsNothingToWrite() throws Exception {
        TemplateScript.Factory constantEmpty = ConfigurationUtils.compileTemplate(
            SetProcessor.TYPE,
            "tag",
            "field",
            "",
            TestTemplateService.instance()
        );
        List<TemplateScript.Factory> fields = List.of(
            constantEmpty,
            new TestTemplateService.MockTemplateScript.Factory(""),
            new TestTemplateService.MockTemplateScript.Factory("a..b")
        );
        for (TemplateScript.Factory field : fields) {
            IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
            Processor processor = new SetProcessor("tag", null, field, null, "source.missing", true, true);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getSource(), equalTo(Map.of()));
        }
    }

    public void testMalformedFieldIsReportedIfThereIsSomethingToWrite() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("source.present", "value");
        Processor processor = createSetProcessorWithCompiledField("a..b", null, "source.present", true, true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path [a..b] is not valid"));
    }

    /**
     * A field that is a template is rendered as often as it has to be and no more: once if there is something to write (whether or not
     * override is on), and not at all if there is nothing to write and nothing else needs to look at it.
     */
    public void testTemplatedFieldIsRenderedAsOftenAsItHasToBe() throws Exception {
        AtomicInteger renders = new AtomicInteger();
        TemplateScript.Factory field = params -> new TemplateScript(params) {
            @Override
            public String execute() {
                renders.incrementAndGet();
                return "target";
            }
        };
        for (boolean overrideEnabled : new boolean[] { true, false }) {
            // something to write
            IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
            ingestDocument.setFieldValue("source", "value");
            renders.set(0);
            new SetProcessor("tag", null, field, null, "source", overrideEnabled, true).execute(ingestDocument);
            assertThat(ingestDocument.getFieldValue("target", String.class), equalTo("value"));
            assertThat("override " + overrideEnabled, renders.get(), equalTo(1));

            // nothing to write: with override the field is not looked at at all, without it the field has to be looked at (once)
            ingestDocument = TestIngestDocument.emptyIngestDocument();
            renders.set(0);
            new SetProcessor("tag", null, field, null, "source.missing", overrideEnabled, true).execute(ingestDocument);
            assertThat(ingestDocument.getSource(), equalTo(Map.of()));
            assertThat("override " + overrideEnabled, renders.get(), equalTo(overrideEnabled ? 0 : 1));
        }
    }

    /**
     * Without override the field has to be looked at before anything is written, to see whether there is something there already.
     */
    public void testMalformedFieldIsReportedWithoutOverrideEvenIfThereIsNothingToWrite() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("a..b", null, "source.missing", false, true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testEmptyConstantFieldFailsAtExecutionTime() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        Processor processor = createSetProcessorWithCompiledField("", "value", null, true, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("path cannot be null nor empty"));
    }

    /**
     * Like {@link #createSetProcessor}, except that the field template is compiled the way that the processor factory compiles it,
     * which (for a field name that has no templating in it) gives a constant template rather than a mock one.
     */
    private static Processor createSetProcessorWithCompiledField(
        String fieldName,
        Object fieldValue,
        String copyFrom,
        boolean overrideEnabled,
        boolean ignoreEmptyValue
    ) {
        String tag = randomAlphaOfLength(10);
        return new SetProcessor(
            tag,
            null,
            ConfigurationUtils.compileTemplate(SetProcessor.TYPE, tag, "field", fieldName, TestTemplateService.instance()),
            fieldValue == null ? null : ValueSource.wrap(fieldValue, TestTemplateService.instance()),
            copyFrom,
            overrideEnabled,
            ignoreEmptyValue
        );
    }

    private static Processor createSetProcessor(
        String fieldName,
        Object fieldValue,
        String copyFrom,
        boolean overrideEnabled,
        boolean ignoreEmptyValue
    ) {
        return new SetProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(fieldName),
            ValueSource.wrap(fieldValue, TestTemplateService.instance()),
            copyFrom,
            overrideEnabled,
            ignoreEmptyValue
        );
    }

    private static void assertMapEquals(Object actual, Object expected) {
        if (expected instanceof Map<?, ?> expectedMap) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            assertThat(actualMap.keySet().toArray(), arrayContainingInAnyOrder(expectedMap.keySet().toArray()));
            for (Map.Entry<?, ?> entry : actualMap.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    assertMapEquals(entry.getValue(), expectedMap.get(entry.getKey()));
                } else {
                    assertThat(entry.getValue(), equalTo(expectedMap.get(entry.getKey())));
                }
            }
        }
    }
}
