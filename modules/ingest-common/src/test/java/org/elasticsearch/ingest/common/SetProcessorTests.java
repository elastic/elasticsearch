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
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot set [inner] with parent object of type [java.lang.String] as " + "part of path [field.inner]")
            );
        }
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
}
