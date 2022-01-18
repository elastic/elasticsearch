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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractStringProcessorTestCase<T> extends ESTestCase {

    protected abstract AbstractStringProcessor<T> newProcessor(String field, boolean ignoreMissing, String targetField);

    protected String modifyInput(String input) {
        return input;
    }

    protected abstract T expectedResult(String input);

    protected Class<?> expectedResultType() {
        return String.class;  // most results types are Strings
    }

    public void testProcessor() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldValue;
        String fieldName;
        String modifiedFieldValue;
        do {
            fieldValue = RandomDocumentPicks.randomString(random());
            modifiedFieldValue = modifyInput(fieldValue);
            fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifiedFieldValue);
        } while (isSupportedValue(modifiedFieldValue) == false);
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), equalTo(expectedResult(fieldValue)));

        int numItems = randomIntBetween(1, 10);
        List<String> fieldValueList = new ArrayList<>();
        List<T> expectedList = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            String randomString;
            String modifiedRandomString;
            do {
                randomString = RandomDocumentPicks.randomString(random());
                modifiedRandomString = modifyInput(randomString);
            } while (isSupportedValue(modifiedRandomString) == false);
            fieldValueList.add(modifiedRandomString);
            expectedList.add(expectedResult(randomString));
        }
        String multiValueFieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValueList);
        Processor multiValueProcessor = newProcessor(multiValueFieldName, randomBoolean(), multiValueFieldName);
        multiValueProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(multiValueFieldName, List.class), equalTo(expectedList));
    }

    public void testFieldNotFound() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() throws Exception {
        Processor processor = newProcessor("field", false, "field");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        Processor processor = newProcessor("field", true, "field");
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonStringValue() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));

        List<Integer> fieldValueList = new ArrayList<>();
        int randomValue = randomInt();
        fieldValueList.add(randomValue);
        ingestDocument.setFieldValue(fieldName, fieldValueList);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(
            exception.getMessage(),
            equalTo(
                "value ["
                    + randomValue
                    + "] of type [java.lang.Integer] in list field ["
                    + fieldName
                    + "] cannot be cast to [java.lang.String]"
            )
        );
    }

    public void testNonStringValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));

        List<Integer> fieldValueList = new ArrayList<>();
        int randomValue = randomInt();
        fieldValueList.add(randomValue);
        ingestDocument.setFieldValue(fieldName, fieldValueList);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(
            exception.getMessage(),
            equalTo(
                "value ["
                    + randomValue
                    + "] of type [java.lang.Integer] in list field ["
                    + fieldName
                    + "] cannot be cast to [java.lang.String]"
            )
        );
    }

    public void testTargetField() throws Exception {
        IngestDocument ingestDocument;
        String fieldValue;
        String fieldName;
        boolean ignoreMissing;
        do {
            ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
            fieldValue = RandomDocumentPicks.randomString(random());
            fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifyInput(fieldValue));
            ignoreMissing = randomBoolean();
        } while (isSupportedValue(ingestDocument.getFieldValue(fieldName, Object.class, ignoreMissing)) == false);
        String targetFieldName = fieldName + "foo";
        Processor processor = newProcessor(fieldName, ignoreMissing, targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, expectedResultType()), equalTo(expectedResult(fieldValue)));
    }

    protected boolean isSupportedValue(Object value) {
        return true;
    }
}
