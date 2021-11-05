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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JoinProcessorTests extends ESTestCase {

    private static final String[] SEPARATORS = new String[] { "-", "_", "." };

    public void testJoinStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<String> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedResult));
    }

    public void testJoinIntegers() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<Integer> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            int value = randomInt();
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedResult));
    }

    public void testJoinNonListField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomAlphaOfLengthBetween(1, 10));
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, "-", fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.String] cannot be cast to [java.util.List]"));
        }
    }

    public void testJoinNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, "-", fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testJoinNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, "field", "-", "field");
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot join."));
        }
    }

    public void testJoinWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<String> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        String targetFieldName = fieldName + randomAlphaOfLength(5);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(expectedResult));
    }
}
