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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class Base64DecodeProcessorTests extends ESTestCase {
    public void testValidPaddedString() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String expectedString = randomAlphaOfLength(10);
        String actualString = Base64.getEncoder().encodeToString(expectedString.getBytes(StandardCharsets.UTF_8));
        String targetFieldName = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, actualString);

        Processor processor = new Base64DecodeProcessor(randomAlphaOfLength(10), null, fieldName, targetFieldName, false, false);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(expectedString));
    }

    public void testInvalidValueError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Object dirtyValue = 1123;
        String targetFieldName = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, dirtyValue);
        Processor processor = new Base64DecodeProcessor(randomAlphaOfLength(10), null, fieldName, targetFieldName,false, false);

        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("Field [" + fieldName + "] cannot be processed due to invalid value type: " + dirtyValue.getClass().getName()));
        }
    }

    public void testValidPaddedStringArray() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Integer randomLength = randomInt(10);
        List<String> actualList = new ArrayList<>(randomLength);
        List<String> expectedList = new ArrayList<>(randomLength);

        for(int i = 0; i < randomLength; i++) {
            String str = randomAlphaOfLength(10);
            expectedList.add(str);
            actualList.add(Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8)));
        }

        String targetFieldName = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, actualList);
        Processor processor = new Base64DecodeProcessor(randomAlphaOfLength(10), null, fieldName, targetFieldName, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, List.class), equalTo(expectedList));
    }
}
