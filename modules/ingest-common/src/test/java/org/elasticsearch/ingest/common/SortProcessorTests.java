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
import org.elasticsearch.ingest.common.SortProcessor.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SortProcessorTests extends ESTestCase {

    public void testSortStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>(numItems);
        List<String> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortIntegersNonRandom() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());

        Integer[] expectedResult = new Integer[] { 1, 2, 3, 4, 5, 10, 20, 21, 22, 50, 100 };
        List<Integer> fieldValue = new ArrayList<>(expectedResult.length);
        fieldValue.addAll(List.of(expectedResult).subList(0, expectedResult.length));
        Collections.shuffle(fieldValue, random());

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, SortOrder.ASCENDING, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class).toArray(), equalTo(expectedResult));
    }

    public void testSortIntegers() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Integer> fieldValue = new ArrayList<>(numItems);
        List<Integer> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Integer value = randomIntBetween(1, 100);
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortShorts() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Short> fieldValue = new ArrayList<>(numItems);
        List<Short> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Short value = randomShort();
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortDoubles() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Double> fieldValue = new ArrayList<>(numItems);
        List<Double> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Double value = randomDoubleBetween(0.0, 100.0, true);
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortFloats() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Float> fieldValue = new ArrayList<>(numItems);
        List<Float> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Float value = randomFloat();
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortBytes() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Byte> fieldValue = new ArrayList<>(numItems);
        List<Byte> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Byte value = randomByte();
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortBooleans() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Boolean> fieldValue = new ArrayList<>(numItems);
        List<Boolean> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            Boolean value = randomBoolean();
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortMixedStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>(numItems);
        List<String> expectedResult = new ArrayList<>(numItems);
        String value;
        for (int j = 0; j < numItems; j++) {
            if (randomBoolean()) {
                value = String.valueOf(randomIntBetween(0, 100));
            } else {
                value = randomAlphaOfLengthBetween(1, 10);
            }
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testSortNonListField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomAlphaOfLengthBetween(1, 10));
        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.String] cannot be cast to [java.util.List]"));
        }
    }

    public void testSortNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testSortNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, "field", order, "field");
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot sort."));
        }
    }

    public void testDescendingSortWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>(numItems);
        List<String> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult.add(value);
        }

        Collections.sort(expectedResult, Collections.reverseOrder());

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        String targetFieldName = fieldName + "foo";
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, SortOrder.DESCENDING, targetFieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(targetFieldName, List.class), expectedResult);
    }

    public void testAscendingSortWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>(numItems);
        List<String> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult.add(value);
        }

        Collections.sort(expectedResult);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        String targetFieldName = fieldName + "foo";
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, SortOrder.ASCENDING, targetFieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(targetFieldName, List.class), expectedResult);
    }

    public void testSortWithTargetFieldLeavesOriginalUntouched() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        List<Integer> fieldValue = List.of(1, 5, 4);
        List<Integer> expectedResult = new ArrayList<>(fieldValue);
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, new ArrayList<>(fieldValue));
        String targetFieldName = fieldName + "foo";
        Processor processor = new SortProcessor(randomAlphaOfLength(10), null, fieldName, order, targetFieldName);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(targetFieldName, List.class), expectedResult);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), fieldValue);
    }
}
