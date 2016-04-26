/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.processor.SortProcessor.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class SortProcessorTests extends ESTestCase {

    public void testSortStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>(numItems);
        List<String> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            String value = randomAsciiOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult.add(value);
        }
        Collections.sort(expectedResult);

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
    }

    public void testSortIntegersNonRandom() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());

        Integer[] expectedResult = new Integer[]{1,2,3,4,5,10,20,21,22,50,100};
        List<Integer> fieldValue = new ArrayList<>(expectedResult.length);
        fieldValue.addAll(Arrays.asList(expectedResult).subList(0, expectedResult.length));
        Collections.shuffle(fieldValue);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, SortOrder.ASCENDING);
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
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
                value = randomAsciiOfLengthBetween(1, 10);
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
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
    }

    public void testSortMixedObjects() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Object> fieldValue = new ArrayList<>(numItems);
        List<Object> expectedResult = new ArrayList<>(numItems);
        Object value;
        for (int j = 0; j < numItems; j++) {
            if (randomBoolean()) {
                value = String.valueOf(randomIntBetween(0, 100));
            } else {
                value = randomAsciiOfLengthBetween(1, 10);
            }
            fieldValue.add(value);
            expectedResult.add(value);
        }
        expectedResult = expectedResult.stream()
            .sorted()
            .collect(Collectors.toList());

        SortOrder order = randomBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING;
        if (order.equals(SortOrder.DESCENDING)) {
            Collections.reverse(expectedResult);
        }

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SortProcessor(randomAsciiOfLength(10), fieldName, order);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
    }



}
