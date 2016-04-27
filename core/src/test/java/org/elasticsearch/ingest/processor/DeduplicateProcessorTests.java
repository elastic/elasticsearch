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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DeduplicateProcessorTests extends ESTestCase {

    public void testPriorSortAndSortedInput() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);

        List<Integer> fieldValue = new ArrayList<>(numItems * 3);
        for (int j = 0; j < numItems; j++) {
            int value = randomIntBetween(0,100);
            fieldValue.add(value);
            fieldValue.add(value);
            fieldValue.add(value);
        }
        Collections.sort(fieldValue);
        Set<Integer> expectedResultSet = new LinkedHashSet<>(fieldValue);
        List<Integer> expectedResult = new ArrayList<>(expectedResultSet);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);

        // Randomize `ordered` because it shouldn't matter...always ordered due to sort
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, randomBoolean());
        processor.setLastType("sort");

        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedResult));
    }

    public void testNoPriorSortAndUnsortedOrdered() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);

        List<Integer> fieldValue = new ArrayList<>(numItems * 3);
        for (int j = 0; j < numItems; j++) {
            int value = randomIntBetween(0,100);
            fieldValue.add(value);
            fieldValue.add(value);
            fieldValue.add(value);
        }
        Set<Integer> expectedResultSet = new LinkedHashSet<>(fieldValue);
        List<Integer> expectedResult = new ArrayList<>(expectedResultSet);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, true);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testNoPriorSortAndUnsortedUnordered() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);

        List<Integer> fieldValue = new ArrayList<>(numItems * 3);
        for (int j = 0; j < numItems; j++) {
            int value = randomIntBetween(0,100);
            fieldValue.add(value);
            fieldValue.add(value);
            fieldValue.add(value);
        }
        Set<Integer> expectedResultSet = new LinkedHashSet<>(fieldValue);  // keep using Linked here so we can tests non-ordering
        List<Integer> expectedResult = new ArrayList<>(expectedResultSet);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, false);
        processor.execute(ingestDocument);

        List dedupedList = ingestDocument.getFieldValue(fieldName, List.class);
        int counter = 0;
        for (Object value : dedupedList) {
            assertEquals(expectedResult.contains(value), true);
            counter++;
        }
        assertEquals(dedupedList.size(), counter);  // Make sure there are no extra elements
    }

    public void testListsOfNull() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);

        List<Integer> fieldValue = new ArrayList<>(numItems * 3);
        List<Integer> expectedResult = new ArrayList<>(numItems);
        for (int j = 0; j < numItems; j++) {
            fieldValue.add(null);
            fieldValue.add(null);
            fieldValue.add(null);

        }
        expectedResult.add(null);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, true);
        processor.execute(ingestDocument);
        assertEquals(ingestDocument.getFieldValue(fieldName, List.class), expectedResult);
    }

    public void testDedupeNonListField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomAsciiOfLengthBetween(1, 10));
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, true);
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.String] cannot be cast to [java.util.List]"));
        }
    }

    public void testDedupeNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), fieldName, true);
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testDedupeNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new DeduplicateProcessor(randomAsciiOfLength(10), "field", true);
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot deduplicate."));
        }
    }

}
