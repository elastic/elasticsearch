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

package org.elasticsearch.ingest.processor.join;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;

import java.util.*;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JoinProcessorTests extends ESTestCase {

    private static final String[] SEPARATORS = new String[]{"-", "_", "."};

    public void testJoinStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, String> fields = new HashMap<>();
        Map<String, String> expectedResultMap = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int numItems = randomIntBetween(1, 10);
            String separator = randomFrom(SEPARATORS);
            List<String> fieldValue = new ArrayList<>(numItems);
            String expectedResult = "";
            for (int j = 0; j < numItems; j++) {
                String value = randomAsciiOfLengthBetween(1, 10);
                fieldValue.add(value);
                expectedResult += value;
                if (j < numItems - 1) {
                    expectedResult += separator;
                }
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            expectedResultMap.put(fieldName, expectedResult);
            fields.put(fieldName, separator);
        }
        Processor processor = new JoinProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, String> entry : expectedResultMap.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), String.class), equalTo(entry.getValue()));
        }
    }

    public void testJoinIntegers() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, String> fields = new HashMap<>();
        Map<String, String> expectedResultMap = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
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
            expectedResultMap.put(fieldName, expectedResult);
            fields.put(fieldName, separator);
        }
        Processor processor = new JoinProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, String> entry : expectedResultMap.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), String.class), equalTo(entry.getValue()));
        }
    }

    public void testJoinNonListField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomAsciiOfLengthBetween(1, 10));
        Map<String, String> join = Collections.singletonMap(fieldName, "-");
        Processor processor = new JoinProcessor(join);
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.String] cannot be cast to [java.util.List]"));
        }
    }

    public void testJoinNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new JoinProcessor(Collections.singletonMap(fieldName, "-"));
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testJoinNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new JoinProcessor(Collections.singletonMap("field", "-"));
        try {
            processor.execute(ingestDocument);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot join."));
        }
    }
}
