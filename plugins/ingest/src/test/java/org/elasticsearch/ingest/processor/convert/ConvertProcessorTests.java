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

package org.elasticsearch.ingest.processor.convert;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.ingest.processor.convert.ConvertProcessor.*;
import static org.hamcrest.Matchers.equalTo;

public class ConvertProcessorTests extends ESTestCase {

    public void testConvertInt() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, ConvertProcessor.Type> fields = new HashMap<>();
        Map<String, Integer> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int randomInt = randomInt();
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomInt);
            fields.put(fieldName, Type.INTEGER);
            expectedResult.put(fieldName, randomInt);
        }
        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, Integer> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), Integer.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertIntList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, ConvertProcessor.Type> fields = new HashMap<>();
        Map<String, List<Integer>> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int numItems = randomIntBetween(1, 10);
            List<String> fieldValue = new ArrayList<>();
            List<Integer> expectedList = new ArrayList<>();
            for (int j = 0; j < numItems; j++) {
                int randomInt = randomInt();
                fieldValue.add(Integer.toString(randomInt));
                expectedList.add(randomInt);
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            fields.put(fieldName, Type.INTEGER);
            expectedResult.put(fieldName, expectedList);
        }
        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, List<Integer>> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), List.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertIntError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAsciiOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Map<String, Type> convert = Collections.singletonMap(fieldName, Type.INTEGER);
        Processor processor = new ConvertProcessor(convert);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to integer"));
        }
    }

    public void testConvertFloat() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, Float> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            float randomFloat = randomFloat();
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomFloat);
            fields.put(fieldName, Type.FLOAT);
            expectedResult.put(fieldName, randomFloat);
        }

        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, Float> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), Float.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertFloatList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, List<Float>> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int numItems = randomIntBetween(1, 10);
            List<String> fieldValue = new ArrayList<>();
            List<Float> expectedList = new ArrayList<>();
            for (int j = 0; j < numItems; j++) {
                float randomFloat = randomFloat();
                fieldValue.add(Float.toString(randomFloat));
                expectedList.add(randomFloat);
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            fields.put(fieldName, Type.FLOAT);
            expectedResult.put(fieldName, expectedList);
        }
        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, List<Float>> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), List.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertFloatError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAsciiOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Map<String, Type> convert = Collections.singletonMap(fieldName, Type.FLOAT);
        Processor processor = new ConvertProcessor(convert);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to float"));
        }
    }

    public void testConvertBoolean() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, Boolean> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            boolean randomBoolean = randomBoolean();
            String booleanString = Boolean.toString(randomBoolean);
            if (randomBoolean) {
                booleanString = booleanString.toUpperCase(Locale.ROOT);
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, booleanString);
            fields.put(fieldName, Type.BOOLEAN);
            expectedResult.put(fieldName, randomBoolean);
        }

        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, Boolean> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), Boolean.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertBooleanList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, List<Boolean>> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int numItems = randomIntBetween(1, 10);
            List<String> fieldValue = new ArrayList<>();
            List<Boolean> expectedList = new ArrayList<>();
            for (int j = 0; j < numItems; j++) {
                boolean randomBoolean = randomBoolean();
                String booleanString = Boolean.toString(randomBoolean);
                if (randomBoolean) {
                    booleanString = booleanString.toUpperCase(Locale.ROOT);
                }
                fieldValue.add(booleanString);
                expectedList.add(randomBoolean);
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            fields.put(fieldName, Type.BOOLEAN);
            expectedResult.put(fieldName, expectedList);
        }
        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, List<Boolean>> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), List.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertBooleanError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String fieldValue;
        if (randomBoolean()) {
            fieldValue = "string-" + randomAsciiOfLengthBetween(1, 10);
        } else {
            //verify that only proper boolean values are supported and we are strict about it
            fieldValue = randomFrom("on", "off", "yes", "no", "0", "1");
        }
        ingestDocument.setFieldValue(fieldName, fieldValue);

        Map<String, Type> convert = Collections.singletonMap(fieldName, Type.BOOLEAN);
        Processor processor = new ConvertProcessor(convert);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(Exception e) {
            assertThat(e.getMessage(), equalTo("[" + fieldValue + "] is not a boolean value, cannot convert to boolean"));
        }
    }

    public void testConvertString() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, String> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            Object fieldValue;
            String expectedFieldValue;
            switch(randomIntBetween(0, 2)) {
                case 0:
                    float randomFloat = randomFloat();
                    fieldValue = randomFloat;
                    expectedFieldValue = Float.toString(randomFloat);
                    break;
                case 1:
                    int randomInt = randomInt();
                    fieldValue = randomInt;
                    expectedFieldValue = Integer.toString(randomInt);
                    break;
                case 2:
                    boolean randomBoolean = randomBoolean();
                    fieldValue = randomBoolean;
                    expectedFieldValue = Boolean.toString(randomBoolean);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            fields.put(fieldName, Type.STRING);
            expectedResult.put(fieldName, expectedFieldValue);
        }

        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, String> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), String.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertStringList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Type> fields = new HashMap<>();
        Map<String, List<String>> expectedResult = new HashMap<>();
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            int numItems = randomIntBetween(1, 10);
            List<Object> fieldValue = new ArrayList<>();
            List<String> expectedList = new ArrayList<>();
            for (int j = 0; j < numItems; j++) {
                Object randomValue;
                String randomValueString;
                switch(randomIntBetween(0, 2)) {
                    case 0:
                        float randomFloat = randomFloat();
                        randomValue = randomFloat;
                        randomValueString = Float.toString(randomFloat);
                        break;
                    case 1:
                        int randomInt = randomInt();
                        randomValue = randomInt;
                        randomValueString = Integer.toString(randomInt);
                        break;
                    case 2:
                        boolean randomBoolean = randomBoolean();
                        randomValue = randomBoolean;
                        randomValueString = Boolean.toString(randomBoolean);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                fieldValue.add(randomValue);
                expectedList.add(randomValueString);
            }
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
            fields.put(fieldName, Type.STRING);
            expectedResult.put(fieldName, expectedList);
        }
        Processor processor = new ConvertProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, List<String>> entry : expectedResult.entrySet()) {
            assertThat(ingestDocument.getFieldValue(entry.getKey(), List.class), equalTo(entry.getValue()));
        }
    }

    public void testConvertNullField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Type type = randomFrom(Type.values());
        Map<String, Type> convert = Collections.singletonMap(fieldName, type);
        Processor processor = new ConvertProcessor(convert);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field [" + fieldName + "] is null, cannot be converted to type [" + type + "]"));
        }
    }
}
