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
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.common.ConvertProcessor.Type;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ConvertProcessorTests extends ESTestCase {

    public void testConvertInt() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int randomInt = randomInt();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomInt);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Integer.class), equalTo(randomInt));
    }

    public void testConvertIntHex() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int randomInt = randomInt();
        String intString = randomInt < 0 ? "-0x" + Integer.toHexString(-randomInt) : "0x" + Integer.toHexString(randomInt);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, intString);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Integer.class), equalTo(randomInt));
    }

    public void testConvertIntLeadingZero() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "010");
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Integer.class), equalTo(10));
    }

    public void testConvertIntHexError() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String value = "0xnotanumber";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, value);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to integer"));
    }

    public void testConvertIntList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>();
        List<Integer> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            int randomInt = randomInt();
            fieldValue.add(Integer.toString(randomInt));
            expectedList.add(randomInt);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertIntError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAlphaOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.INTEGER, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to integer"));
        }
    }

    public void testConvertLong() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Long> expectedResult = new HashMap<>();
        long randomLong = randomLong();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomLong);
        expectedResult.put(fieldName, randomLong);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Long.class), equalTo(randomLong));
    }

    public void testConvertLongHex() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        long randomLong = randomLong();
        String longString = randomLong < 0 ? "-0x" + Long.toHexString(-randomLong) : "0x" + Long.toHexString(randomLong);
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, longString);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Long.class), equalTo(randomLong));
    }

    public void testConvertLongLeadingZero() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "010");
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Long.class), equalTo(10L));
    }

    public void testConvertLongHexError() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String value = "0xnotanumber";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, value);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to long"));
    }

    public void testConvertLongList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>();
        List<Long> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            long randomLong = randomLong();
            fieldValue.add(Long.toString(randomLong));
            expectedList.add(randomLong);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertLongError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAlphaOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.LONG, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to long"));
        }
    }

    public void testConvertDouble() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Double> expectedResult = new HashMap<>();
        double randomDouble = randomDouble();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomDouble);
        expectedResult.put(fieldName, randomDouble);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.DOUBLE, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Double.class), equalTo(randomDouble));
    }

    public void testConvertDoubleList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>();
        List<Double> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            double randomDouble = randomDouble();
            fieldValue.add(Double.toString(randomDouble));
            expectedList.add(randomDouble);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.DOUBLE, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertDoubleError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAlphaOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.DOUBLE, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to double"));
        }
    }

    public void testConvertFloat() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Map<String, Float> expectedResult = new HashMap<>();
        float randomFloat = randomFloat();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomFloat);
        expectedResult.put(fieldName, randomFloat);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.FLOAT, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Float.class), equalTo(randomFloat));
    }

    public void testConvertFloatList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>();
        List<Float> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            float randomFloat = randomFloat();
            fieldValue.add(Float.toString(randomFloat));
            expectedList.add(randomFloat);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.FLOAT, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertFloatError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String value = "string-" + randomAlphaOfLengthBetween(1, 10);
        ingestDocument.setFieldValue(fieldName, value);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.FLOAT, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to convert [" + value + "] to float"));
        }
    }

    public void testConvertBoolean() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        boolean randomBoolean = randomBoolean();
        String booleanString = Boolean.toString(randomBoolean);
        if (randomBoolean) {
            booleanString = booleanString.toUpperCase(Locale.ROOT);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, booleanString);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.BOOLEAN, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, Boolean.class), equalTo(randomBoolean));
    }

    public void testConvertBooleanList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
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
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.BOOLEAN, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertBooleanError() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String fieldValue;
        if (randomBoolean()) {
            fieldValue = "string-" + randomAlphaOfLengthBetween(1, 10);
        } else {
            // verify that only proper boolean values are supported and we are strict about it
            fieldValue = randomFrom("on", "off", "yes", "no", "0", "1");
        }
        ingestDocument.setFieldValue(fieldName, fieldValue);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.BOOLEAN, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("[" + fieldValue + "] is not a boolean value, cannot convert to boolean"));
        }
    }

    public void testConvertIpV4() throws Exception {
        // valid ipv4 address
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetField = randomValueOtherThan(fieldName, () -> RandomDocumentPicks.randomFieldName(random()));
        String validIpV4 = "192.168.1.1";
        ingestDocument.setFieldValue(fieldName, validIpV4);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetField, String.class), equalTo(validIpV4));

        // invalid ipv4 address
        IngestDocument ingestDocument2 = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        fieldName = RandomDocumentPicks.randomFieldName(random());
        targetField = randomValueOtherThan(fieldName, () -> RandomDocumentPicks.randomFieldName(random()));
        String invalidIpV4 = "192.168.1.256";
        ingestDocument2.setFieldValue(fieldName, invalidIpV4);

        Processor processor2 = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor2.execute(ingestDocument2));
        assertThat(e.getMessage(), containsString("'" + invalidIpV4 + "' is not an IP string literal."));
    }

    public void testConvertIpV6() throws Exception {
        // valid ipv6 address
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetField = randomValueOtherThan(fieldName, () -> RandomDocumentPicks.randomFieldName(random()));
        String validIpV6 = "2001:db8:3333:4444:5555:6666:7777:8888";
        ingestDocument.setFieldValue(fieldName, validIpV6);

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetField, String.class), equalTo(validIpV6));

        // invalid ipv6 address
        IngestDocument ingestDocument2 = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        fieldName = RandomDocumentPicks.randomFieldName(random());
        targetField = randomValueOtherThan(fieldName, () -> RandomDocumentPicks.randomFieldName(random()));
        String invalidIpV6 = "2001:db8:3333:4444:5555:6666:7777:88888";
        ingestDocument2.setFieldValue(fieldName, invalidIpV6);

        Processor processor2 = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor2.execute(ingestDocument2));
        assertThat(e.getMessage(), containsString("'" + invalidIpV6 + "' is not an IP string literal."));
    }

    public void testConvertIpList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<String> fieldValue = new ArrayList<>();
        List<String> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            String value;
            if (randomBoolean()) {
                // ipv4 value
                value = "192.168.1." + randomIntBetween(0, 255);
            } else {
                // ipv6 value
                value = "2001:db8:3333:4444:5555:6666:7777:" + Long.toString(randomLongBetween(0, 65535), 16);
            }
            fieldValue.add(value);
            expectedList.add(value);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.IP, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertString() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Object fieldValue;
        String expectedFieldValue;
        switch (randomIntBetween(0, 2)) {
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

        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.STRING, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedFieldValue));
    }

    public void testConvertStringList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        List<Object> fieldValue = new ArrayList<>();
        List<String> expectedList = new ArrayList<>();
        for (int j = 0; j < numItems; j++) {
            Object randomValue;
            String randomValueString;
            switch (randomIntBetween(0, 4)) {
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
                case 3:
                    long randomLong = randomLong();
                    randomValue = randomLong;
                    randomValueString = Long.toString(randomLong);
                    break;
                case 4:
                    double randomDouble = randomDouble();
                    randomValue = randomDouble;
                    randomValueString = Double.toString(randomDouble);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            fieldValue.add(randomValue);
            expectedList.add(randomValueString);
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, Type.STRING, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expectedList));
    }

    public void testConvertNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Type type = randomFrom(Type.values());
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, type, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testConvertNullField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Type type = randomFrom(Type.values());
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", type, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field [field] is null, cannot be converted to type [" + type + "]"));
        }
    }

    public void testConvertNonExistingFieldWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Type type = randomFrom(Type.values());
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, fieldName, type, true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testConvertNullFieldWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Type type = randomFrom(Type.values());
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", type, true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testAutoConvertNotString() throws Exception {
        Object randomValue;
        switch (randomIntBetween(0, 2)) {
            case 0:
                float randomFloat = randomFloat();
                randomValue = randomFloat;
                break;
            case 1:
                int randomInt = randomInt();
                randomValue = randomInt;
                break;
            case 2:
                boolean randomBoolean = randomBoolean();
                randomValue = randomBoolean;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", randomValue));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, sameInstance(randomValue));
    }

    public void testAutoConvertStringNotMatched() throws Exception {
        String value = "notAnIntFloatOrBool";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", value));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, sameInstance(value));
    }

    public void testAutoConvertMatchBoolean() throws Exception {
        boolean randomBoolean = randomBoolean();
        String booleanString = Boolean.toString(randomBoolean);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("field", booleanString)
        );
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomBoolean));
    }

    public void testAutoConvertMatchInteger() throws Exception {
        int randomInt = randomInt();
        String randomString = Integer.toString(randomInt);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", randomString));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomInt));
    }

    public void testAutoConvertMatchLong() throws Exception {
        long randomLong = randomLong();
        String randomString = Long.toString(randomLong);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", randomString));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomLong));
    }

    public void testAutoConvertDoubleNotMatched() throws Exception {
        double randomDouble = randomDouble();
        String randomString = Double.toString(randomDouble);
        float randomFloat = Float.parseFloat(randomString);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", randomString));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, not(randomDouble));
        assertThat(convertedValue, equalTo(randomFloat));
    }

    public void testAutoConvertMatchFloat() throws Exception {
        float randomFloat = randomFloat();
        String randomString = Float.toString(randomFloat);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", randomString));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomFloat));
    }

    public void testTargetField() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        int randomInt = randomInt();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, String.valueOf(randomInt));
        String targetField = fieldName + randomAlphaOfLength(5);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(String.valueOf(randomInt)));
        assertThat(ingestDocument.getFieldValue(targetField, Integer.class), equalTo(randomInt));
    }
}
