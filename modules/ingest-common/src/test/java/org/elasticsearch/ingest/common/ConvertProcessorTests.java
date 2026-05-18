/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.common.ConvertProcessor.Type;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
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
        long randomLong = randomLong();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomLong);

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
        double randomDouble = randomDouble();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomDouble);

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
        float randomFloat = randomFloat();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomFloat);

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
        {
            // valid ipv4 address
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
            String fieldName = RandomDocumentPicks.randomFieldName(random());
            // We can't have targetField be a nested field under fieldName since we're going to set a top-level value for fieldName:
            String targetField = randomValueOtherThanMany(
                targetFieldName -> fieldName.equals(targetFieldName) || targetFieldName.startsWith(fieldName + "."),
                () -> RandomDocumentPicks.randomFieldName(random())
            );
            String validIpV4 = "192.168.1.1";
            ingestDocument.setFieldValue(fieldName, validIpV4);

            Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getFieldValue(targetField, String.class), equalTo(validIpV4));
        }

        {
            // invalid ipv4 address
            IngestDocument ingestDocument2 = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
            String fieldName = RandomDocumentPicks.randomFieldName(random());
            // We can't have targetField be a nested field under fieldName since we're going to set a top-level value for fieldName:
            String targetField = randomValueOtherThanMany(
                targetFieldName -> fieldName.equals(targetFieldName) || targetFieldName.startsWith(fieldName + "."),
                () -> RandomDocumentPicks.randomFieldName(random())
            );
            String invalidIpV4 = "192.168.1.256";
            ingestDocument2.setFieldValue(fieldName, invalidIpV4);

            Processor processor2 = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.IP, false);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor2.execute(ingestDocument2));
            assertThat(e.getMessage(), containsString("'" + invalidIpV4 + "' is not an IP string literal."));
        }
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
            case 0 -> {
                float randomFloat = randomFloat();
                fieldValue = randomFloat;
                expectedFieldValue = Float.toString(randomFloat);
            }
            case 1 -> {
                int randomInt = randomInt();
                fieldValue = randomInt;
                expectedFieldValue = Integer.toString(randomInt);
            }
            case 2 -> {
                boolean randomBoolean = randomBoolean();
                fieldValue = randomBoolean;
                expectedFieldValue = Boolean.toString(randomBoolean);
            }
            default -> throw new UnsupportedOperationException();
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
                case 0 -> {
                    float randomFloat = randomFloat();
                    randomValue = randomFloat;
                    randomValueString = Float.toString(randomFloat);
                }
                case 1 -> {
                    int randomInt = randomInt();
                    randomValue = randomInt;
                    randomValueString = Integer.toString(randomInt);
                }
                case 2 -> {
                    boolean randomBoolean = randomBoolean();
                    randomValue = randomBoolean;
                    randomValueString = Boolean.toString(randomBoolean);
                }
                case 3 -> {
                    long randomLong = randomLong();
                    randomValue = randomLong;
                    randomValueString = Long.toString(randomLong);
                }
                case 4 -> {
                    double randomDouble = randomDouble();
                    randomValue = randomDouble;
                    randomValueString = Double.toString(randomDouble);
                }
                default -> throw new UnsupportedOperationException();
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
            case 0 -> {
                randomValue = randomFloat();
            }
            case 1 -> {
                randomValue = randomInt();
            }
            case 2 -> {
                randomValue = randomBoolean();
            }
            default -> throw new UnsupportedOperationException();
        }
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", randomValue)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, sameInstance(randomValue));
    }

    public void testAutoConvertStringNotMatched() throws Exception {
        String value = "notAnIntFloatOrBool";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", value)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, sameInstance(value));
    }

    public void testAutoConvertMatchBoolean() throws Exception {
        boolean randomBoolean = randomBoolean();
        String booleanString = Boolean.toString(randomBoolean);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", booleanString)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomBoolean));
    }

    public void testAutoConvertMatchInteger() throws Exception {
        int randomInt = randomInt();
        String randomString = Integer.toString(randomInt);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", randomString)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomInt));
    }

    public void testAutoConvertMatchLong() throws Exception {
        long randomLong = randomLong();
        String randomString = Long.toString(randomLong);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", randomString)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomLong));
    }

    public void testAutoConvertDoubleNotMatched() throws Exception {
        double randomDouble = randomDouble();
        String randomString = Double.toString(randomDouble);
        float randomFloat = Float.parseFloat(randomString);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", randomString)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, not(randomDouble));
        assertThat(convertedValue, equalTo(randomFloat));
    }

    public void testAutoConvertMatchFloat() throws Exception {
        float randomFloat = randomFloat();
        String randomString = Float.toString(randomFloat);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", randomString)));
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", Type.AUTO, false);
        processor.execute(ingestDocument);
        Object convertedValue = ingestDocument.getFieldValue("field", Object.class);
        assertThat(convertedValue, equalTo(randomFloat));
    }

    public void testTargetField() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        int randomInt = randomInt();
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, String.valueOf(randomInt));
        String targetField = fieldName + randomAlphaOfLength(5);
        Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, fieldName, targetField, Type.INTEGER, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(String.valueOf(randomInt)));
        assertThat(ingestDocument.getFieldValue(targetField, Integer.class), equalTo(randomInt));
    }

    /**
     * This class encapsulates a matrix of tests for converting between different numeric types, including string representations of
     * numbers, and including conversion to {@link Type#AUTO}.
     *
     * <p>The {@link #streamTests()} returns a stream of test cases. The {@link TestCase#run()} method of each case runs that test.
     *
     * <p>N.B. The purpose of this test matrix is at least partly to describe the current behaviour, to prevent changes being made
     * accidentally. The presence of a test case in this list is not intended as a statement that this is actually the intended behaviour.
     */
    private static class NumericConversionTestMatrix {

        private static final String TEST_CASES_CSV = """
            Integer,123,STRING,String,123
            Long,123,STRING,String,123
            Float,123.0,STRING,String,123.0
            Double,123.0,STRING,String,123.0
            String,123,STRING,String,123
            String,0x7b,STRING,String,0x7b
            String,123.0,STRING,String,123.0
            String,1.230000e+02,STRING,String,1.230000e+02
            Float,123.45,STRING,String,123.45
            Double,123.45,STRING,String,123.45
            String,123.45,STRING,String,123.45
            String,1.234500e+02,STRING,String,1.234500e+02
            String,0x1.edcdp6,STRING,String,0x1.edcdp6
            Integer,16777217,STRING,String,16777217
            Long,16777217,STRING,String,16777217
            Double,1.6777217E7,STRING,String,1.6777217E7
            String,16777217,STRING,String,16777217
            String,16777217.0,STRING,String,16777217.0
            Long,2147483648,STRING,String,2147483648
            Double,2.147483648E9,STRING,String,2.147483648E9
            String,2147483648,STRING,String,2147483648
            String,2147483648.0,STRING,String,2147483648.0
            Long,9007199254740993,STRING,String,9007199254740993
            String,9007199254740993,STRING,String,9007199254740993
            String,9007199254740993.0,STRING,String,9007199254740993.0
            String,9223372036854775808,STRING,String,9223372036854775808
            String,9223372036854775808.0,STRING,String,9223372036854775808.0
            String,680564693277057720000000000000000000000,STRING,String,680564693277057720000000000000000000000
            String,680564693277057720000000000000000000000.0,STRING,String,680564693277057720000000000000000000000.0
            Integer,123,INTEGER,Integer,123
            Long,123,INTEGER,Integer,123
            Float,123.0,INTEGER,THROWS,
            Double,123.0,INTEGER,THROWS,
            String,123,INTEGER,Integer,123
            String,0x7b,INTEGER,Integer,123
            String,123.0,INTEGER,THROWS,
            String,1.230000e+02,INTEGER,THROWS,
            Float,123.45,INTEGER,THROWS,
            Double,123.45,INTEGER,THROWS,
            String,123.45,INTEGER,THROWS,
            String,1.234500e+02,INTEGER,THROWS,
            String,0x1.edcdp6,INTEGER,THROWS,
            Integer,16777217,INTEGER,Integer,16777217
            Long,16777217,INTEGER,Integer,16777217
            Double,1.6777217E7,INTEGER,THROWS,
            String,16777217,INTEGER,Integer,16777217
            String,16777217.0,INTEGER,THROWS,
            Long,2147483648,INTEGER,THROWS,
            Double,2.147483648E9,INTEGER,THROWS,
            String,2147483648,INTEGER,THROWS,
            String,2147483648.0,INTEGER,THROWS,
            Long,9007199254740993,INTEGER,THROWS,
            String,9007199254740993,INTEGER,THROWS,
            String,9007199254740993.0,INTEGER,THROWS,
            String,9223372036854775808,INTEGER,THROWS,
            String,9223372036854775808.0,INTEGER,THROWS,
            String,680564693277057720000000000000000000000,INTEGER,THROWS,
            String,680564693277057720000000000000000000000.0,INTEGER,THROWS,
            Integer,123,LONG,Long,123
            Long,123,LONG,Long,123
            Float,123.0,LONG,THROWS,
            Double,123.0,LONG,THROWS,
            String,123,LONG,Long,123
            String,0x7b,LONG,Long,123
            String,123.0,LONG,THROWS,
            String,1.230000e+02,LONG,THROWS,
            Float,123.45,LONG,THROWS,
            Double,123.45,LONG,THROWS,
            String,123.45,LONG,THROWS,
            String,1.234500e+02,LONG,THROWS,
            String,0x1.edcdp6,LONG,THROWS,
            Integer,16777217,LONG,Long,16777217
            Long,16777217,LONG,Long,16777217
            Double,1.6777217E7,LONG,THROWS,
            String,16777217,LONG,Long,16777217
            String,16777217.0,LONG,THROWS,
            Long,2147483648,LONG,Long,2147483648
            Double,2.147483648E9,LONG,THROWS,
            String,2147483648,LONG,Long,2147483648
            String,2147483648.0,LONG,THROWS,
            Long,9007199254740993,LONG,Long,9007199254740993
            String,9007199254740993,LONG,Long,9007199254740993
            String,9007199254740993.0,LONG,THROWS,
            String,9223372036854775808,LONG,THROWS,
            String,9223372036854775808.0,LONG,THROWS,
            String,680564693277057720000000000000000000000,LONG,THROWS,
            String,680564693277057720000000000000000000000.0,LONG,THROWS,
            Integer,123,DOUBLE,Double,123.0
            Long,123,DOUBLE,Double,123.0
            Float,123.0,DOUBLE,Double,123.0
            Double,123.0,DOUBLE,Double,123.0
            String,123,DOUBLE,Double,123.0
            String,0x7b,DOUBLE,THROWS,
            String,123.0,DOUBLE,Double,123.0
            String,1.230000e+02,DOUBLE,Double,123.0
            Float,123.45,DOUBLE,Double,123.45
            Double,123.45,DOUBLE,Double,123.45
            String,123.45,DOUBLE,Double,123.45
            String,1.234500e+02,DOUBLE,Double,123.45
            String,0x1.edcdp6,DOUBLE,Double,123.4501953125
            Integer,16777217,DOUBLE,Double,1.6777217E7
            Long,16777217,DOUBLE,Double,1.6777217E7
            Double,1.6777217E7,DOUBLE,Double,1.6777217E7
            String,16777217,DOUBLE,Double,1.6777217E7
            String,16777217.0,DOUBLE,Double,1.6777217E7
            Long,2147483648,DOUBLE,Double,2.147483648E9
            Double,2.147483648E9,DOUBLE,Double,2.147483648E9
            String,2147483648,DOUBLE,Double,2.147483648E9
            String,2147483648.0,DOUBLE,Double,2.147483648E9
            Long,9007199254740993,DOUBLE,Double,9.007199254740992E15
            String,9007199254740993,DOUBLE,Double,9.007199254740992E15
            String,9007199254740993.0,DOUBLE,Double,9.007199254740992E15
            String,9223372036854775808,DOUBLE,Double,9.223372036854776E18
            String,9223372036854775808.0,DOUBLE,Double,9.223372036854776E18
            String,680564693277057720000000000000000000000,DOUBLE,Double,6.805646932770577E38
            String,680564693277057720000000000000000000000.0,DOUBLE,Double,6.805646932770577E38
            Integer,123,FLOAT,Float,123.0
            Long,123,FLOAT,Float,123.0
            Float,123.0,FLOAT,Float,123.0
            Double,123.0,FLOAT,Float,123.0
            String,123,FLOAT,Float,123.0
            String,0x7b,FLOAT,THROWS,
            String,123.0,FLOAT,Float,123.0
            String,1.230000e+02,FLOAT,Float,123.0
            Float,123.45,FLOAT,Float,123.45
            Double,123.45,FLOAT,Float,123.45
            String,123.45,FLOAT,Float,123.45
            String,1.234500e+02,FLOAT,Float,123.45
            String,0x1.edcdp6,FLOAT,Float,123.450195
            Integer,16777217,FLOAT,Float,1.6777216E7
            Long,16777217,FLOAT,Float,1.6777216E7
            Double,1.6777217E7,FLOAT,Float,1.6777216E7
            String,16777217,FLOAT,Float,1.6777216E7
            String,16777217.0,FLOAT,Float,1.6777216E7
            Long,2147483648,FLOAT,Float,2.1474836E9
            Double,2.147483648E9,FLOAT,Float,2.1474836E9
            String,2147483648,FLOAT,Float,2.1474836E9
            String,2147483648.0,FLOAT,Float,2.1474836E9
            Long,9007199254740993,FLOAT,Float,9.007199E15
            String,9007199254740993,FLOAT,Float,9.007199E15
            String,9007199254740993.0,FLOAT,Float,9.007199E15
            String,9223372036854775808,FLOAT,Float,9.223372E18
            String,9223372036854775808.0,FLOAT,Float,9.223372E18
            String,680564693277057720000000000000000000000,FLOAT,Float,Infinity
            String,680564693277057720000000000000000000000.0,FLOAT,Float,Infinity
            Integer,123,AUTO,Integer,123
            Long,123,AUTO,Long,123
            Float,123.0,AUTO,Float,123.0
            Double,123.0,AUTO,Double,123.0
            String,123,AUTO,Integer,123
            String,0x7b,AUTO,Integer,123
            String,123.0,AUTO,Float,123.0
            String,1.230000e+02,AUTO,Float,123.0
            Float,123.45,AUTO,Float,123.45
            Double,123.45,AUTO,Double,123.45
            String,123.45,AUTO,Float,123.45
            String,1.234500e+02,AUTO,Float,123.45
            String,0x1.edcdp6,AUTO,Float,123.450195
            Integer,16777217,AUTO,Integer,16777217
            Long,16777217,AUTO,Long,16777217
            Double,1.6777217E7,AUTO,Double,1.6777217E7
            String,16777217,AUTO,Integer,16777217
            String,16777217.0,AUTO,Float,1.6777216E7
            Long,2147483648,AUTO,Long,2147483648
            Double,2.147483648E9,AUTO,Double,2.147483648E9
            String,2147483648,AUTO,Long,2147483648
            String,2147483648.0,AUTO,Float,2.1474836E9
            Long,9007199254740993,AUTO,Long,9007199254740993
            String,9007199254740993,AUTO,Long,9007199254740993
            String,9007199254740993.0,AUTO,Float,9.007199E15
            String,9223372036854775808,AUTO,Float,9.223372E18
            String,9223372036854775808.0,AUTO,Float,9.223372E18
            String,680564693277057720000000000000000000000,AUTO,Float,Infinity
            String,680564693277057720000000000000000000000.0,AUTO,Float,Infinity
            """;

        static Stream<TestCase> streamTests() {
            return Stream.of(TEST_CASES_CSV.split("\n")).map(NumericConversionTestMatrix::parseTestCaseFromCsv);
        }

        private static TestCase parseTestCaseFromCsv(String csv) {
            String[] fields = csv.split(",");
            return switch (fields.length) {
                case 5 -> new ExpectConvertsTestCase(
                    parseObjectOfType(fields[0], fields[1]),
                    Type.valueOf(fields[2]),
                    parseObjectOfType(fields[3], fields[4])
                );
                case 4 -> {
                    if (fields[3].equals("THROWS")) {
                        yield new ExpectThrowsTestCase(parseObjectOfType(fields[0], fields[1]), Type.valueOf(fields[2]));
                    } else {
                        throw new IllegalArgumentException("With 4 comma-delimited fields, expected 4th to be THROWS, was " + fields[3]);
                    }
                }
                default -> throw new IllegalArgumentException("Expected 4 or 5 comma-delimited fields, got " + csv);
            };
        }

        private static Object parseObjectOfType(String type, String string) {
            return switch (type) {
                case "Integer" -> Integer.decode(string);
                case "Long" -> Long.decode(string);
                case "Float" -> Float.valueOf(string);
                case "Double" -> Double.valueOf(string);
                case "String" -> string;
                default -> throw new IllegalArgumentException("Unexpected type " + type);
            };
        }

        interface TestCase {

            Object input();

            Type targetType();

            TestResult run();

            default Object attemptConversion() throws Exception {
                IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("field", input())));
                Processor processor = new ConvertProcessor(randomAlphaOfLength(10), null, "field", "field", targetType(), false);
                processor.execute(ingestDocument);
                return ingestDocument.getFieldValue("field", Object.class);
            }

            default String describeTest() {
                return Strings.format("converting (%s) %s to %s", input().getClass().getSimpleName(), input(), targetType());
            }
        }

        record ExpectConvertsTestCase(Object input, Type targetType, Object expectedOutput) implements TestCase {

            @Override
            public TestResult run() {
                try {
                    Object actualOutput = attemptConversion();
                    if (expectedOutput.equals(actualOutput)) {
                        return new TestPass(this);
                    } else {
                        return new TestFailureWrongValue(this, expectedOutput, actualOutput);
                    }
                } catch (Exception e) {
                    return new TestFailureUnexpectedException(this, expectedOutput, e);
                }
            }

            @Override
            public String toString() {
                return Strings.format(
                    "Expected %s to give (%s) %s",
                    describeTest(),
                    expectedOutput.getClass().getSimpleName(),
                    expectedOutput
                );
            }
        }

        record ExpectThrowsTestCase(Object input, Type targetType) implements TestCase {

            @Override
            public TestResult run() {
                try {
                    Object actualOutput = attemptConversion();
                    return new TestFailureMissingException(this, actualOutput);
                } catch (Exception e) {
                    return new TestPass(this);
                }
            }

            @Override
            public String toString() {
                return Strings.format("Expected %s to throw", describeTest());
            }
        }

        interface TestResult {}

        record TestPass(TestCase testCase) implements TestResult {}

        record TestFailureWrongValue(TestCase testCase, Object expected, Object actual) implements TestResult {

            @Override
            public String toString() {
                return Strings.format("%s but got (%s) %s", testCase, actual.getClass().getSimpleName(), actual);
            }
        }

        record TestFailureUnexpectedException(TestCase testCase, Object expected, Exception threw) implements TestResult {

            @Override
            public String toString() {
                return Strings.format("%s but threw (%s) %s", testCase, threw.getClass().getSimpleName(), threw.getMessage());
            }
        }

        record TestFailureMissingException(TestCase testCase, Object actual) implements TestResult {

            @Override
            public String toString() {
                return Strings.format("%s but got (%s) %s", testCase, actual.getClass().getSimpleName(), actual);
            }
        }
    }

    public void testNumericConversionMatrix() {
        List<NumericConversionTestMatrix.TestResult> testResults = NumericConversionTestMatrix.streamTests()
            .map(NumericConversionTestMatrix.TestCase::run)
            .toList();
        assertThat(testResults, everyItem(instanceOf(NumericConversionTestMatrix.TestPass.class)));
    }
}
