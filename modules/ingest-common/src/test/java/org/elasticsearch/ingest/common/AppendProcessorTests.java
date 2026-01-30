/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AppendProcessorTests extends ESTestCase {

    public void testAppendValuesToExistingList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = randomFrom(Scalar.values());
        List<Object> list = new ArrayList<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            list.add(scalar.randomValue());
        }
        List<Object> checkList = new ArrayList<>(list);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, list);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value, null, true, false);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, null, true, false);
        }
        appendProcessor.execute(ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(list.size(), equalTo(size + values.size()));
        for (int i = 0; i < size; i++) {
            assertThat(list.get(i), equalTo(checkList.get(i)));
        }
        for (int i = size; i < size + values.size(); i++) {
            assertThat(list.get(i), equalTo(values.get(i - size)));
        }
    }

    public void testAppendValuesToNonExistingList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String field = RandomDocumentPicks.randomFieldName(random());
        Scalar scalar = randomFrom(Scalar.values());
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value, null, true, false);
        } else {
            int valuesSize = randomIntBetween(1, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, null, true, false);
        }
        appendProcessor.execute(ingestDocument);
        List<?> list = ingestDocument.getFieldValue(field, List.class);
        assertThat(list, not(sameInstance(values)));
        assertThat(list, equalTo(values));
    }

    public void testConvertScalarToList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = randomFrom(Scalar.values());
        Object initialValue = scalar.randomValue();
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, initialValue);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value, null, true, false);
        } else {
            int valuesSize = randomIntBetween(1, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, null, true, false);
        }
        appendProcessor.execute(ingestDocument);
        List<?> fieldValue = ingestDocument.getFieldValue(field, List.class);
        assertThat(fieldValue.size(), equalTo(values.size() + 1));
        assertThat(fieldValue.get(0), equalTo(initialValue));
        for (int i = 1; i < values.size() + 1; i++) {
            assertThat(fieldValue.get(i), equalTo(values.get(i - 1)));
        }
    }

    public void testAppendingDuplicateValueToScalarDoesNotModifyDocument() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String originalValue = randomAlphaOfLengthBetween(1, 10);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, originalValue);

        List<Object> valuesToAppend = new ArrayList<>();
        valuesToAppend.add(originalValue);
        Processor appendProcessor = createAppendProcessor(field, valuesToAppend, null, false, false);
        appendProcessor.execute(ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        assertThat(fieldValue, not(instanceOf(List.class)));
        assertThat(fieldValue, equalTo(originalValue));
    }

    public void testAppendingUniqueValueToScalar() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String originalValue = randomAlphaOfLengthBetween(1, 10);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, originalValue);

        List<Object> valuesToAppend = new ArrayList<>();
        String newValue = randomValueOtherThan(originalValue, () -> randomAlphaOfLengthBetween(1, 10));
        valuesToAppend.add(newValue);
        Processor appendProcessor = createAppendProcessor(field, valuesToAppend, null, false, false);
        appendProcessor.execute(ingestDocument);
        List<?> list = ingestDocument.getFieldValue(field, List.class);
        assertThat(list.size(), equalTo(2));
        assertThat(list, equalTo(List.of(originalValue, newValue)));
    }

    public void testAppendingToListWithDuplicatesDisallowed() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int size = randomIntBetween(0, 10);
        List<String> list = Stream.generate(() -> randomAlphaOfLengthBetween(1, 10)).limit(size).collect(Collectors.toList());
        String originalField = RandomDocumentPicks.addRandomField(random(), ingestDocument, list);
        List<String> expectedValues = new ArrayList<>(list);
        List<String> existingValues = randomSubsetOf(list);

        // generate new values
        int nonexistingValuesSize = randomIntBetween(0, 10);
        Set<String> newValues = Stream.generate(() -> randomAlphaOfLengthBetween(1, 10))
            .limit(nonexistingValuesSize)
            .collect(Collectors.toSet());

        // create a set using the new values making sure there are no overlapping values already present in the existing values
        Set<String> nonexistingValues = Sets.difference(newValues, new HashSet<>(list));
        List<String> valuesToAppend = new ArrayList<>(existingValues);
        valuesToAppend.addAll(nonexistingValues);
        expectedValues.addAll(nonexistingValues);
        Collections.sort(valuesToAppend);

        // attempt to append both new and existing values
        Processor appendProcessor = createAppendProcessor(originalField, valuesToAppend, null, false, false);
        appendProcessor.execute(ingestDocument);
        List<?> fieldValue = ingestDocument.getFieldValue(originalField, List.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(fieldValue, containsInAnyOrder(expectedValues.toArray()));
    }

    public void testAppendingToListWithNoEmptyValuesAndEmptyValuesDisallowed() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = randomValueOtherThan(Scalar.NULL, () -> randomFrom(Scalar.values()));
        List<Object> list = new ArrayList<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            list.add(scalar.randomValue());
        }
        List<Object> checkList = new ArrayList<>(list);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, list);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value, null, true, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, null, true, true);
        }
        appendProcessor.execute(ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(list.size(), equalTo(size + values.size()));
        for (int i = 0; i < size; i++) {
            assertThat(list.get(i), equalTo(checkList.get(i)));
        }
        for (int i = size; i < size + values.size(); i++) {
            assertThat(list.get(i), equalTo(values.get(i - size)));
        }
    }

    public void testAppendingToListEmptyStringAndEmptyValuesDisallowed() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = Scalar.STRING;
        List<Object> list = new ArrayList<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            list.add(scalar.randomValue());
        }
        List<Object> checkList = new ArrayList<>(list);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, list);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value;
            if (randomBoolean()) {
                value = "";
            } else {
                value = scalar.randomValue();
                values.add(value);
            }
            appendProcessor = createAppendProcessor(field, value, null, true, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            List<Object> allValues = new ArrayList<>();
            for (int i = 0; i < valuesSize; i++) {
                Object value;
                if (randomBoolean()) {
                    value = "";
                } else {
                    value = scalar.randomValue();
                    values.add(value);
                }
                allValues.add(value);
            }
            appendProcessor = createAppendProcessor(field, allValues, null, true, true);
        }
        appendProcessor.execute(ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(list.size(), equalTo(size + values.size()));
        for (int i = 0; i < size; i++) {
            assertThat(list.get(i), equalTo(checkList.get(i)));
        }
        for (int i = size; i < size + values.size(); i++) {
            assertThat(list.get(i), equalTo(values.get(i - size)));
        }
    }

    public void testAppendingToNonExistingListEmptyStringAndEmptyValuesDisallowed() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String field = RandomDocumentPicks.randomFieldName(random());
        Scalar scalar = Scalar.STRING;
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value;
            if (randomBoolean()) {
                value = "";
            } else {
                value = scalar.randomValue();
                values.add(value);
            }
            appendProcessor = createAppendProcessor(field, value, null, true, true);
        } else {
            List<Object> allValues = new ArrayList<>();
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                Object value;
                if (randomBoolean()) {
                    value = "";
                } else {
                    value = scalar.randomValue();
                    values.add(value);
                }
                allValues.add(value);
            }
            appendProcessor = createAppendProcessor(field, allValues, null, true, true);
        }
        appendProcessor.execute(ingestDocument);
        List<?> list = ingestDocument.getFieldValue(field, List.class, true);
        assertThat(list, not(sameInstance(values)));
        if (values.isEmpty()) {
            assertThat(list, nullValue());
        } else {
            assertThat(list, equalTo(values));
        }
    }

    public void testCopyFromOtherFieldSimple() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        ingestDocument.setFieldValue("foo", 1);
        ingestDocument.setFieldValue("bar", 2);
        ingestDocument.setFieldValue("baz", new ArrayList<>(List.of(3)));

        createAppendProcessor("bar", null, "foo", false, false).execute(ingestDocument);
        createAppendProcessor("baz", null, "bar", false, false).execute(ingestDocument);
        createAppendProcessor("quux", null, "baz", false, false).execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getCtxMap().getSource();
        assertThat(result.get("foo"), equalTo(1));
        assertThat(result.get("bar"), equalTo(List.of(2, 1)));
        assertThat(result.get("baz"), equalTo(List.of(3, 2, 1)));
        assertThat(result.get("quux"), equalTo(List.of(3, 2, 1)));
    }

    public void testCopyFromOtherField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());

        // generate values, add some to a target field, the rest to a source field
        int size = randomIntBetween(0, 10);
        Set<String> allValues = Stream.generate(() -> randomAlphaOfLengthBetween(1, 10)).limit(size).collect(Collectors.toSet());
        List<String> originalValues = randomSubsetOf(allValues);
        List<String> additionalValues = new ArrayList<>(Sets.difference(new HashSet<>(allValues), new HashSet<>(originalValues)));
        List<String> targetFieldValue = new ArrayList<>(originalValues);
        String targetField = RandomDocumentPicks.addRandomField(random(), ingestDocument, targetFieldValue);
        String sourceField = RandomDocumentPicks.addRandomField(random(), ingestDocument, additionalValues);

        // add two empty values onto the source field, these will be ignored
        ingestDocument.appendFieldValue(sourceField, null);
        ingestDocument.appendFieldValue(sourceField, "");

        Processor appendProcessor = createAppendProcessor(targetField, null, sourceField, false, true);
        appendProcessor.execute(ingestDocument);
        List<?> fieldValue = ingestDocument.getFieldValue(targetField, List.class);
        assertThat(fieldValue, sameInstance(targetFieldValue));
        assertThat(fieldValue, containsInAnyOrder(allValues.toArray()));
        assertThat(fieldValue, not(contains(null, "")));
    }

    public void testCopyFromCopiesNonPrimitiveMutableTypes() throws Exception {
        Map<String, Object> document;
        IngestDocument ingestDocument;
        final String sourceField = "sourceField";
        final String targetField = "targetField";
        Processor processor = createAppendProcessor(targetField, null, sourceField, false, false);

        // map types
        document = new HashMap<>();
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("foo", "bar");
        document.put(sourceField, sourceMap);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        sourceMap.put("foo", "not-bar");
        Map<?, ?> outputMap = (Map<?, ?>) ingestDocument.getFieldValue(targetField, List.class).getFirst();
        assertThat(outputMap.get("foo"), equalTo("bar"));

        // set types
        document = new HashMap<>();
        Set<String> sourceSet = randomUnique(() -> randomAlphaOfLength(5), 5);
        Set<String> preservedSet = new HashSet<>(sourceSet);
        document.put(sourceField, sourceSet);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        sourceSet.add(randomValueOtherThanMany(sourceSet::contains, () -> randomAlphaOfLength(5)));
        Set<?> outputSet = (Set<?>) ingestDocument.getFieldValue(targetField, List.class).getFirst();
        assertThat(outputSet, equalTo(preservedSet));

        // list types (the outer list isn't used, but an inner list should be copied)
        document = new HashMap<>();
        List<String> sourceList = randomList(1, 5, () -> randomAlphaOfLength(5));
        List<String> preservedList = new ArrayList<>(sourceList);
        List<List<String>> wrappedSourceList = List.of(sourceList);
        document.put(sourceField, wrappedSourceList);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        sourceList.add(randomValueOtherThanMany(sourceList::contains, () -> randomAlphaOfLength(5)));
        List<?> unwrappedOutputList = (List<?>) ingestDocument.getFieldValue(targetField, List.class).getFirst();
        assertThat(unwrappedOutputList, equalTo(preservedList));

        // byte[] types
        document = new HashMap<>();
        byte[] sourceBytes = randomByteArrayOfLength(10);
        byte[] preservedBytes = new byte[sourceBytes.length];
        System.arraycopy(sourceBytes, 0, preservedBytes, 0, sourceBytes.length);
        document.put(sourceField, sourceBytes);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        sourceBytes[0] = sourceBytes[0] == 0 ? (byte) 1 : (byte) 0;
        byte[] outputBytes = (byte[]) ingestDocument.getFieldValue(targetField, List.class).getFirst();
        assertThat(outputBytes, equalTo(preservedBytes));

        // Date types
        document = new HashMap<>();
        Date sourceDate = new Date();
        Date preservedDate = new Date(sourceDate.getTime());
        document.put(sourceField, sourceDate);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        sourceDate.setTime(sourceDate.getTime() + 1);
        Date outputDate = (Date) ingestDocument.getFieldValue(targetField, List.class).getFirst();
        assertThat(outputDate, equalTo(preservedDate));
    }

    public void testCopyFromDeepCopiesNonPrimitiveMutableTypes() throws Exception {
        final String sourceField = "sourceField";
        final String targetField = "targetField";
        Processor processor = createAppendProcessor(targetField, null, sourceField, false, false);
        Map<String, Object> document = new HashMap<>();

        // a root map with values of map, set, list, bytes, date
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("foo", "bar");
        Set<String> sourceSet = randomUnique(() -> randomAlphaOfLength(5), 5);
        List<String> sourceList = randomList(1, 5, () -> randomAlphaOfLength(5));
        byte[] sourceBytes = randomByteArrayOfLength(10);
        Date sourceDate = new Date();
        Map<String, Object> root = new HashMap<>();
        root.put("foo", "bar");
        root.put("map", sourceMap);
        root.put("set", sourceSet);
        root.put("list", sourceList);
        root.put("bytes", sourceBytes);
        root.put("date", sourceDate);

        Set<String> preservedSet = new HashSet<>(sourceSet);
        List<String> preservedList = new ArrayList<>(sourceList);
        byte[] preservedBytes = new byte[sourceBytes.length];
        System.arraycopy(sourceBytes, 0, preservedBytes, 0, sourceBytes.length);
        Date preservedDate = new Date(sourceDate.getTime());

        document.put(sourceField, root);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        Map<?, ?> outputRoot = (Map<?, ?>) ingestDocument.getFieldValue(targetField, List.class).getFirst();

        root.put("foo", "not-bar");
        sourceMap.put("foo", "not-bar");
        sourceSet.add(randomValueOtherThanMany(sourceSet::contains, () -> randomAlphaOfLength(5)));
        sourceList.add(randomValueOtherThanMany(sourceList::contains, () -> randomAlphaOfLength(5)));
        sourceBytes[0] = sourceBytes[0] == 0 ? (byte) 1 : (byte) 0;
        sourceDate.setTime(sourceDate.getTime() + 1);

        assertThat(outputRoot.get("foo"), equalTo("bar"));
        assertThat(((Map<?, ?>) outputRoot.get("map")).get("foo"), equalTo("bar"));
        assertThat(((Set<?>) outputRoot.get("set")), equalTo(preservedSet));
        assertThat(((List<?>) outputRoot.get("list")), equalTo(preservedList));
        assertThat(((byte[]) outputRoot.get("bytes")), equalTo(preservedBytes));
        assertThat(((Date) outputRoot.get("date")), equalTo(preservedDate));
    }

    private static Processor createAppendProcessor(
        String fieldName,
        Object fieldValue,
        String copyFrom,
        boolean allowDuplicates,
        boolean ignoreEmptyValues
    ) {
        return new AppendProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(fieldName),
            ValueSource.wrap(fieldValue, TestTemplateService.instance()),
            copyFrom,
            allowDuplicates,
            ignoreEmptyValues
        );
    }

    private enum Scalar {
        INTEGER {
            @Override
            Object randomValue() {
                return randomInt();
            }
        },
        DOUBLE {
            @Override
            Object randomValue() {
                return randomDouble();
            }
        },
        FLOAT {
            @Override
            Object randomValue() {
                return randomFloat();
            }
        },
        BOOLEAN {
            @Override
            Object randomValue() {
                return randomBoolean();
            }
        },
        STRING {
            @Override
            Object randomValue() {
                return randomAlphaOfLengthBetween(1, 10);
            }
        },
        MAP {
            @Override
            Object randomValue() {
                int numItems = randomIntBetween(1, 10);
                Map<String, Object> map = Maps.newMapWithExpectedSize(numItems);
                for (int i = 0; i < numItems; i++) {
                    map.put(randomAlphaOfLengthBetween(1, 10), randomFrom(Scalar.values()).randomValue());
                }
                return map;
            }
        },
        NULL {
            @Override
            Object randomValue() {
                return null;
            }
        };

        abstract Object randomValue();
    }
}
