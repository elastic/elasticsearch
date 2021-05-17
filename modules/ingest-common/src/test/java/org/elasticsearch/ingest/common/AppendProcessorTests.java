/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.Metadata;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsInAnyOrder;

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
            appendProcessor = createAppendProcessor(field, value, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, true);
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
            appendProcessor = createAppendProcessor(field, value, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, true);
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
            appendProcessor = createAppendProcessor(field, value, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values, true);
        }
        appendProcessor.execute(ingestDocument);
        List<?> fieldValue = ingestDocument.getFieldValue(field, List.class);
        assertThat(fieldValue.size(), equalTo(values.size() + 1));
        assertThat(fieldValue.get(0), equalTo(initialValue));
        for (int i = 1; i < values.size() + 1; i++) {
            assertThat(fieldValue.get(i), equalTo(values.get(i - 1)));
        }
    }

    public void testAppendMetadataExceptVersion() throws Exception {
        // here any metadata field value becomes a list, which won't make sense in most of the cases,
        // but support for append is streamlined like for set so we test it
        Metadata randomMetadata = randomFrom(Metadata.INDEX, Metadata.ID, Metadata.ROUTING);
        List<String> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            String value = randomAlphaOfLengthBetween(1, 10);
            values.add(value);
            appendProcessor = createAppendProcessor(randomMetadata.getFieldName(), value, true);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(randomAlphaOfLengthBetween(1, 10));
            }
            appendProcessor = createAppendProcessor(randomMetadata.getFieldName(), values, true);
        }

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Object initialValue = ingestDocument.getSourceAndMetadata().get(randomMetadata.getFieldName());
        appendProcessor.execute(ingestDocument);
        List<?> list = ingestDocument.getFieldValue(randomMetadata.getFieldName(), List.class);
        if (initialValue == null) {
            assertThat(list, equalTo(values));
        } else {
            assertThat(list.size(), equalTo(values.size() + 1));
            assertThat(list.get(0), equalTo(initialValue));
            for (int i = 1; i < list.size(); i++) {
                assertThat(list.get(i), equalTo(values.get(i - 1)));
            }
        }
    }

    public void testAppendingDuplicateValueToScalarDoesNotModifyDocument() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String originalValue = randomAlphaOfLengthBetween(1, 10);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, originalValue);

        List<Object> valuesToAppend = new ArrayList<>();
        valuesToAppend.add(originalValue);
        Processor appendProcessor = createAppendProcessor(field, valuesToAppend, false);
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
        Processor appendProcessor = createAppendProcessor(field, valuesToAppend, false);
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
        Processor appendProcessor = createAppendProcessor(originalField, valuesToAppend, false);
        appendProcessor.execute(ingestDocument);
        List<?> fieldValue = ingestDocument.getFieldValue(originalField, List.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(fieldValue, containsInAnyOrder(expectedValues.toArray()));
    }

    private static Processor createAppendProcessor(String fieldName, Object fieldValue, boolean allowDuplicates) {
        return new AppendProcessor(randomAlphaOfLength(10),
            null, new TestTemplateService.MockTemplateScript.Factory(fieldName),
            ValueSource.wrap(fieldValue, TestTemplateService.instance()), allowDuplicates);
    }

    private enum Scalar {
        INTEGER {
            @Override
            Object randomValue() {
                return randomInt();
            }
        }, DOUBLE {
            @Override
            Object randomValue() {
                return randomDouble();
            }
        }, FLOAT {
            @Override
            Object randomValue() {
                return randomFloat();
            }
        }, BOOLEAN {
            @Override
            Object randomValue() {
                return randomBoolean();
            }
        }, STRING {
            @Override
            Object randomValue() {
                return randomAlphaOfLengthBetween(1, 10);
            }
        }, MAP {
            @Override
            Object randomValue() {
                int numItems = randomIntBetween(1, 10);
                Map<String, Object> map = new HashMap<>(numItems);
                for (int i = 0; i < numItems; i++) {
                    map.put(randomAlphaOfLengthBetween(1, 10), randomFrom(Scalar.values()).randomValue());
                }
                return map;
            }
        }, NULL {
            @Override
            Object randomValue() {
                return null;
            }
        };

        abstract Object randomValue();
    }
}
