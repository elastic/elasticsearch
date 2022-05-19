/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class DocumentFieldTests extends ESTestCase {

    public void testToXContent() {
        DocumentField documentField = new DocumentField("field", Arrays.asList("value1", "value2"), Arrays.asList("ignored1", "ignored2"));
        String output = Strings.toString(documentField.getValidValuesWriter());
        assertEquals("{\"field\":[\"value1\",\"value2\"]}", output);
        String ignoredOutput = Strings.toString(documentField.getIgnoredValuesWriter());
        assertEquals("{\"field\":[\"ignored1\",\"ignored2\"]}", ignoredOutput);
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(
            randomDocumentField(XContentType.JSON).v1(),
            DocumentFieldTests::copyDocumentField,
            DocumentFieldTests::mutateDocumentField
        );
    }

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<DocumentField, DocumentField> tuple = randomDocumentField(xContentType);
        DocumentField documentField = tuple.v1();
        DocumentField expectedDocumentField = tuple.v2();
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(
            documentField.getValidValuesWriter(),
            xContentType,
            ToXContent.EMPTY_PARAMS,
            humanReadable
        );
        // test that we can parse what we print out
        DocumentField parsedDocumentField;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            // we need to move to the next token, the start object one that we manually added is not expected
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedDocumentField = DocumentField.fromXContent(parser);
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(expectedDocumentField, parsedDocumentField);
        BytesReference finalBytes = toXContent(parsedDocumentField.getValidValuesWriter(), xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    private static DocumentField copyDocumentField(DocumentField documentField) {
        return new DocumentField(documentField.getName(), documentField.getValues());
    }

    private static DocumentField mutateDocumentField(DocumentField documentField) {
        List<Supplier<DocumentField>> mutations = new ArrayList<>();
        mutations.add(() -> new DocumentField(randomUnicodeOfCodepointLength(15), documentField.getValues()));
        mutations.add(() -> new DocumentField(documentField.getName(), randomDocumentField(XContentType.JSON).v1().getValues()));
        final int index = randomFrom(0, 1);
        final DocumentField randomCandidate = mutations.get(index).get();
        if (documentField.equals(randomCandidate) == false) {
            return randomCandidate;
        } else {
            // we are unlucky and our random mutation is equal to our mutation, try the other candidate
            final DocumentField otherCandidate = mutations.get(1 - index).get();
            assert documentField.equals(otherCandidate) == false : documentField;
            return otherCandidate;
        }
    }

    public static Tuple<DocumentField, DocumentField> randomDocumentField(XContentType xContentType) {
        return randomDocumentField(xContentType, randomBoolean(), fieldName -> false);  // don't exclude any meta-fields
    }

    public static Tuple<DocumentField, DocumentField> randomDocumentField(
        XContentType xContentType,
        boolean isMetafield,
        Predicate<String> excludeMetaFieldFilter
    ) {
        if (isMetafield) {
            String metaField = randomValueOtherThanMany(excludeMetaFieldFilter, () -> randomFrom(IndicesModule.getBuiltInMetadataFields()));
            DocumentField documentField;
            if (metaField.equals(IgnoredFieldMapper.NAME)) {
                int numValues = randomIntBetween(1, 3);
                List<Object> ignoredFields = new ArrayList<>(numValues);
                for (int i = 0; i < numValues; i++) {
                    ignoredFields.add(randomAlphaOfLengthBetween(3, 10));
                }
                documentField = new DocumentField(metaField, ignoredFields);
            } else {
                // meta fields are single value only, besides _ignored
                documentField = new DocumentField(metaField, Collections.singletonList(randomAlphaOfLengthBetween(3, 10)));
            }
            return Tuple.tuple(documentField, documentField);
        } else {
            return switch (randomIntBetween(0, 2)) {
                case 0 -> {
                    String fieldName = randomAlphaOfLengthBetween(3, 10);
                    Tuple<List<Object>, List<Object>> tuple = RandomObjects.randomStoredFieldValues(random(), xContentType);
                    DocumentField input = new DocumentField(fieldName, tuple.v1());
                    DocumentField expected = new DocumentField(fieldName, tuple.v2());
                    yield Tuple.tuple(input, expected);
                }
                case 1 -> {
                    List<Object> listValues = randomList(1, 5, () -> randomList(1, 5, ESTestCase::randomInt));
                    DocumentField listField = new DocumentField(randomAlphaOfLength(5), listValues);
                    yield Tuple.tuple(listField, listField);
                }
                case 2 -> {
                    List<Object> objectValues = randomList(1, 5, () -> {
                        Map<String, Object> values = new HashMap<>();
                        values.put(randomAlphaOfLength(5), randomInt());
                        values.put(randomAlphaOfLength(5), randomBoolean());
                        values.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
                        return values;
                    });
                    DocumentField objectField = new DocumentField(randomAlphaOfLength(5), objectValues);
                    yield Tuple.tuple(objectField, objectField);
                }
                default -> throw new IllegalStateException();
            };
        }
    }
}
