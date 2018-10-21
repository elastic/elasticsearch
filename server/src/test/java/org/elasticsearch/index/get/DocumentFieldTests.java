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

package org.elasticsearch.index.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class DocumentFieldTests extends ESTestCase {

    public void testToXContent() {
        DocumentField documentField = new DocumentField("field", Arrays.asList("value1", "value2"));
        String output = Strings.toString(documentField);
        assertEquals("{\"field\":[\"value1\",\"value2\"]}", output);
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomDocumentField(XContentType.JSON).v1(), DocumentFieldTests::copyDocumentField,
                DocumentFieldTests::mutateDocumentField);
    }

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<DocumentField, DocumentField> tuple = randomDocumentField(xContentType);
        DocumentField documentField = tuple.v1();
        DocumentField expectedDocumentField = tuple.v2();
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(documentField, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        //test that we can parse what we print out
        DocumentField parsedDocumentField;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            //we need to move to the next token, the start object one that we manually added is not expected
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedDocumentField = DocumentField.fromXContent(parser);
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(expectedDocumentField, parsedDocumentField);
        BytesReference finalBytes = toXContent(parsedDocumentField, xContentType, humanReadable);
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
        if (!documentField.equals(randomCandidate)) {
            return randomCandidate;
        } else {
            // we are unlucky and our random mutation is equal to our mutation, try the other candidate
            final DocumentField otherCandidate = mutations.get(1 - index).get();
            assert !documentField.equals(otherCandidate) : documentField;
            return otherCandidate;
        }
    }

    public static Tuple<DocumentField, DocumentField> randomDocumentField(XContentType xContentType) {
        if (randomBoolean()) {
            String metaField = randomValueOtherThanMany(field -> field.equals(TypeFieldMapper.NAME)
                    || field.equals(IndexFieldMapper.NAME) || field.equals(IdFieldMapper.NAME),
                () -> randomFrom(MapperService.getAllMetaFields()));
            DocumentField documentField;
            if (metaField.equals(IgnoredFieldMapper.NAME)) {
                int numValues = randomIntBetween(1, 3);
                List<Object> ignoredFields = new ArrayList<>(numValues);
                for (int i = 0; i < numValues; i++) {
                    ignoredFields.add(randomAlphaOfLengthBetween(3, 10));
                }
                documentField = new DocumentField(metaField, ignoredFields);
            } else {
                //meta fields are single value only, besides _ignored
                documentField = new DocumentField(metaField, Collections.singletonList(randomAlphaOfLengthBetween(3, 10)));
            }
            return Tuple.tuple(documentField, documentField);
        } else {
            String fieldName = randomAlphaOfLengthBetween(3, 10);
            Tuple<List<Object>, List<Object>> tuple = RandomObjects.randomStoredFieldValues(random(), xContentType);
            DocumentField input = new DocumentField(fieldName, tuple.v1());
            DocumentField expected = new DocumentField(fieldName, tuple.v2());
            return Tuple.tuple(input, expected);
        }
    }
}
