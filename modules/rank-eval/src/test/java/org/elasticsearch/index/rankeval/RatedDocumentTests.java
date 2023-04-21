/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class RatedDocumentTests extends ESTestCase {

    public static RatedDocument createRatedDocument() {
        return new RatedDocument(randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt());
    }

    public void testXContentParsing() throws IOException {
        RatedDocument testItem = createRatedDocument();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            RatedDocument parsedItem = RatedDocument.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        RatedDocument testItem = createRatedDocument();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            XContentParseException exception = expectThrows(XContentParseException.class, () -> RatedDocument.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[rated_document] unknown field"));
        }
    }

    public void testSerialization() throws IOException {
        RatedDocument original = createRatedDocument();
        RatedDocument deserialized = ESTestCase.copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            RatedDocument::new
        );
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createRatedDocument(), original -> {
            return new RatedDocument(original.getIndex(), original.getDocID(), original.getRating());
        }, RatedDocumentTests::mutateTestItem);
    }

    private static RatedDocument mutateTestItem(RatedDocument original) {
        int rating = original.getRating();
        String index = original.getIndex();
        String docId = original.getDocID();

        switch (randomIntBetween(0, 2)) {
            case 0 -> rating = randomValueOtherThan(rating, () -> randomInt());
            case 1 -> index = randomValueOtherThan(index, () -> randomAlphaOfLength(10));
            case 2 -> docId = randomValueOtherThan(docId, () -> randomAlphaOfLength(10));
            default -> throw new IllegalStateException("The test should only allow two parameters mutated");
        }
        return new RatedDocument(index, docId, rating);
    }
}
