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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.OptionalInt;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class RatedSearchHitTests extends ESTestCase {

    private static final ConstructingObjectParser<RatedSearchHit, Void> PARSER = new ConstructingObjectParser<>(
        "rated_hit",
        true,
        a -> new RatedSearchHit((SearchHit) a[0], (OptionalInt) a[1])
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SearchResponseUtils.parseSearchHit(p),
            new ParseField("hit")
        );
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? OptionalInt.empty() : OptionalInt.of(p.intValue()),
            new ParseField("rating"),
            ObjectParser.ValueType.INT_OR_NULL
        );
    }

    public static RatedSearchHit parseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static RatedSearchHit randomRatedSearchHit() {
        OptionalInt rating = randomBoolean() ? OptionalInt.empty() : OptionalInt.of(randomIntBetween(0, 5));
        SearchHit searchHit = SearchHit.unpooled(randomIntBetween(0, 10), randomAlphaOfLength(10));
        RatedSearchHit ratedSearchHit = new RatedSearchHit(searchHit, rating);
        return ratedSearchHit;
    }

    private static RatedSearchHit mutateTestItem(RatedSearchHit original) {
        OptionalInt rating = original.getRating();
        SearchHit hit = original.getSearchHit();
        switch (randomIntBetween(0, 1)) {
            case 0 -> rating = rating.isPresent() ? OptionalInt.of(rating.getAsInt() + 1) : OptionalInt.of(randomInt(5));
            case 1 -> hit = SearchHit.unpooled(hit.docId(), hit.getId() + randomAlphaOfLength(10));
            default -> throw new IllegalStateException("The test should only allow two parameters mutated");
        }
        return new RatedSearchHit(hit, rating);
    }

    public void testSerialization() throws IOException {
        RatedSearchHit original = randomRatedSearchHit();
        RatedSearchHit deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testXContentRoundtrip() throws IOException {
        RatedSearchHit testItem = randomRatedSearchHit();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            RatedSearchHit parsedItem = parseInstance(parser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(randomRatedSearchHit(), RatedSearchHitTests::copy, RatedSearchHitTests::mutateTestItem);
    }

    private static RatedSearchHit copy(RatedSearchHit original) throws IOException {
        return ESTestCase.copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), RatedSearchHit::new);
    }

}
