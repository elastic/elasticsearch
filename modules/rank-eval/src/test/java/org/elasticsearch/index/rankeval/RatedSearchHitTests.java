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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.OptionalInt;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class RatedSearchHitTests extends ESTestCase {

    public static RatedSearchHit randomRatedSearchHit() {
        OptionalInt rating = randomBoolean() ? OptionalInt.empty()
                : OptionalInt.of(randomIntBetween(0, 5));
        SearchHit searchHit = new SearchHit(randomIntBetween(0, 10), randomAlphaOfLength(10), Collections.emptyMap(),
            Collections.emptyMap());
        RatedSearchHit ratedSearchHit = new RatedSearchHit(searchHit, rating);
        return ratedSearchHit;
    }

    private static RatedSearchHit mutateTestItem(RatedSearchHit original) {
        OptionalInt rating = original.getRating();
        SearchHit hit = original.getSearchHit();
        switch (randomIntBetween(0, 1)) {
        case 0:
            rating = rating.isPresent() ? OptionalInt.of(rating.getAsInt() + 1) : OptionalInt.of(randomInt(5));
            break;
        case 1:
            hit = new SearchHit(hit.docId(), hit.getId() + randomAlphaOfLength(10), Collections.emptyMap(),
                Collections.emptyMap());
            break;
        default:
            throw new IllegalStateException("The test should only allow two parameters mutated");
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
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            RatedSearchHit parsedItem = RatedSearchHit.parse(parser);
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
