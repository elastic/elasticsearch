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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RatedDocumentTests extends ESTestCase {

    public static RatedDocument createRatedDocument() {
        String index = randomAsciiOfLength(10);
        String type = randomAsciiOfLength(10);
        String docId = randomAsciiOfLength(10);
        int rating = randomInt();

        return new RatedDocument(index, type, docId, rating);
    }

    public void testXContentParsing() throws IOException {
        RatedDocument testItem = createRatedDocument();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(
                testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            RatedDocument parsedItem = RatedDocument.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testSerialization() throws IOException {
        RatedDocument original = createRatedDocument();
        RatedDocument deserialized = RankEvalTestHelper.copy(original, RatedDocument::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        RatedDocument testItem = createRatedDocument();
        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(testItem),
                RankEvalTestHelper.copy(testItem, RatedDocument::new));
    }

    public void testInvalidParsing() throws IOException {
        expectThrows(IllegalArgumentException.class,
                () -> new RatedDocument(null, "abc", "abc", 10));
        expectThrows(IllegalArgumentException.class, () -> new RatedDocument("", "abc", "abc", 10));
        expectThrows(IllegalArgumentException.class,
                () -> new RatedDocument("abc", null, "abc", 10));
        expectThrows(IllegalArgumentException.class, () -> new RatedDocument("abc", "", "abc", 10));
        expectThrows(IllegalArgumentException.class,
                () -> new RatedDocument("abc", "abc", null, 10));
        expectThrows(IllegalArgumentException.class, () -> new RatedDocument("abc", "abc", "", 10));
    }

    private static RatedDocument mutateTestItem(RatedDocument original) {
        int rating = original.getRating();
        String index = original.getIndex();
        String type = original.getType();
        String docId = original.getDocID();

        switch (randomIntBetween(0, 3)) {
        case 0:
            rating = randomValueOtherThan(rating, () -> randomInt());
            break;
        case 1:
            index = randomValueOtherThan(index, () -> randomAsciiOfLength(10));
            break;
        case 2:
            type = randomValueOtherThan(type, () -> randomAsciiOfLength(10));
            break;
        case 3:
            docId = randomValueOtherThan(docId, () -> randomAsciiOfLength(10));
            break;
        default:
            throw new IllegalStateException("The test should only allow two parameters mutated");
        }
        return new RatedDocument(index, type, docId, rating);
    }
}
