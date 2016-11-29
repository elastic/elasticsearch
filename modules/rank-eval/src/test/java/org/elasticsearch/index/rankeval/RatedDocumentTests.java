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

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

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
        XContentParser itemParser = RankEvalTestHelper.roundtrip(testItem);
        RatedDocument parsedItem = RatedDocument.fromXContent(itemParser, () -> ParseFieldMatcher.STRICT);
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());
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

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    public void testInvalidParsing() throws IOException {
        exception.expect(IllegalArgumentException.class);
        new RatedDocument(null, randomAsciiOfLength(10), randomAsciiOfLength(10), randomInt());
        
        exception.expect(IllegalArgumentException.class);
        new RatedDocument("", randomAsciiOfLength(10), randomAsciiOfLength(10), randomInt());

        exception.expect(IllegalArgumentException.class);
        new RatedDocument(randomAsciiOfLength(10), null, randomAsciiOfLength(10), randomInt());
        
        exception.expect(IllegalArgumentException.class);
        new RatedDocument(randomAsciiOfLength(10), "", randomAsciiOfLength(10), randomInt());
        
        exception.expect(IllegalArgumentException.class);
        new RatedDocument(randomAsciiOfLength(10), randomAsciiOfLength(10), null, randomInt());

        exception.expect(IllegalArgumentException.class);
        new RatedDocument(randomAsciiOfLength(10), randomAsciiOfLength(10), "", randomInt());
    }

    private static RatedDocument mutateTestItem(RatedDocument original) {
        int rating = original.getRating();
        String index = original.getIndex();
        String type = original.getType();
        String docId = original.getDocID();
        switch (randomIntBetween(0, 3)) {
        case 0:
        {
            int mutation = randomInt();
            while (mutation == rating) {
                mutation = randomInt();
            }
            rating = mutation;
            break;
        }
        case 1:
        {
            String mutation = randomAsciiOfLength(10);
            while (mutation.equals(index)) {
                mutation = randomAsciiOfLength(10);
            }
            index = mutation;
            break;
        }
        case 2:
        {
            String mutation = randomAsciiOfLength(10);
            while (mutation.equals(type)) {
                mutation = randomAsciiOfLength(10);
            }
            type = mutation;
            break;
        }
        case 3:
        {
            String mutation = randomAsciiOfLength(10);
            while (mutation.equals(docId)) {
                mutation = randomAsciiOfLength(10);
            }
            docId = mutation;
            break;
        }
        default:
            throw new IllegalStateException("The test should only allow two parameters mutated");
        }
        return new RatedDocument(index, type, docId, rating);
    }
}
