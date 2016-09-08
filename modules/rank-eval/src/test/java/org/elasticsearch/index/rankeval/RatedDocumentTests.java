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
}
