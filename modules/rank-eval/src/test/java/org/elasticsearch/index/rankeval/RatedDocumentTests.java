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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RatedDocumentTests extends ESTestCase {

    public void testXContentParsing() throws IOException {
        String index = randomAsciiOfLength(10);
        String type = randomAsciiOfLength(10);
        String docId = randomAsciiOfLength(10);
        int rating = randomInt();
        RatedDocument testItem = new RatedDocument(index, type, docId, rating);

        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        testItem.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        XContentParser itemParser = XContentHelper.createParser(shuffled.bytes());
        itemParser.nextToken();

        RankEvalContext context = new RankEvalContext(ParseFieldMatcher.STRICT, null, null, null);
        RatedDocument parsedItem = RatedDocument.fromXContent(itemParser, context);
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());

    }

}
