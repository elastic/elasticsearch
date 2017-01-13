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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class InternalSearchHitsTests extends ESTestCase {

    public static InternalSearchHits createTestItem() {
        int searchHits = randomIntBetween(0, 5);
        InternalSearchHit[] hits = new InternalSearchHit[searchHits];
        for (int i = 0; i < searchHits; i++) {
            hits[i] = InternalSearchHitTests.createTestItem(false); // creating random innerHits could create loops
        }
        long totalHits = randomLong();
        float maxScore = frequently() ? randomFloat() : Float.NaN;
        return new InternalSearchHits(hits, totalHits, maxScore);
    }

    public void testFromXContent() throws IOException {
        InternalSearchHits searchHits = createTestItem();
        XContentType xcontentType = XContentType.JSON; //randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder.startObject();
        builder = searchHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = createParser(builder);
        InternalSearchHits parsed = InternalSearchHits.fromXContent(parser);
        assertToXContentEquivalent(builder.bytes(), toXContent(parsed, xcontentType), xcontentType);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        parser.nextToken();
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        InternalSearchHit[] hits = new InternalSearchHit[] {
                new InternalSearchHit(1, "id1", new Text("type"), Collections.emptyMap()),
                new InternalSearchHit(2, "id2", new Text("type"), Collections.emptyMap()) };

        long totalHits = 1000;
        float maxScore = 1.5f;
        InternalSearchHits searchHits = new InternalSearchHits(hits, totalHits, maxScore);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        searchHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"hits\":{\"total\":1000,\"max_score\":1.5," +
                "\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":\"-Infinity\"},"+
                          "{\"_type\":\"type\",\"_id\":\"id2\",\"_score\":\"-Infinity\"}]}}", builder.string());
    }

}
