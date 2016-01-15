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
package org.elasticsearch.index.query.support;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class QueryInnerHitsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        copyAndAssert(new QueryInnerHits());
        copyAndAssert(new QueryInnerHits("foo", new InnerHitsBuilder.InnerHit()));
        copyAndAssert(new QueryInnerHits("foo", null));
        copyAndAssert(new QueryInnerHits("foo", new InnerHitsBuilder.InnerHit().setSize(randomIntBetween(0, 100))));
    }

    public void testToXContent() throws IOException {
        assertJson("{\"inner_hits\":{}}", new QueryInnerHits());
        assertJson("{\"inner_hits\":{\"name\":\"foo\"}}", new QueryInnerHits("foo", new InnerHitsBuilder.InnerHit()));
        assertJson("{\"inner_hits\":{\"name\":\"bar\"}}", new QueryInnerHits("bar", null));
        assertJson("{\"inner_hits\":{\"name\":\"foo\",\"size\":42}}", new QueryInnerHits("foo", new InnerHitsBuilder.InnerHit().setSize(42)));
        assertJson("{\"inner_hits\":{\"name\":\"boom\",\"from\":66,\"size\":666}}", new QueryInnerHits("boom", new InnerHitsBuilder.InnerHit().setFrom(66).setSize(666)));
    }

    private void assertJson(String expected, QueryInnerHits hits) throws IOException {
        QueryInnerHits queryInnerHits = copyAndAssert(hits);
        String actual;
        if (randomBoolean()) {
            actual = oneLineJSON(queryInnerHits);
        } else {
            actual = oneLineJSON(hits);
        }
        assertEquals(expected, actual);
        XContentParser parser = hits.getXcontentParser();
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        QueryInnerHits other = copyAndAssert(new QueryInnerHits(parser));
        assertEquals(expected, oneLineJSON(other));
    }

    public QueryInnerHits copyAndAssert(QueryInnerHits hits) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        hits.writeTo(out);
        QueryInnerHits copy = randomBoolean() ? hits.readFrom(StreamInput.wrap(out.bytes())) : new QueryInnerHits(StreamInput.wrap(out.bytes()));
        assertEquals(copy.toString() + " vs. " + hits.toString(), copy, hits);
        return copy;
    }

    private String oneLineJSON(QueryInnerHits hits) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        hits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.string().trim();
    }
}
