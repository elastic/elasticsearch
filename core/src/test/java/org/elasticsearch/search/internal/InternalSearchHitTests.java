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

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightFieldTests;
import org.elasticsearch.search.internal.InternalSearchHit.InternalNestedIdentity;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalSearchHitTests extends ESTestCase {

    private static Set<String> META_FIELDS = Sets.newHashSet("_uid", "_all", "_parent", "_routing", "_size", "_timestamp", "_ttl");

    public static InternalSearchHit createTestItem(boolean withOptionalInnerHits) {
        int internalId = randomInt();
        String uid = randomAsciiOfLength(10);
        Text type = new Text(randomAsciiOfLengthBetween(5, 10));
        InternalNestedIdentity nestedIdentity = null;
        if (randomBoolean()) {
            nestedIdentity = InternalNestedIdentityTests.createTestItem(randomIntBetween(0, 2));
        }
        Map<String, SearchHitField> fields = new HashMap<>();
        if (randomBoolean()) {
            int size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                Tuple<List<Object>, List<Object>> values = RandomObjects.randomStoredFieldValues(random(),
                        XContentType.JSON);
                if (randomBoolean()) {
                    String metaField = randomFrom(META_FIELDS);
                    fields.put(metaField, new InternalSearchHitField(metaField, values.v1()));
                } else {
                    String fieldName = randomAsciiOfLengthBetween(5, 10);
                    fields.put(fieldName, new InternalSearchHitField(fieldName, values.v1()));
                }
            }
        }
        InternalSearchHit hit = new InternalSearchHit(internalId, uid, type, nestedIdentity, fields);
        if (frequently()) {
            if (rarely()) {
                hit.score(Float.NaN);
            } else {
                hit.score(randomFloat());
            }
        }
        if (frequently()) {
            hit.sourceRef(RandomObjects.randomSource(random()));
        }
        if (randomBoolean()) {
            hit.version(randomLong());
        }
        if (randomBoolean()) {
            hit.sortValues(SearchSortValuesTests.createTestItem());
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            Map<String, HighlightField> highlightFields = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                highlightFields.put(randomAsciiOfLength(5), HighlightFieldTests.createTestItem());
            }
            hit.highlightFields(highlightFields);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            String[] matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = randomAsciiOfLength(5);
            }
            hit.matchedQueries(matchedQueries);
        }
        if (randomBoolean()) {
            hit.explanation(createExplanation(randomIntBetween(0, 5)));
        }
        if (withOptionalInnerHits) {
            int innerHitsSize = randomIntBetween(0, 3);
            Map<String, InternalSearchHits> innerHits = new HashMap<>(innerHitsSize);
            for (int i = 0; i < innerHitsSize; i++) {
                innerHits.put(randomAsciiOfLength(5), InternalSearchHitsTests.createTestItem());
            }
            hit.setInnerHits(innerHits);
        }
        if (randomBoolean()) {
            hit.shard(new SearchShardTarget(randomAsciiOfLengthBetween(5, 10),
                    new ShardId(new Index(randomAsciiOfLengthBetween(5, 10), randomAsciiOfLengthBetween(5, 10)), randomInt())));
        }
        return hit;
    }

    public void testFromXContent() throws IOException {
        InternalSearchHit searchHit = createTestItem(true);
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder = searchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        parser.nextToken(); // jump to first START_OBJECT
        InternalSearchHit parsed = InternalSearchHit.fromXContent(parser);
        assertToXContentEquivalent(builder.bytes(), toXContent(parsed, xcontentType), xcontentType);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        InternalSearchHit internalSearchHit = new InternalSearchHit(1, "id1", new Text("type"), Collections.emptyMap());
        internalSearchHit.score(1.5f);
        XContentBuilder builder = JsonXContent.contentBuilder();
        internalSearchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":1.5}", builder.string());
    }

    public void testSerializeShardTarget() throws Exception {
        SearchShardTarget target = new SearchShardTarget("_node_id", new Index("_index", "_na_"), 0);

        Map<String, InternalSearchHits> innerHits = new HashMap<>();
        InternalSearchHit innerHit1 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit1.shard(target);
        InternalSearchHit innerInnerHit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerInnerHit2.shard(target);
        innerHits.put("1", new InternalSearchHits(new InternalSearchHit[]{innerInnerHit2}, 1, 1f));
        innerHit1.setInnerHits(innerHits);
        InternalSearchHit innerHit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit2.shard(target);
        InternalSearchHit innerHit3 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit3.shard(target);

        innerHits = new HashMap<>();
        InternalSearchHit hit1 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHits.put("1", new InternalSearchHits(new InternalSearchHit[]{innerHit1, innerHit2}, 1, 1f));
        innerHits.put("2", new InternalSearchHits(new InternalSearchHit[]{innerHit3}, 1, 1f));
        hit1.shard(target);
        hit1.setInnerHits(innerHits);

        InternalSearchHit hit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        hit2.shard(target);

        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[]{hit1, hit2}, 2, 1f);

        BytesStreamOutput output = new BytesStreamOutput();
        hits.writeTo(output);
        InputStream input = output.bytes().streamInput();
        InternalSearchHits results = InternalSearchHits.readSearchHits(new InputStreamStreamInput(input));
        assertThat(results.getAt(0).shard(), equalTo(target));
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getInnerHits().get("1").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(1).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("2").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(1).shard(), equalTo(target));
    }

    public void testNullSource() throws Exception {
        InternalSearchHit searchHit = new InternalSearchHit(0, "_id", new Text("_type"), null);

        assertThat(searchHit.source(), nullValue());
        assertThat(searchHit.sourceRef(), nullValue());
        assertThat(searchHit.sourceAsMap(), nullValue());
        assertThat(searchHit.sourceAsString(), nullValue());
        assertThat(searchHit.getSource(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
    }

    public void testHasSource() {
        InternalSearchHit searchHit = new InternalSearchHit(randomInt());
        assertFalse(searchHit.hasSource());
        searchHit.sourceRef(new BytesArray("{}"));
        assertTrue(searchHit.hasSource());
    }

    private static Explanation createExplanation(int depth) {
        String description = randomAsciiOfLengthBetween(5, 20);
        float value = randomFloat();
        List<Explanation> details = new ArrayList<>();
        if (depth > 0) {
            int numberOfDetails = randomIntBetween(1, 3);
            for (int i = 0; i < numberOfDetails; i++) {
                details.add(createExplanation(depth - 1));
            }
        }
        return Explanation.match(value, description, details);
    }
}
