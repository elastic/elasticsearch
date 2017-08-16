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

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit.NestedIdentity;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightFieldTests;
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
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchHitTests extends ESTestCase {

    private static Set<String> META_FIELDS = Sets.newHashSet("_uid", "_all", "_parent", "_routing", "_size", "_timestamp", "_ttl");

    public static SearchHit createTestItem(boolean withOptionalInnerHits) {
        int internalId = randomInt();
        String uid = randomAlphaOfLength(10);
        Text type = new Text(randomAlphaOfLengthBetween(5, 10));
        NestedIdentity nestedIdentity = null;
        if (randomBoolean()) {
            nestedIdentity = NestedIdentityTests.createTestItem(randomIntBetween(0, 2));
        }
        Map<String, DocumentField> fields = new HashMap<>();
        if (randomBoolean()) {
            int size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                Tuple<List<Object>, List<Object>> values = RandomObjects.randomStoredFieldValues(random(),
                        XContentType.JSON);
                if (randomBoolean()) {
                    String metaField = randomFrom(META_FIELDS);
                    fields.put(metaField, new DocumentField(metaField, values.v1()));
                } else {
                    String fieldName = randomAlphaOfLengthBetween(5, 10);
                    fields.put(fieldName, new DocumentField(fieldName, values.v1()));
                }
            }
        }
        SearchHit hit = new SearchHit(internalId, uid, type, nestedIdentity, fields);
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
                highlightFields.put(randomAlphaOfLength(5), HighlightFieldTests.createTestItem());
            }
            hit.highlightFields(highlightFields);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            String[] matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = randomAlphaOfLength(5);
            }
            hit.matchedQueries(matchedQueries);
        }
        if (randomBoolean()) {
            hit.explanation(createExplanation(randomIntBetween(0, 5)));
        }
        if (withOptionalInnerHits) {
            int innerHitsSize = randomIntBetween(0, 3);
            Map<String, SearchHits> innerHits = new HashMap<>(innerHitsSize);
            for (int i = 0; i < innerHitsSize; i++) {
                innerHits.put(randomAlphaOfLength(5), SearchHitsTests.createTestItem());
            }
            hit.setInnerHits(innerHits);
        }
        if (randomBoolean()) {
            hit.shard(new SearchShardTarget(randomAlphaOfLengthBetween(5, 10),
                    new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)), randomInt()), null,
                    OriginalIndices.NONE));
        }
        return hit;
    }

    public void testFromXContent() throws IOException {
        SearchHit searchHit = createTestItem(true);
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(searchHit, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchHit parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    /**
     * This test adds randomized fields on all json objects and checks that we can parse it to
     * ensure the parsing is lenient for forward compatibility.
     * We need to exclude json objects with the "highlight" and "fields" field name since these
     * objects allow arbitrary keys (the field names that are queries). Also we want to exclude
     * to add anything under "_source" since it is not parsed, and avoid complexity by excluding
     * everything under "inner_hits". They are also keyed by arbitrary names and contain SearchHits,
     * which are already tested elsewhere.
     */
    public void testFromXContentLenientParsing() throws IOException {
        SearchHit searchHit = createTestItem(true);
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(searchHit, xContentType, true);
        Predicate<String> pathsToExclude = path -> (path.endsWith("highlight") || path.endsWith("fields") || path.contains("_source")
                || path.contains("inner_hits"));
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());

        SearchHit parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, true), xContentType);
    }

    /**
     * When e.g. with "stored_fields": "_none_", only "_index" and "_score" are returned.
     */
    public void testFromXContentWithoutTypeAndId() throws IOException {
        String hit = "{\"_index\": \"my_index\", \"_score\": 1}";
        SearchHit parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, hit)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals("my_index", parsed.getIndex());
        assertEquals(1, parsed.getScore(), Float.MIN_VALUE);
        assertNull(parsed.getType());
        assertNull(parsed.getId());
    }

    public void testToXContent() throws IOException {
        SearchHit searchHit = new SearchHit(1, "id1", new Text("type"), Collections.emptyMap());
        searchHit.score(1.5f);
        XContentBuilder builder = JsonXContent.contentBuilder();
        searchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":1.5}", builder.string());
    }

    public void testSerializeShardTarget() throws Exception {
        String clusterAlias = randomBoolean() ? null : "cluster_alias";
        SearchShardTarget target = new SearchShardTarget("_node_id", new Index("_index", "_na_"), 0, clusterAlias);

        Map<String, SearchHits> innerHits = new HashMap<>();
        SearchHit innerHit1 = new SearchHit(0, "_id", new Text("_type"), null);
        innerHit1.shard(target);
        SearchHit innerInnerHit2 = new SearchHit(0, "_id", new Text("_type"), null);
        innerInnerHit2.shard(target);
        innerHits.put("1", new SearchHits(new SearchHit[]{innerInnerHit2}, 1, 1f));
        innerHit1.setInnerHits(innerHits);
        SearchHit innerHit2 = new SearchHit(0, "_id", new Text("_type"), null);
        innerHit2.shard(target);
        SearchHit innerHit3 = new SearchHit(0, "_id", new Text("_type"), null);
        innerHit3.shard(target);

        innerHits = new HashMap<>();
        SearchHit hit1 = new SearchHit(0, "_id", new Text("_type"), null);
        innerHits.put("1", new SearchHits(new SearchHit[]{innerHit1, innerHit2}, 1, 1f));
        innerHits.put("2", new SearchHits(new SearchHit[]{innerHit3}, 1, 1f));
        hit1.shard(target);
        hit1.setInnerHits(innerHits);

        SearchHit hit2 = new SearchHit(0, "_id", new Text("_type"), null);
        hit2.shard(target);

        SearchHits hits = new SearchHits(new SearchHit[]{hit1, hit2}, 2, 1f);


        BytesStreamOutput output = new BytesStreamOutput();
        hits.writeTo(output);
        InputStream input = output.bytes().streamInput();
        SearchHits results = SearchHits.readSearchHits(new InputStreamStreamInput(input));
        assertThat(results.getAt(0).getShard(), equalTo(target));
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getInnerHits().get("1").getAt(0).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(1).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("2").getAt(0).getShard(), notNullValue());
        for (SearchHit hit : results) {
            assertEquals(clusterAlias, hit.getClusterAlias());
            if (hit.getInnerHits() != null) {
                for (SearchHits innerhits : hit.getInnerHits().values()) {
                    for (SearchHit innerHit : innerhits) {
                        assertEquals(clusterAlias, innerHit.getClusterAlias());
                    }
                }
            }
        }

        assertThat(results.getAt(1).getShard(), equalTo(target));
    }

    public void testNullSource() throws Exception {
        SearchHit searchHit = new SearchHit(0, "_id", new Text("_type"), null);

        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
    }

    public void testHasSource() {
        SearchHit searchHit = new SearchHit(randomInt());
        assertFalse(searchHit.hasSource());
        searchHit.sourceRef(new BytesArray("{}"));
        assertTrue(searchHit.hasSource());
    }

    private static Explanation createExplanation(int depth) {
        String description = randomAlphaOfLengthBetween(5, 20);
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
