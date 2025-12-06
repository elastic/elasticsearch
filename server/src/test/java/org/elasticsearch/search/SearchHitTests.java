/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit.NestedIdentity;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightFieldTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchHitTests extends AbstractWireSerializingTestCase<SearchHit> {

    public static SearchHit createTestItem(boolean withOptionalInnerHits, boolean withShardTarget) {
        return createTestItem(randomFrom(XContentType.values()).canonical(), withOptionalInnerHits, withShardTarget);
    }

    public static SearchHit createTestItem(XContentType xContentType, boolean withOptionalInnerHits, boolean transportSerialization) {
        int internalId = randomInt();
        String uid = randomAlphaOfLength(10);
        NestedIdentity nestedIdentity = null;
        if (randomBoolean()) {
            nestedIdentity = NestedIdentityTests.createTestItem(randomIntBetween(0, 2));
        }
        Map<String, DocumentField> metaFields = new HashMap<>();
        Map<String, DocumentField> documentFields = new HashMap<>();
        if (frequently()) {
            if (randomBoolean()) {
                metaFields = GetResultTests.randomDocumentFields(xContentType, true).v2();
                documentFields = GetResultTests.randomDocumentFields(xContentType, false).v2();
            }
        }
        SearchHit hit = new SearchHit(internalId, uid, nestedIdentity);
        hit.addDocumentFields(documentFields, metaFields);
        if (frequently()) {
            if (rarely()) {
                hit.score(Float.NaN);
            } else if (rarely()) {
                hit.setRank(randomInt());
            } else {
                hit.score(randomFloat());
            }
        }
        if (frequently()) {
            hit.sourceRef(RandomObjects.randomSource(random(), xContentType));
        }
        if (randomBoolean()) {
            hit.version(randomLong());
        }

        if (randomBoolean()) {
            hit.version(randomNonNegativeLong());
            hit.version(randomLongBetween(1, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            hit.sortValues(SearchSortValuesTests.createTestItem(xContentType, transportSerialization));
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            Map<String, HighlightField> highlightFields = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                HighlightField testItem = HighlightFieldTests.createTestItem();
                highlightFields.put(testItem.name(), testItem);
            }
            hit.highlightFields(highlightFields);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            Map<String, Float> matchedQueries = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(randomAlphaOfLength(5), Float.NaN);
            }
            hit.matchedQueries(matchedQueries);
        }
        if (randomBoolean()) {
            hit.explanation(createExplanation(randomIntBetween(0, 5)));
        }
        if (withOptionalInnerHits) {
            int innerHitsSize = randomIntBetween(0, 3);
            if (innerHitsSize > 0) {
                Map<String, SearchHits> innerHits = Maps.newMapWithExpectedSize(innerHitsSize);
                for (int i = 0; i < innerHitsSize; i++) {
                    innerHits.put(randomAlphaOfLength(5), SearchHitsTests.createTestItem(xContentType, false, transportSerialization));
                }
                hit.setInnerHits(innerHits);
            }
        }
        if (transportSerialization && randomBoolean()) {
            String index = randomAlphaOfLengthBetween(5, 10);
            String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            hit.shard(
                new SearchShardTarget(
                    randomAlphaOfLengthBetween(5, 10),
                    new ShardId(new Index(index, randomAlphaOfLengthBetween(5, 10)), randomInt()),
                    clusterAlias
                )
            );
        }
        return hit;
    }

    @Override
    protected Writeable.Reader<SearchHit> instanceReader() {
        return in -> SearchHit.readFrom(in, randomBoolean());
    }

    @Override
    protected SearchHit createTestInstance() {
        return createTestItem(randomFrom(XContentType.values()).canonical(), randomBoolean(), randomBoolean());
    }

    @Override
    protected SearchHit mutateInstance(SearchHit instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values()).canonical();
        SearchHit searchHit = createTestItem(xContentType, true, false);
        try {
            boolean humanReadable = randomBoolean();
            BytesReference originalBytes = toShuffledXContent(searchHit, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
            SearchHit parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchResponseUtils.parseSearchHit(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
        } finally {
            searchHit.decRef();
        }
    }

    /**
     * This test adds randomized fields on all json objects and checks that we can parse it to
     * ensure the parsing is lenient for forward compatibility.
     * We need to exclude json objects with the "highlight" and "fields" field name since these
     * objects allow arbitrary keys (the field names that are queries). Also we want to exclude
     * to add anything under "_source" since it is not parsed, and avoid complexity by excluding
     * everything under "inner_hits". They are also keyed by arbitrary names and contain SearchHits,
     * which are already tested elsewhere. We also exclude the root level, as all unknown fields
     * on a root level are interpreted as meta-fields and will be kept.
     */
    public void testFromXContentLenientParsing() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        SearchHit searchHit = createTestItem(xContentType, true, true);
        try {
            BytesReference originalBytes = toXContent(searchHit, xContentType, true);
            Predicate<String> pathsToExclude = path -> path.endsWith("highlight")
                || path.contains("fields")
                || path.contains("_source")
                || path.contains("inner_hits")
                || path.isEmpty();
            BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());

            SearchHit parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchResponseUtils.parseSearchHit(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, true), xContentType);
        } finally {
            searchHit.decRef();
        }
    }

    /**
     * When e.g. with "stored_fields": "_none_", only "_index" and "_score" are returned.
     */
    public void testFromXContentWithoutTypeAndId() throws IOException {
        String hit = "{\"_index\": \"my_index\", \"_score\": 1}";
        SearchHit parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, hit)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchResponseUtils.parseSearchHit(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals("my_index", parsed.getIndex());
        assertEquals(1, parsed.getScore(), Float.MIN_VALUE);
        assertNull(parsed.getId());
    }

    public void testToXContent() throws IOException {
        SearchHit searchHit = new SearchHit(1, "id1");
        try {
            searchHit.score(1.5f);
            XContentBuilder builder = JsonXContent.contentBuilder();
            searchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("""
                {"_id":"id1","_score":1.5}""", Strings.toString(builder));
        } finally {
            searchHit.decRef();
        }
    }

    public void testRankToXContent() throws IOException {
        SearchHit searchHit = SearchHit.unpooled(1, "id1");
        searchHit.setRank(1);
        XContentBuilder builder = JsonXContent.contentBuilder();
        searchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"_id":"id1","_score":null,"_rank":1}""", Strings.toString(builder));
    }

    public void testSerializeShardTarget() throws Exception {
        String clusterAlias = randomBoolean() ? null : "cluster_alias";
        SearchShardTarget target = new SearchShardTarget("_node_id", new ShardId(new Index("_index", "_na_"), 0), clusterAlias);

        Map<String, SearchHits> innerHits = new HashMap<>();
        SearchHit innerHit1 = new SearchHit(0, "_id");
        innerHit1.shard(target);
        SearchHit innerInnerHit2 = new SearchHit(0, "_id");
        innerInnerHit2.shard(target);
        innerHits.put("1", new SearchHits(new SearchHit[] { innerInnerHit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        innerHit1.setInnerHits(innerHits);
        SearchHit innerHit2 = new SearchHit(0, "_id");
        innerHit2.shard(target);
        SearchHit innerHit3 = new SearchHit(0, "_id");
        innerHit3.shard(target);

        innerHits = new HashMap<>();
        SearchHit hit1 = new SearchHit(0, "_id");
        innerHits.put("1", new SearchHits(new SearchHit[] { innerHit1, innerHit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        innerHits.put("2", new SearchHits(new SearchHit[] { innerHit3 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        hit1.shard(target);
        hit1.setInnerHits(innerHits);

        SearchHit hit2 = new SearchHit(0, "_id");
        hit2.shard(target);

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1f);
        try {
            TransportVersion version = TransportVersionUtils.randomVersion(random());
            SearchHits results = copyWriteable(
                hits,
                getNamedWriteableRegistry(),
                (StreamInput in) -> SearchHits.readFrom(in, randomBoolean()),
                version
            );
            try {
                SearchShardTarget deserializedTarget = results.getAt(0).getShard();
                assertThat(deserializedTarget, equalTo(target));
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
            } finally {
                results.decRef();
            }
        } finally {
            hits.decRef();
        }
    }

    public void testNullSource() {
        SearchHit searchHit = SearchHit.unpooled(0, "_id");

        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
    }

    public void testHasSource() {
        SearchHit searchHit = SearchHit.unpooled(randomInt());
        assertFalse(searchHit.hasSource());
        searchHit.sourceRef(new BytesArray("{}"));
        assertTrue(searchHit.hasSource());
    }

    public void testWeirdScriptFields() throws Exception {
        {
            XContentParser parser = createParser(XContentType.JSON.xContent(), """
                {
                  "_index": "twitter",
                  "_id": "1",
                  "_score": 1.0,
                  "fields": {
                    "result": [null]
                  }
                }""");
            SearchHit searchHit = SearchResponseUtils.parseSearchHit(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            assertNull(result.getValues().get(0));
        }
        {
            XContentParser parser = createParser(XContentType.JSON.xContent(), """
                {
                  "_index": "twitter",
                  "_id": "1",
                  "_score": 1.0,
                  "fields": {
                    "result": [{}]
                  }
                }""");

            SearchHit searchHit = SearchResponseUtils.parseSearchHit(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            Object value = result.getValues().get(0);
            assertThat(value, instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) value;
            assertEquals(0, map.size());
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, """
                {
                  "_index": "twitter",
                  "_id": "1",
                  "_score": 1.0,
                  "fields": {
                    "result": [
                      []
                    ]
                  }
                }""");

            SearchHit searchHit = SearchResponseUtils.parseSearchHit(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            Object value = result.getValues().get(0);
            assertThat(value, instanceOf(List.class));
            List<?> list = (List<?>) value;
            assertEquals(0, list.size());
        }
    }

    public void testToXContentEmptyFields() throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", Collections.emptyList()));
        fields.put("bar", new DocumentField("bar", Collections.emptyList()));
        SearchHit hit = SearchHit.unpooled(0, "_id");
        hit.addDocumentFields(fields, Map.of());
        {
            BytesReference originalBytes = toShuffledXContent(hit, XContentType.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            // checks that the fields section is completely omitted in the rendering.
            assertThat(originalBytes.utf8ToString(), not(containsString("fields")));
            final SearchHit parsed;
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchResponseUtils.parseSearchHit(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            try {
                assertThat(parsed.getFields().size(), equalTo(0));
            } finally {
                parsed.decRef();
            }
        }

        fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", Collections.emptyList()));
        fields.put("bar", new DocumentField("bar", Collections.singletonList("value")));
        hit = SearchHit.unpooled(0, "_id");
        hit.addDocumentFields(fields, Collections.emptyMap());
        {
            BytesReference originalBytes = toShuffledXContent(hit, XContentType.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            final SearchHit parsed;
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchResponseUtils.parseSearchHit(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertThat(parsed.getFields().size(), equalTo(1));
            assertThat(parsed.getFields().get("bar").getValues(), equalTo(Collections.singletonList("value")));
        }

        Map<String, DocumentField> metadata = new HashMap<>();
        metadata.put("_routing", new DocumentField("_routing", Collections.emptyList()));
        hit = SearchHit.unpooled(0, "_id");
        hit.addDocumentFields(fields, Collections.emptyMap());
        {
            BytesReference originalBytes = toShuffledXContent(hit, XContentType.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            final SearchHit parsed;
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchResponseUtils.parseSearchHit(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertThat(parsed.getFields().size(), equalTo(1));
            assertThat(parsed.getFields().get("bar").getValues(), equalTo(Collections.singletonList("value")));
            assertNull(parsed.getFields().get("_routing"));
        }
    }

    @Override
    protected void dispose(SearchHit searchHit) {
        if (searchHit != null) {
            searchHit.decRef();
        }
    }

    static Explanation createExplanation(int depth) {
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
