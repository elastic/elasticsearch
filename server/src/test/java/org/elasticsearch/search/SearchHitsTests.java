/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.LuceneTests;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.function.Predicate;

public class SearchHitsTests extends AbstractChunkedSerializingTestCase<SearchHits> {

    public static SearchHits createTestItem(boolean withOptionalInnerHits, boolean withShardTarget) {
        return createTestItem(randomFrom(XContentType.values()).canonical(), withOptionalInnerHits, withShardTarget);
    }

    private static SearchHit[] createSearchHitArray(
        int size,
        XContentType xContentType,
        boolean withOptionalInnerHits,
        boolean transportSerialization
    ) {
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = SearchHitTests.createTestItem(xContentType, withOptionalInnerHits, transportSerialization);
        }
        return hits;
    }

    private static TotalHits randomTotalHits(TotalHits.Relation relation) {
        long totalHits = TestUtil.nextLong(random(), 0, Long.MAX_VALUE);
        return new TotalHits(totalHits, relation);
    }

    public static SearchHits createTestItem(XContentType xContentType, boolean withOptionalInnerHits, boolean transportSerialization) {
        return createTestItem(xContentType, withOptionalInnerHits, transportSerialization, randomFrom(TotalHits.Relation.values()));
    }

    private static SearchHits createTestItem(
        XContentType xContentType,
        boolean withOptionalInnerHits,
        boolean transportSerialization,
        TotalHits.Relation totalHitsRelation
    ) {
        int searchHits = randomIntBetween(0, 5);
        SearchHit[] hits = createSearchHitArray(searchHits, xContentType, withOptionalInnerHits, transportSerialization);
        TotalHits totalHits = frequently() ? randomTotalHits(totalHitsRelation) : null;
        float maxScore = frequently() ? randomFloat() : Float.NaN;
        SortField[] sortFields = null;
        String collapseField = null;
        Object[] collapseValues = null;
        if (transportSerialization) {
            sortFields = randomBoolean() ? createSortFields(randomIntBetween(1, 5)) : null;
            collapseField = randomAlphaOfLengthBetween(5, 10);
            collapseValues = randomBoolean() ? createCollapseValues(randomIntBetween(1, 10)) : null;
        }
        return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
    }

    private static SortField[] createSortFields(int size) {
        SortField[] sortFields = new SortField[size];
        for (int i = 0; i < sortFields.length; i++) {
            // sort fields are simplified before serialization, we write directly the simplified version
            // otherwise equality comparisons become complicated
            sortFields[i] = LuceneTests.randomSortField().v2();
        }
        return sortFields;
    }

    private static Object[] createCollapseValues(int size) {
        Object[] collapseValues = new Object[size];
        for (int i = 0; i < collapseValues.length; i++) {
            collapseValues[i] = LuceneTests.randomSortValue();
        }
        return collapseValues;
    }

    @Override
    protected SearchHits mutateInstance(SearchHits instance) {
        switch (randomIntBetween(0, 5)) {
            case 0:
                return new SearchHits(
                    createSearchHitArray(
                        instance.getHits().length + 1,
                        randomFrom(XContentType.values()).canonical(),
                        false,
                        randomBoolean()
                    ),
                    instance.getTotalHits(),
                    instance.getMaxScore()
                );
            case 1:
                final TotalHits totalHits;
                if (instance.getTotalHits() == null) {
                    totalHits = randomTotalHits(randomFrom(TotalHits.Relation.values()));
                } else {
                    totalHits = null;
                }
                return new SearchHits(instance.getHits(), totalHits, instance.getMaxScore());
            case 2:
                final float maxScore;
                if (Float.isNaN(instance.getMaxScore())) {
                    maxScore = randomFloat();
                } else {
                    maxScore = Float.NaN;
                }
                return new SearchHits(instance.getHits(), instance.getTotalHits(), maxScore);
            case 3:
                SortField[] sortFields;
                if (instance.getSortFields() == null) {
                    sortFields = createSortFields(randomIntBetween(1, 5));
                } else {
                    sortFields = randomBoolean() ? createSortFields(instance.getSortFields().length + 1) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    sortFields,
                    instance.getCollapseField(),
                    instance.getCollapseValues()
                );
            case 4:
                String collapseField;
                if (instance.getCollapseField() == null) {
                    collapseField = randomAlphaOfLengthBetween(5, 10);
                } else {
                    collapseField = randomBoolean() ? instance.getCollapseField() + randomAlphaOfLengthBetween(2, 5) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    instance.getSortFields(),
                    collapseField,
                    instance.getCollapseValues()
                );
            case 5:
                Object[] collapseValues;
                if (instance.getCollapseValues() == null) {
                    collapseValues = createCollapseValues(randomIntBetween(1, 5));
                } else {
                    collapseValues = randomBoolean() ? createCollapseValues(instance.getCollapseValues().length + 1) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    instance.getSortFields(),
                    instance.getCollapseField(),
                    collapseValues
                );
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return path -> (path.isEmpty()
            || path.contains("inner_hits")
            || path.contains("highlight")
            || path.contains("fields")
            || path.contains("_source"));
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "_source" };
    }

    @Override
    protected Writeable.Reader<SearchHits> instanceReader() {
        return SearchHits::new;
    }

    @Override
    protected SearchHits createTestInstance() {
        // This instance is used to test the transport serialization so it's fine
        // to produce shard targets (withShardTarget is true) since they are serialized
        // in this layer.
        return createTestItem(randomFrom(XContentType.values()).canonical(), true, true);
    }

    @Override
    protected SearchHits createXContextTestInstance(XContentType xContentType) {
        // We don't set SearchHit#shard (withShardTarget is false) in this test
        // because the rest serialization does not render this information so the
        // deserialized hit cannot be equal to the original instance.
        // There is another test (#testFromXContentWithShards) that checks the
        // rest serialization with shard targets.
        return createTestItem(xContentType, true, false);
    }

    @Override
    protected SearchHits doParseInstance(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(SearchHits.Fields.HITS, parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        SearchHits searchHits = SearchHits.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return searchHits;
    }

    public void testToXContent() throws IOException {
        SearchHit[] hits = new SearchHit[] { new SearchHit(1, "id1"), new SearchHit(2, "id2") };

        long totalHits = 1000;
        float maxScore = 1.5f;
        SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(searchHits).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "hits": {
                "total": {
                  "value": 1000,
                  "relation": "eq"
                },
                "max_score": 1.5,
                "hits": [ { "_id": "id1", "_score": null }, { "_id": "id2", "_score": null } ]
              }
            }"""), Strings.toString(builder));
    }

    public void testFromXContentWithShards() throws IOException {
        for (boolean withExplanation : new boolean[] { true, false }) {
            final SearchHit[] hits = new SearchHit[] { new SearchHit(1, "id1"), new SearchHit(2, "id2"), new SearchHit(10, "id10") };

            for (SearchHit hit : hits) {
                String index = randomAlphaOfLengthBetween(5, 10);
                String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
                final SearchShardTarget shardTarget = new SearchShardTarget(
                    randomAlphaOfLengthBetween(5, 10),
                    new ShardId(new Index(index, randomAlphaOfLengthBetween(5, 10)), randomInt()),
                    clusterAlias
                );
                if (withExplanation) {
                    hit.explanation(SearchHitTests.createExplanation(randomIntBetween(0, 5)));
                }
                hit.shard(shardTarget);
            }

            long totalHits = 1000;
            float maxScore = 1.5f;
            SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);
            XContentType xContentType = randomFrom(XContentType.values()).canonical();
            BytesReference bytes = toShuffledXContent(
                ChunkedToXContent.wrapAsToXContent(searchHits),
                xContentType,
                ToXContent.EMPTY_PARAMS,
                false
            );
            try (
                XContentParser parser = xContentType.xContent()
                    .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, bytes.streamInput())
            ) {
                SearchHits newSearchHits = doParseInstance(parser);
                assertEquals(3, newSearchHits.getHits().length);
                assertEquals("id1", newSearchHits.getAt(0).getId());
                for (int i = 0; i < hits.length; i++) {
                    assertEquals(hits[i].getExplanation(), newSearchHits.getAt(i).getExplanation());
                    if (withExplanation) {
                        assertEquals(hits[i].getShard().getIndex(), newSearchHits.getAt(i).getShard().getIndex());
                        assertEquals(hits[i].getShard().getShardId().getId(), newSearchHits.getAt(i).getShard().getShardId().getId());
                        assertEquals(
                            hits[i].getShard().getShardId().getIndexName(),
                            newSearchHits.getAt(i).getShard().getShardId().getIndexName()
                        );
                        assertEquals(hits[i].getShard().getNodeId(), newSearchHits.getAt(i).getShard().getNodeId());
                        // The index uuid is not serialized in the rest layer
                        assertNotEquals(
                            hits[i].getShard().getShardId().getIndex().getUUID(),
                            newSearchHits.getAt(i).getShard().getShardId().getIndex().getUUID()
                        );
                    } else {
                        assertNull(newSearchHits.getAt(i).getShard());
                    }
                }
            }

        }
    }
}
