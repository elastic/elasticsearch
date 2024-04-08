/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class MeanReciprocalRankTests extends ESTestCase {

    private static final int IRRELEVANT_RATING_0 = 0;
    private static final int RELEVANT_RATING_1 = 1;

    public void testParseFromXContent() throws IOException {
        String xContent = "{ }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            MeanReciprocalRank mrr = MeanReciprocalRank.fromXContent(parser);
            assertEquals(1, mrr.getRelevantRatingThreshold());
            assertEquals(10, mrr.getK());
        }

        xContent = "{ \"relevant_rating_threshold\": 2 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            MeanReciprocalRank mrr = MeanReciprocalRank.fromXContent(parser);
            assertEquals(2, mrr.getRelevantRatingThreshold());
            assertEquals(10, mrr.getK());
        }

        xContent = "{ \"relevant_rating_threshold\": 2, \"k\" : 15 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            MeanReciprocalRank mrr = MeanReciprocalRank.fromXContent(parser);
            assertEquals(2, mrr.getRelevantRatingThreshold());
            assertEquals(15, mrr.getK());
        }

        xContent = "{ \"k\" : 15 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            MeanReciprocalRank mrr = MeanReciprocalRank.fromXContent(parser);
            assertEquals(1, mrr.getRelevantRatingThreshold());
            assertEquals(15, mrr.getK());
        }
    }

    public void testMaxAcceptableRank() {
        MeanReciprocalRank reciprocalRank = new MeanReciprocalRank();
        int searchHits = randomIntBetween(1, 50);
        SearchHit[] hits = createSearchHits(0, searchHits, "test");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        int relevantAt = randomIntBetween(0, searchHits);
        for (int i = 0; i <= searchHits; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument("test", Integer.toString(i), RELEVANT_RATING_1));
            } else {
                ratedDocs.add(new RatedDocument("test", Integer.toString(i), IRRELEVANT_RATING_0));
            }
        }

        int rankAtFirstRelevant = relevantAt + 1;
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(1.0 / rankAtFirstRelevant, evaluation.metricScore(), Double.MIN_VALUE);
        assertEquals(rankAtFirstRelevant, ((MeanReciprocalRank.Detail) evaluation.getMetricDetails()).getFirstRelevantRank());

        // check that if we have fewer search hits than relevant doc position,
        // we don't find any result and get 0.0 score
        reciprocalRank = new MeanReciprocalRank();
        evaluation = reciprocalRank.evaluate("id", Arrays.copyOfRange(hits, 0, relevantAt), ratedDocs);
        assertEquals(0.0, evaluation.metricScore(), Double.MIN_VALUE);
    }

    public void testEvaluationOneRelevantInResults() {
        MeanReciprocalRank reciprocalRank = new MeanReciprocalRank();
        SearchHit[] hits = createSearchHits(0, 9, "test");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        // mark one of the ten docs relevant
        int relevantAt = randomIntBetween(0, 9);
        for (int i = 0; i <= 20; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument("test", Integer.toString(i), RELEVANT_RATING_1));
            } else {
                ratedDocs.add(new RatedDocument("test", Integer.toString(i), IRRELEVANT_RATING_0));
            }
        }

        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(1.0 / (relevantAt + 1), evaluation.metricScore(), Double.MIN_VALUE);
        assertEquals(relevantAt + 1, ((MeanReciprocalRank.Detail) evaluation.getMetricDetails()).getFirstRelevantRank());
    }

    /**
     * test that the relevant rating threshold can be set to something larger than
     * 1. e.g. we set it to 2 here and expect dics 0-2 to be not relevant, so first
     * relevant doc has third ranking position, so RR should be 1/3
     */
    public void testPrecisionAtFiveRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "0", 0));
        rated.add(new RatedDocument("test", "1", 1));
        rated.add(new RatedDocument("test", "2", 2));
        rated.add(new RatedDocument("test", "3", 3));
        rated.add(new RatedDocument("test", "4", 4));
        SearchHit[] hits = createSearchHits(0, 5, "test");

        MeanReciprocalRank reciprocalRank = new MeanReciprocalRank(2, 10);
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, rated);
        assertEquals((double) 1 / 3, evaluation.metricScore(), 0.00001);
        assertEquals(3, ((MeanReciprocalRank.Detail) evaluation.getMetricDetails()).getFirstRelevantRank());
    }

    public void testCombine() {
        MeanReciprocalRank reciprocalRank = new MeanReciprocalRank();
        List<EvalQueryQuality> partialResults = new ArrayList<>(3);
        partialResults.add(new EvalQueryQuality("id1", 0.5));
        partialResults.add(new EvalQueryQuality("id2", 1.0));
        partialResults.add(new EvalQueryQuality("id3", 0.75));
        assertEquals(0.75, reciprocalRank.combine(partialResults), Double.MIN_VALUE);
    }

    public void testEvaluationNoRelevantInResults() {
        MeanReciprocalRank reciprocalRank = new MeanReciprocalRank();
        SearchHit[] hits = createSearchHits(0, 9, "test");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(0.0, evaluation.metricScore(), Double.MIN_VALUE);
    }

    public void testNoResults() throws Exception {
        EvalQueryQuality evaluated = (new MeanReciprocalRank()).evaluate("id", SearchHits.EMPTY, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(-1, ((MeanReciprocalRank.Detail) evaluated.getMetricDetails()).getFirstRelevantRank());
    }

    public void testXContentRoundtrip() throws IOException {
        MeanReciprocalRank testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            MeanReciprocalRank parsedItem = MeanReciprocalRank.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        MeanReciprocalRank testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken();
            parser.nextToken();
            XContentParseException exception = expectThrows(XContentParseException.class, () -> MeanReciprocalRank.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[reciprocal_rank] unknown field"));
        }
    }

    /**
     * Create SearchHits for testing, starting from dociId 'from' up to docId 'to'.
     * The search hits index also need to be provided
     */
    private static SearchHit[] createSearchHits(int from, int to, String index) {
        SearchHit[] hits = new SearchHit[to + 1 - from];
        for (int i = from; i <= to; i++) {
            hits[i] = SearchHit.unpooled(i, i + "");
            hits[i].shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null));
        }
        return hits;
    }

    static MeanReciprocalRank createTestItem() {
        return new MeanReciprocalRank(randomIntBetween(0, 20), randomIntBetween(1, 20));
    }

    public void testSerialization() throws IOException {
        MeanReciprocalRank original = createTestItem();
        MeanReciprocalRank deserialized = ESTestCase.copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            MeanReciprocalRank::new
        );
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), MeanReciprocalRankTests::copy, MeanReciprocalRankTests::mutate);
    }

    private static MeanReciprocalRank copy(MeanReciprocalRank testItem) {
        return new MeanReciprocalRank(testItem.getRelevantRatingThreshold(), testItem.getK());
    }

    private static MeanReciprocalRank mutate(MeanReciprocalRank testItem) {
        if (randomBoolean()) {
            return new MeanReciprocalRank(testItem.getRelevantRatingThreshold() + 1, testItem.getK());
        } else {
            return new MeanReciprocalRank(testItem.getRelevantRatingThreshold(), testItem.getK() + 1);
        }
    }

    public void testInvalidRelevantThreshold() {
        expectThrows(IllegalArgumentException.class, () -> new MeanReciprocalRank(-1, 1));
    }

    public void testInvalidK() {
        expectThrows(IllegalArgumentException.class, () -> new MeanReciprocalRank(1, -1));
    }
}
