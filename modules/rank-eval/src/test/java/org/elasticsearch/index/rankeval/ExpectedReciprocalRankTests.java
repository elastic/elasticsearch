/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class ExpectedReciprocalRankTests extends ESTestCase {

    private static final double DELTA = 10E-14;

    public void testProbabilityOfRelevance() {
        ExpectedReciprocalRank err = new ExpectedReciprocalRank(5);
        assertEquals(0.0, err.probabilityOfRelevance(0), 0.0);
        assertEquals(1d/32d, err.probabilityOfRelevance(1), 0.0);
        assertEquals(3d/32d, err.probabilityOfRelevance(2), 0.0);
        assertEquals(7d/32d, err.probabilityOfRelevance(3), 0.0);
        assertEquals(15d/32d, err.probabilityOfRelevance(4), 0.0);
        assertEquals(31d/32d, err.probabilityOfRelevance(5), 0.0);
    }

    /**
     * Assuming the result ranking is
     *
     * <pre>{@code
     * rank | relevance | probR / r | p        | p * probR / r
     * -------------------------------------------------------
     * 1    | 3         | 0.875     | 1        | 0.875       |
     * 2    | 2         | 0.1875    | 0.125    | 0.0234375   |
     * 3    | 0         | 0         | 0.078125 | 0           |
     * 4    | 1         | 0.03125   | 0.078125 | 0.00244140625 |
     * }</pre>
     *
     * err = sum of last column
     */
    public void testERRAt() {
        List<RatedDocument> rated = new ArrayList<>();
        Integer[] relevanceRatings = new Integer[] { 3, 2, 0, 1};
        SearchHit[] hits = createSearchHits(rated, relevanceRatings);
        ExpectedReciprocalRank err = new ExpectedReciprocalRank(3, 0, 3);
        assertEquals(0.8984375, err.evaluate("id", hits, rated).metricScore(), DELTA);
        // take 4th rank into window
        err = new ExpectedReciprocalRank(3, 0, 4);
        assertEquals(0.8984375 + 0.00244140625, err.evaluate("id", hits, rated).metricScore(), DELTA);
    }

    /**
     * Assuming the result ranking is
     *
     * <pre>{@code
     * rank | relevance | probR / r | p        | p * probR / r
     * -------------------------------------------------------
     * 1    | 3         | 0.875     | 1        | 0.875       |
     * 2    | n/a       | n/a       | 0.125    | n/a   |
     * 3    | 0         | 0         | 0.125    | 0           |
     * 4    | 1         | 0.03125   | 0.125    | 0.00390625 |
     * }</pre>
     *
     * err = sum of last column
     */
    public void testERRMissingRatings() {
        List<RatedDocument> rated = new ArrayList<>();
        Integer[] relevanceRatings = new Integer[] { 3, null, 0, 1};
        SearchHit[] hits = createSearchHits(rated, relevanceRatings);
        ExpectedReciprocalRank err = new ExpectedReciprocalRank(3, null, 4);
        EvalQueryQuality evaluation = err.evaluate("id", hits, rated);
        assertEquals(0.875 + 0.00390625, evaluation.metricScore(), DELTA);
        assertEquals(1, ((ExpectedReciprocalRank.Detail) evaluation.getMetricDetails()).getUnratedDocs());
        // if we supply e.g. 2 as unknown docs rating, it should be the same as in the other test above
        err = new ExpectedReciprocalRank(3, 2, 4);
        assertEquals(0.8984375 + 0.00244140625, err.evaluate("id", hits, rated).metricScore(), DELTA);
    }

    private SearchHit[] createSearchHits(List<RatedDocument> rated, Integer[] relevanceRatings) {
        SearchHit[] hits = new SearchHit[relevanceRatings.length];
        for (int i = 0; i < relevanceRatings.length; i++) {
            if (relevanceRatings[i] != null) {
                rated.add(new RatedDocument("index", Integer.toString(i), relevanceRatings[i]));
            }
            hits[i] = new SearchHit(i, Integer.toString(i), Collections.emptyMap(), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE));
        }
        return hits;
    }

    /**
     * test that metric returns 0.0 when there are no search results
     */
    public void testNoResults() throws Exception {
        ExpectedReciprocalRank err = new ExpectedReciprocalRank(5, 0, 10);
        assertEquals(0.0, err.evaluate("id", new SearchHit[0], Collections.emptyList()).metricScore(), DELTA);
    }

    public void testParseFromXContent() throws IOException {
        assertParsedCorrect("{ \"unknown_doc_rating\": 2, \"maximum_relevance\": 5, \"k\" : 15 }", 2, 5, 15);
        assertParsedCorrect("{ \"unknown_doc_rating\": 2, \"maximum_relevance\": 4 }", 2, 4, 10);
        assertParsedCorrect("{ \"maximum_relevance\": 4, \"k\": 23 }", null, 4, 23);
    }

    private void assertParsedCorrect(String xContent, Integer expectedUnknownDocRating, int expectedMaxRelevance, int expectedK)
            throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            ExpectedReciprocalRank errAt = ExpectedReciprocalRank.fromXContent(parser);
            assertEquals(expectedUnknownDocRating, errAt.getUnknownDocRating());
            assertEquals(expectedK, errAt.getK());
            assertEquals(expectedMaxRelevance, errAt.getMaxRelevance());
        }
    }

    public static ExpectedReciprocalRank createTestItem() {
        Integer unknownDocRating = frequently() ? Integer.valueOf(randomIntBetween(0, 10)) : null;
        int maxRelevance = randomIntBetween(1, 10);
        return new ExpectedReciprocalRank(maxRelevance, unknownDocRating, randomIntBetween(1, 10));
    }

    public void testXContentRoundtrip() throws IOException {
        ExpectedReciprocalRank testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            ExpectedReciprocalRank parsedItem = ExpectedReciprocalRank.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        ExpectedReciprocalRank testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken();
            parser.nextToken();
            XContentParseException exception = expectThrows(XContentParseException.class,
                    () -> DiscountedCumulativeGain.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[dcg] unknown field"));
        }
    }

    public void testMetricDetails() {
        int unratedDocs = randomIntBetween(0, 100);
        ExpectedReciprocalRank.Detail detail = new ExpectedReciprocalRank.Detail(unratedDocs);
        assertEquals(unratedDocs, detail.getUnratedDocs());
    }

    public void testSerialization() throws IOException {
        ExpectedReciprocalRank original = createTestItem();
        ExpectedReciprocalRank deserialized = ESTestCase.copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()),
                ExpectedReciprocalRank::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), original -> {
            return new ExpectedReciprocalRank(original.getMaxRelevance(), original.getUnknownDocRating(), original.getK());
        }, ExpectedReciprocalRankTests::mutateTestItem);
    }

    private static ExpectedReciprocalRank mutateTestItem(ExpectedReciprocalRank original) {
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new ExpectedReciprocalRank(original.getMaxRelevance() + 1, original.getUnknownDocRating(), original.getK());
        case 1:
            return new ExpectedReciprocalRank(original.getMaxRelevance(),
                    randomValueOtherThan(original.getUnknownDocRating(), () -> randomIntBetween(0, 10)), original.getK());
        case 2:
            return new ExpectedReciprocalRank(original.getMaxRelevance(), original.getUnknownDocRating(),
                    randomValueOtherThan(original.getK(), () -> randomIntBetween(1, 10)));
        default:
            throw new IllegalArgumentException("mutation variant not allowed");
        }
    }
}
