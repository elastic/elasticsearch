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
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class RecallAtKTests extends ESTestCase {

    private static final int IRRELEVANT_RATING = 0;
    private static final int RELEVANT_RATING = 1;

    public void testCalculation() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));

        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals(1, evaluated.metricScore(), 0.00001);
        assertEquals(1, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(1, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testIgnoreOneResult() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "2", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "3", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "4", IRRELEVANT_RATING));

        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 4 / 4, evaluated.metricScore(), 0.00001);
        assertEquals(4, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(4, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    /**
     * Test that the relevant rating threshold can be set to something larger than
     * 1. e.g. we set it to 2 here and expect docs 0-1 to be not relevant, docs 2-4
     * to be relevant, and only 0-3 are hits.
     */
    public void testRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", 0)); // not relevant, hit
        rated.add(createRatedDoc("test", "1", 1)); // not relevant, hit
        rated.add(createRatedDoc("test", "2", 2)); // relevant, hit
        rated.add(createRatedDoc("test", "3", 3)); // relevant
        rated.add(createRatedDoc("test", "4", 4)); // relevant

        RecallAtK recallAtN = new RecallAtK(2, 5);

        EvalQueryQuality evaluated = recallAtN.evaluate("id", toSearchHits(rated.subList(0, 3), "test"), rated);
        assertEquals((double) 1 / 3, evaluated.metricScore(), 0.00001);
        assertEquals(1, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testCorrectIndex() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test_other", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test_other", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "2", IRRELEVANT_RATING));

        // the following search hits contain only the last three documents
        List<RatedDocument> ratedSubList = rated.subList(2, 5);

        EvalQueryQuality evaluated = (new RecallAtK(1, 5)).evaluate("id", toSearchHits(ratedSubList, "test"), rated);
        assertEquals((double) 2 / 4, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(4, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testNoRatedDocs() throws Exception {
        int k = 5;
        SearchHit[] hits = new SearchHit[k];
        for (int i = 0; i < k; i++) {
            hits[i] = new SearchHit(i, i + "");
            hits[i].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null));
        }

        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testNoResults() throws Exception {
        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", new SearchHit[0], Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testNoResultsWithRatedDocs() throws Exception {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));

        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", new SearchHit[0], rated);
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(1, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevant());
    }

    public void testParseFromXContent() throws IOException {
        String xContent = " {\n" + "   \"relevant_rating_threshold\" : 2" + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            RecallAtK recallAtK = RecallAtK.fromXContent(parser);
            assertEquals(2, recallAtK.getRelevantRatingThreshold());
        }
    }

    public void testCombine() {
        RecallAtK metric = new RecallAtK();
        List<EvalQueryQuality> partialResults = new ArrayList<>(3);
        partialResults.add(new EvalQueryQuality("a", 0.1));
        partialResults.add(new EvalQueryQuality("b", 0.2));
        partialResults.add(new EvalQueryQuality("c", 0.6));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public void testInvalidRelevantThreshold() {
        expectThrows(IllegalArgumentException.class, () -> new RecallAtK(-1, 10));
    }

    public void testInvalidK() {
        expectThrows(IllegalArgumentException.class, () -> new RecallAtK(1, -10));
    }

    public static RecallAtK createTestItem() {
        return new RecallAtK(randomIntBetween(0, 10), randomIntBetween(1, 50));
    }

    public void testXContentRoundtrip() throws IOException {
        RecallAtK testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            RecallAtK parsedItem = RecallAtK.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        RecallAtK testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken();
            parser.nextToken();
            XContentParseException exception = expectThrows(XContentParseException.class, () -> RecallAtK.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[recall] unknown field"));
        }
    }

    public void testSerialization() throws IOException {
        RecallAtK original = createTestItem();
        RecallAtK deserialized = ESTestCase.copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), RecallAtK::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), RecallAtKTests::copy, RecallAtKTests::mutate);
    }

    private static RecallAtK copy(RecallAtK original) {
        return new RecallAtK(original.getRelevantRatingThreshold(), original.forcedSearchSize().getAsInt());
    }

    private static RecallAtK mutate(RecallAtK original) {
        RecallAtK recallAtK = switch (randomIntBetween(0, 1)) {
            case 0 -> new RecallAtK(
                randomValueOtherThan(original.getRelevantRatingThreshold(), () -> randomIntBetween(0, 10)),
                original.forcedSearchSize().getAsInt()
            );
            case 1 -> new RecallAtK(original.getRelevantRatingThreshold(), original.forcedSearchSize().getAsInt() + 1);
            default -> throw new IllegalStateException("The test should only allow two parameters mutated");
        };
        return recallAtK;
    }

    private static SearchHit[] toSearchHits(List<RatedDocument> rated, String index) {
        SearchHit[] hits = new SearchHit[rated.size()];
        for (int i = 0; i < rated.size(); i++) {
            hits[i] = new SearchHit(i, i + "");
            hits[i].shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null));
        }
        return hits;
    }

    private static RatedDocument createRatedDoc(String index, String id, int rating) {
        return new RatedDocument(index, id, rating);
    }
}
