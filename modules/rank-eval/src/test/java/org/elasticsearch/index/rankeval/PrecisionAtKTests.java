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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class PrecisionAtKTests extends ESTestCase {

    private static final int IRRELEVANT_RATING = 0;
    private static final int RELEVANT_RATING = 1;

    public void testCalculation() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals(1, evaluated.metricScore(), 0.00001);
        assertEquals(1, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(1, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testIgnoreOneResult() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "2", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "3", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "4", IRRELEVANT_RATING));
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 4 / 5, evaluated.metricScore(), 0.00001);
        assertEquals(4, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    /**
     * test that the relevant rating threshold can be set to something larger than
     * 1. e.g. we set it to 2 here and expect docs 0-1 to be not relevant, docs 2-4
     * to be relevant
     */
    public void testRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", 0));
        rated.add(createRatedDoc("test", "1", 1));
        rated.add(createRatedDoc("test", "2", 2));
        rated.add(createRatedDoc("test", "3", 3));
        rated.add(createRatedDoc("test", "4", 4));
        PrecisionAtK precisionAtN = new PrecisionAtK(2, false, 5);
        EvalQueryQuality evaluated = precisionAtN.evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 3 / 5, evaluated.metricScore(), 0.00001);
        assertEquals(3, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveCorrectIndex() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test_other", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test_other", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "2", IRRELEVANT_RATING));
        // the following search hits contain only the last three documents
        List<RatedDocument> ratedSubList = rated.subList(2, 5);
        PrecisionAtK precisionAtK = new PrecisionAtK(1, false, 5);
        EvalQueryQuality evaluated = (precisionAtK).evaluate("id", toSearchHits(ratedSubList, "test"), rated);
        assertEquals((double) 2 / 3, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testIgnoreUnlabeled() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING));
        // add an unlabeled search hit
        SearchHit[] searchHits = Arrays.copyOf(toSearchHits(rated, "test"), 3);
        searchHits[2] = new SearchHit(2, "2");
        searchHits[2].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null));

        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 3, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        PrecisionAtK prec = new PrecisionAtK(true);
        evaluated = prec.evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 2, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(2, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testNoRatedDocs() throws Exception {
        SearchHit[] hits = new SearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new SearchHit(i, i + "");
            hits[i].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null));
        }
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        PrecisionAtK prec = new PrecisionAtK(true);
        evaluated = prec.evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testNoResults() throws Exception {
        SearchHit[] hits = new SearchHit[0];
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((PrecisionAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testParseFromXContent() throws IOException {
        String xContent = " {\n" + "   \"relevant_rating_threshold\" : 2" + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            PrecisionAtK precicionAt = PrecisionAtK.fromXContent(parser);
            assertEquals(2, precicionAt.getRelevantRatingThreshold());
        }
    }

    public void testCombine() {
        PrecisionAtK metric = new PrecisionAtK();
        List<EvalQueryQuality> partialResults = new ArrayList<>(3);
        partialResults.add(new EvalQueryQuality("a", 0.1));
        partialResults.add(new EvalQueryQuality("b", 0.2));
        partialResults.add(new EvalQueryQuality("c", 0.6));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public void testInvalidRelevantThreshold() {
        expectThrows(IllegalArgumentException.class, () -> new PrecisionAtK(-1, false, 10));
    }

    public void testInvalidK() {
        expectThrows(IllegalArgumentException.class, () -> new PrecisionAtK(1, false, -10));
    }

    public static PrecisionAtK createTestItem() {
        return new PrecisionAtK(randomIntBetween(0, 10), randomBoolean(), randomIntBetween(1, 50));
    }

    public void testXContentRoundtrip() throws IOException {
        PrecisionAtK testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            PrecisionAtK parsedItem = PrecisionAtK.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        PrecisionAtK testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken();
            parser.nextToken();
            XContentParseException exception = expectThrows(XContentParseException.class, () -> PrecisionAtK.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[precision] unknown field"));
        }
    }

    public void testSerialization() throws IOException {
        PrecisionAtK original = createTestItem();
        PrecisionAtK deserialized = ESTestCase.copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            PrecisionAtK::new
        );
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), PrecisionAtKTests::copy, PrecisionAtKTests::mutate);
    }

    private static PrecisionAtK copy(PrecisionAtK original) {
        return new PrecisionAtK(
            original.getRelevantRatingThreshold(),
            original.getIgnoreUnlabeled(),
            original.forcedSearchSize().getAsInt()
        );
    }

    private static PrecisionAtK mutate(PrecisionAtK original) {
        PrecisionAtK pAtK = switch (randomIntBetween(0, 2)) {
            case 0 -> new PrecisionAtK(
                original.getRelevantRatingThreshold(),
                original.getIgnoreUnlabeled() == false,
                original.forcedSearchSize().getAsInt()
            );
            case 1 -> new PrecisionAtK(
                randomValueOtherThan(original.getRelevantRatingThreshold(), () -> randomIntBetween(0, 10)),
                original.getIgnoreUnlabeled(),
                original.forcedSearchSize().getAsInt()
            );
            case 2 -> new PrecisionAtK(
                original.getRelevantRatingThreshold(),
                original.getIgnoreUnlabeled(),
                original.forcedSearchSize().getAsInt() + 1
            );
            default -> throw new IllegalStateException("The test should only allow three parameters mutated");
        };
        return pAtK;
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
