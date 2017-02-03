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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.rankeval.PrecisionTests.Rating;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class ReciprocalRankTests extends ESTestCase {

    public void testMaxAcceptableRank() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();

        int searchHits = randomIntBetween(1, 50);

        SearchHit[] hits = createSearchHits(0, searchHits, "test", "type");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        int relevantAt = randomIntBetween(0, searchHits);
        for (int i = 0; i <= searchHits; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument("test", "type", Integer.toString(i), Rating.RELEVANT.ordinal()));
            } else {
                ratedDocs.add(new RatedDocument("test", "type", Integer.toString(i), Rating.IRRELEVANT.ordinal()));
            }
        }

        int rankAtFirstRelevant = relevantAt + 1;
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(1.0 / rankAtFirstRelevant, evaluation.getQualityLevel(), Double.MIN_VALUE);
        assertEquals(rankAtFirstRelevant, ((ReciprocalRank.Breakdown) evaluation.getMetricDetails()).getFirstRelevantRank());

        // check that if we have fewer search hits than relevant doc position, we don't find any result and get 0.0 quality level
        reciprocalRank = new ReciprocalRank();
        evaluation = reciprocalRank.evaluate("id", Arrays.copyOfRange(hits, 0, relevantAt), ratedDocs);
        assertEquals(0.0, evaluation.getQualityLevel(), Double.MIN_VALUE);
    }

    public void testEvaluationOneRelevantInResults() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        SearchHit[] hits = createSearchHits(0, 9, "test", "type");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        // mark one of the ten docs relevant
        int relevantAt = randomIntBetween(0, 9);
        for (int i = 0; i <= 20; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument("test", "type", Integer.toString(i), Rating.RELEVANT.ordinal()));
            } else {
                ratedDocs.add(new RatedDocument("test", "type", Integer.toString(i), Rating.IRRELEVANT.ordinal()));
            }
        }

        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(1.0 / (relevantAt + 1), evaluation.getQualityLevel(), Double.MIN_VALUE);
        assertEquals(relevantAt + 1, ((ReciprocalRank.Breakdown) evaluation.getMetricDetails()).getFirstRelevantRank());
    }

    /**
     * test that the relevant rating threshold can be set to something larger than 1.
     * e.g. we set it to 2 here and expect dics 0-2 to be not relevant, so first relevant doc has
     * third ranking position, so RR should be 1/3
     */
    public void testPrecisionAtFiveRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", 0));
        rated.add(new RatedDocument("test", "testtype", "1", 1));
        rated.add(new RatedDocument("test", "testtype", "2", 2));
        rated.add(new RatedDocument("test", "testtype", "3", 3));
        rated.add(new RatedDocument("test", "testtype", "4", 4));
        SearchHit[] hits = createSearchHits(0, 5, "test", "testtype");

        ReciprocalRank reciprocalRank = new ReciprocalRank();
        reciprocalRank.setRelevantRatingThreshhold(2);
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, rated);
        assertEquals((double) 1 / 3, evaluation.getQualityLevel(), 0.00001);
        assertEquals(3, ((ReciprocalRank.Breakdown) evaluation.getMetricDetails()).getFirstRelevantRank());
    }

    public void testCombine() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        Vector<EvalQueryQuality> partialResults = new Vector<>(3);
        partialResults.add(new EvalQueryQuality("id1", 0.5));
        partialResults.add(new EvalQueryQuality("id2", 1.0));
        partialResults.add(new EvalQueryQuality("id3", 0.75));
        assertEquals(0.75, reciprocalRank.combine(partialResults), Double.MIN_VALUE);
    }

    public void testEvaluationNoRelevantInResults() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        SearchHit[] hits = createSearchHits(0, 9, "test", "type");
        List<RatedDocument> ratedDocs = new ArrayList<>();
        EvalQueryQuality evaluation = reciprocalRank.evaluate("id", hits, ratedDocs);
        assertEquals(0.0, evaluation.getQualityLevel(), Double.MIN_VALUE);
    }

    public void testXContentRoundtrip() throws IOException {
        ReciprocalRank testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            ReciprocalRank parsedItem = ReciprocalRank.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    /**
     * Create InternalSearchHits for testing, starting from dociId 'from' up to docId 'to'.
     * The search hits index and type also need to be provided
     */
    private static SearchHit[] createSearchHits(int from, int to, String index, String type) {
        InternalSearchHit[] hits = new InternalSearchHit[to + 1 - from];
        for (int i = from; i <= to; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text(type), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index(index, "uuid"), 0));
        }
        return hits;
    }

    private ReciprocalRank createTestItem() {
        ReciprocalRank testItem = new ReciprocalRank();
        testItem.setRelevantRatingThreshhold(randomIntBetween(0, 20));
        return testItem;
    }

    public void testSerialization() throws IOException {
        ReciprocalRank original = createTestItem();

        ReciprocalRank deserialized = RankEvalTestHelper.copy(original, ReciprocalRank::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        ReciprocalRank testItem = createTestItem();
        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(testItem),
                RankEvalTestHelper.copy(testItem, ReciprocalRank::new));
    }

    private ReciprocalRank mutateTestItem(ReciprocalRank testItem) {
        int relevantThreshold = testItem.getRelevantRatingThreshold();
        ReciprocalRank rank = new ReciprocalRank();
        rank.setRelevantRatingThreshhold(randomValueOtherThan(relevantThreshold, () -> randomIntBetween(0, 10)));
        return rank;
    }

    public void testInvalidRelevantThreshold() {
        ReciprocalRank prez = new ReciprocalRank();
        expectThrows(IllegalArgumentException.class, () -> prez.setRelevantRatingThreshhold(-1));
    }
}
