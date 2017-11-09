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
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class PrecisionTests extends ESTestCase {

    public void testPrecisionAtFiveCalculation() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "0", Rating.RELEVANT.ordinal()));
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals(1, evaluated.getQualityLevel(), 0.00001);
        assertEquals(1, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(1, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveIgnoreOneResult() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "2", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "3", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "4", Rating.IRRELEVANT.ordinal()));
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 4 / 5, evaluated.getQualityLevel(), 0.00001);
        assertEquals(4, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    /**
     * test that the relevant rating threshold can be set to something larger than
     * 1. e.g. we set it to 2 here and expect dics 0-2 to be not relevant, doc 3 and
     * 4 to be relevant
     */
    public void testPrecisionAtFiveRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "0", 0));
        rated.add(new RatedDocument("test", "1", 1));
        rated.add(new RatedDocument("test", "2", 2));
        rated.add(new RatedDocument("test", "3", 3));
        rated.add(new RatedDocument("test", "4", 4));
        PrecisionAtK precisionAtN = new PrecisionAtK();
        precisionAtN.setRelevantRatingThreshhold(2);
        EvalQueryQuality evaluated = precisionAtN.evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 3 / 5, evaluated.getQualityLevel(), 0.00001);
        assertEquals(3, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveCorrectIndex() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test_other", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test_other", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "2", Rating.IRRELEVANT.ordinal()));
        // the following search hits contain only the last three documents
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", toSearchHits(rated.subList(2, 5), "test"), rated);
        assertEquals((double) 2 / 3, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testIgnoreUnlabeled() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "1", Rating.RELEVANT.ordinal()));
        // add an unlabeled search hit
        SearchHit[] searchHits = Arrays.copyOf(toSearchHits(rated, "test"), 3);
        searchHits[2] = new SearchHit(2, "2", new Text("testtype"), Collections.emptyMap());
        searchHits[2].shard(new SearchShardTarget("testnode", new Index("index", "uuid"), 0, null));

        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 3, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        PrecisionAtK prec = new PrecisionAtK();
        prec.setIgnoreUnlabeled(true);
        evaluated = prec.evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 2, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(2, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testNoRatedDocs() throws Exception {
        SearchHit[] hits = new SearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new SearchHit(i, i + "", new Text("type"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("index", "uuid"), 0, null));
        }
        EvalQueryQuality evaluated = (new PrecisionAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.getQualityLevel(), 0.00001);
        assertEquals(0, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        PrecisionAtK prec = new PrecisionAtK();
        prec.setIgnoreUnlabeled(true);
        evaluated = prec.evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.getQualityLevel(), 0.00001);
        assertEquals(0, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((PrecisionAtK.Breakdown) evaluated.getMetricDetails()).getRetrieved());
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
        Vector<EvalQueryQuality> partialResults = new Vector<>(3);
        partialResults.add(new EvalQueryQuality("a", 0.1));
        partialResults.add(new EvalQueryQuality("b", 0.2));
        partialResults.add(new EvalQueryQuality("c", 0.6));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public void testInvalidRelevantThreshold() {
        PrecisionAtK prez = new PrecisionAtK();
        expectThrows(IllegalArgumentException.class, () -> prez.setRelevantRatingThreshhold(-1));
    }

    public static PrecisionAtK createTestItem() {
        PrecisionAtK precision = new PrecisionAtK();
        if (randomBoolean()) {
            precision.setRelevantRatingThreshhold(randomIntBetween(0, 10));
        }
        precision.setIgnoreUnlabeled(randomBoolean());
        return precision;
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

    public void testSerialization() throws IOException {
        PrecisionAtK original = createTestItem();
        PrecisionAtK deserialized = RankEvalTestHelper.copy(original, PrecisionAtK::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        PrecisionAtK testItem = createTestItem();
        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(testItem), RankEvalTestHelper.copy(testItem, PrecisionAtK::new));
    }

    private static PrecisionAtK mutateTestItem(PrecisionAtK original) {
        boolean ignoreUnlabeled = original.getIgnoreUnlabeled();
        int relevantThreshold = original.getRelevantRatingThreshold();
        PrecisionAtK precision = new PrecisionAtK();
        precision.setIgnoreUnlabeled(ignoreUnlabeled);
        precision.setRelevantRatingThreshhold(relevantThreshold);

        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> precision.setIgnoreUnlabeled(!ignoreUnlabeled));
        mutators.add(() -> precision.setRelevantRatingThreshhold(randomValueOtherThan(relevantThreshold, () -> randomIntBetween(0, 10))));
        randomFrom(mutators).run();
        return precision;
    }

    private static SearchHit[] toSearchHits(List<RatedDocument> rated, String index) {
        SearchHit[] hits = new SearchHit[rated.size()];
        for (int i = 0; i < rated.size(); i++) {
            hits[i] = new SearchHit(i, i + "", new Text(""), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index(index, "uuid"), 0, null));
        }
        return hits;
    }

    public enum Rating {
        IRRELEVANT, RELEVANT;
    }
}
