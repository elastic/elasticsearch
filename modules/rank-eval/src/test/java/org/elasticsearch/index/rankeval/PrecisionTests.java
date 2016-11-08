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

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
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

public class PrecisionTests extends ESTestCase {

    public void testPrecisionAtFiveCalculation() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        EvalQueryQuality evaluated = (new Precision()).evaluate("id", toSearchHits(rated, "test", "testtype"), rated);
        assertEquals(1, evaluated.getQualityLevel(), 0.00001);
        assertEquals(1, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(1, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveIgnoreOneResult() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "3", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "4", Rating.IRRELEVANT.ordinal()));
        EvalQueryQuality evaluated = (new Precision()).evaluate("id", toSearchHits(rated, "test", "testtype"), rated);
        assertEquals((double) 4 / 5, evaluated.getQualityLevel(), 0.00001);
        assertEquals(4, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    /**
     * test that the relevant rating threshold can be set to something larger than 1.
     * e.g. we set it to 2 here and expect dics 0-2 to be not relevant, doc 3 and 4 to be relevant
     */
    public void testPrecisionAtFiveRelevanceThreshold() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", 0));
        rated.add(new RatedDocument("test", "testtype", "1", 1));
        rated.add(new RatedDocument("test", "testtype", "2", 2));
        rated.add(new RatedDocument("test", "testtype", "3", 3));
        rated.add(new RatedDocument("test", "testtype", "4", 4));
        Precision precisionAtN = new Precision();
        precisionAtN.setRelevantRatingThreshhold(2);
        EvalQueryQuality evaluated = precisionAtN.evaluate("id", toSearchHits(rated, "test", "testtype"), rated);
        assertEquals((double) 3 / 5, evaluated.getQualityLevel(), 0.00001);
        assertEquals(3, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveCorrectIndex() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test_other", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test_other", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.IRRELEVANT.ordinal()));
        // the following search hits contain only the last three documents
        EvalQueryQuality evaluated = (new Precision()).evaluate("id", toSearchHits(rated.subList(2, 5), "test", "testtype"), rated);
        assertEquals((double) 2 / 3, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testPrecisionAtFiveCorrectType() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "other_type", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "other_type", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.IRRELEVANT.ordinal()));
        EvalQueryQuality evaluated = (new Precision()).evaluate("id", toSearchHits(rated.subList(2, 5), "test", "testtype"), rated);
        assertEquals((double) 2 / 3, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testIgnoreUnlabeled() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "1", Rating.RELEVANT.ordinal()));
        // add an unlabeled search hit
        SearchHit[] searchHits = Arrays.copyOf(toSearchHits(rated, "test", "testtype"), 3);
        searchHits[2] = new InternalSearchHit(2, "2", new Text("testtype"), Collections.emptyMap());
        ((InternalSearchHit)searchHits[2]).shard(new SearchShardTarget("testnode", new Index("index", "uuid"), 0));

        EvalQueryQuality evaluated = (new Precision()).evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 3, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        Precision prec = new Precision();
        prec.setIgnoreUnlabeled(true);
        evaluated = prec.evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 2, evaluated.getQualityLevel(), 0.00001);
        assertEquals(2, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(2, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testNoRatedDocs() throws Exception {
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text("type"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("index", "uuid"), 0));
        }
        EvalQueryQuality evaluated = (new Precision()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.getQualityLevel(), 0.00001);
        assertEquals(0, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        Precision prec = new Precision();
        prec.setIgnoreUnlabeled(true);
        evaluated = prec.evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.getQualityLevel(), 0.00001);
        assertEquals(0, ((Precision.Breakdown) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((Precision.Breakdown) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testParseFromXContent() throws IOException {
        String xContent = " {\n"
         + "   \"relevant_rating_threshold\" : 2"
         + "}";
        XContentParser parser = XContentFactory.xContent(xContent).createParser(xContent);
        Precision precicionAt = Precision.fromXContent(parser, () -> ParseFieldMatcher.STRICT);
        assertEquals(2, precicionAt.getRelevantRatingThreshold());
    }

    public void testCombine() {
        Precision metric = new Precision();
        Vector<EvalQueryQuality> partialResults = new Vector<>(3);
        partialResults.add(new EvalQueryQuality("a", 0.1));
        partialResults.add(new EvalQueryQuality("b", 0.2));
        partialResults.add(new EvalQueryQuality("c", 0.6));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public static Precision createTestItem() {
        Precision precision = new Precision();
        if (randomBoolean()) {
            precision.setRelevantRatingThreshhold(randomIntBetween(0, 10));
        }
        precision.setIgnoreUnlabeled(randomBoolean());
        return precision;
    }

    public void testXContentRoundtrip() throws IOException {
        Precision testItem = createTestItem();
        XContentParser itemParser = RankEvalTestHelper.roundtrip(testItem);
        itemParser.nextToken();
        itemParser.nextToken();
        Precision parsedItem = Precision.fromXContent(itemParser, () -> ParseFieldMatcher.STRICT);
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());
    }

    private static SearchHit[] toSearchHits(List<RatedDocument> rated, String index, String type) {
        InternalSearchHit[] hits = new InternalSearchHit[rated.size()];
        for (int i = 0; i < rated.size(); i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text(type), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index(index, "uuid"), 0));
        }
        return hits;
    }

    public enum Rating {
        IRRELEVANT, RELEVANT;
    }
}
