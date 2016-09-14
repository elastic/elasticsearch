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
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyList;

public class PrecisionAtNTests extends ESTestCase {

    public void testPrecisionAtFiveCalculation() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        InternalSearchHit[] hits = new InternalSearchHit[1];
        hits[0] = new InternalSearchHit(0, "0", new Text("testtype"), Collections.emptyMap());
        hits[0].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        assertEquals(1, (new PrecisionAtN(5)).evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    public void testPrecisionAtFiveIgnoreOneResult() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "3", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "4", Rating.IRRELEVANT.ordinal()));
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text("testtype"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        assertEquals((double) 4 / 5, (new PrecisionAtN(5)).evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    /**
     * test that the relevant rating threshold can be set to something larger than 1.
     * e.g. we set it to 2 here and expect dics 0-2 to be not relevant, doc 3 and 4 to be relevant
     */
    public void testPrecisionAtFiveRelevanceThreshold() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "testtype", "0", 0));
        rated.add(new RatedDocument("test", "testtype", "1", 1));
        rated.add(new RatedDocument("test", "testtype", "2", 2));
        rated.add(new RatedDocument("test", "testtype", "3", 3));
        rated.add(new RatedDocument("test", "testtype", "4", 4));
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text("testtype"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        PrecisionAtN precisionAtN = new PrecisionAtN(5);
        precisionAtN.setRelevantRatingThreshhold(2);
        assertEquals((double) 3 / 5, precisionAtN.evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    public void testPrecisionAtFiveCorrectIndex() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test_other", "testtype", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test_other", "testtype", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "3", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "4", Rating.IRRELEVANT.ordinal()));
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text("testtype"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        assertEquals((double) 2 / 3, (new PrecisionAtN(5)).evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    public void testPrecisionAtFiveCorrectType() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument("test", "other_type", "0", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "other_type", "1", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "2", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "3", Rating.RELEVANT.ordinal()));
        rated.add(new RatedDocument("test", "testtype", "4", Rating.IRRELEVANT.ordinal()));
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i+"", new Text("testtype"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        assertEquals((double) 2 / 3, (new PrecisionAtN(5)).evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    public void testParseFromXContent() throws IOException {
        String xContent = " {\n"
         + "   \"size\": 10,\n"
         + "   \"relevant_rating_threshold\" : 2"
         + "}";
        XContentParser parser = XContentFactory.xContent(xContent).createParser(xContent);
        PrecisionAtN precicionAt = PrecisionAtN.fromXContent(parser, () -> ParseFieldMatcher.STRICT);
        assertEquals(10, precicionAt.getN());
        assertEquals(2, precicionAt.getRelevantRatingThreshold());
    }

    public void testCombine() {
        PrecisionAtN metric = new PrecisionAtN();
        Vector<EvalQueryQuality> partialResults = new Vector<>(3);
        partialResults.add(new EvalQueryQuality(0.1, emptyList()));
        partialResults.add(new EvalQueryQuality(0.2, emptyList()));
        partialResults.add(new EvalQueryQuality(0.6, emptyList()));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public static PrecisionAtN createTestItem() {
        int position = randomIntBetween(0, 1000);
        return new PrecisionAtN(position);
    }

    public void testXContentRoundtrip() throws IOException {
        PrecisionAtN testItem = createTestItem();
        XContentParser itemParser = XContentTestHelper.roundtrip(testItem);
        itemParser.nextToken();
        itemParser.nextToken();
        PrecisionAtN parsedItem = PrecisionAtN.fromXContent(itemParser, () -> ParseFieldMatcher.STRICT);
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());
    }
}
