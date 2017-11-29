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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static org.elasticsearch.index.rankeval.EvaluationMetric.filterUnknownDocuments;

public class RankEvalRequestIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "berlin", "title", "Berlin, Germany", "population", 3670622).get();
        client().prepareIndex("test", "testtype").setId("2").setSource("text", "amsterdam", "population", 851573).get();
        client().prepareIndex("test", "testtype").setId("3").setSource("text", "amsterdam", "population", 851573).get();
        client().prepareIndex("test", "testtype").setId("4").setSource("text", "amsterdam", "population", 851573).get();
        client().prepareIndex("test", "testtype").setId("5").setSource("text", "amsterdam", "population", 851573).get();
        client().prepareIndex("test", "testtype").setId("6").setSource("text", "amsterdam", "population", 851573).get();
        refresh();
    }

    public void testPrecisionAtRequest() {
        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("_id");
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query",
                createRelevant("2", "3", "4", "5"), testQuery);
        amsterdamRequest.addSummaryFields(Arrays.asList(new String[] { "text", "title" }));

        specifications.add(amsterdamRequest);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("1"),
                testQuery);
        berlinRequest.addSummaryFields(Arrays.asList(new String[] { "text", "title" }));
        specifications.add(berlinRequest);

        PrecisionAtK metric = new PrecisionAtK(1, false, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(),
                RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request())
                .actionGet();
        double expectedPrecision = (1.0 / 6.0 + 4.0 / 6.0) / 2.0;
        assertEquals(expectedPrecision, response.getEvaluationResult(), Double.MIN_VALUE);
        Set<Entry<String, EvalQueryQuality>> entrySet = response.getPartialResults().entrySet();
        assertEquals(2, entrySet.size());
        for (Entry<String, EvalQueryQuality> entry : entrySet) {
            EvalQueryQuality quality = entry.getValue();
            if (entry.getKey() == "amsterdam_query") {
                assertEquals(2, filterUnknownDocuments(quality.getHitsAndRatings()).size());
                List<RatedSearchHit> hitsAndRatings = quality.getHitsAndRatings();
                assertEquals(6, hitsAndRatings.size());
                for (RatedSearchHit hit : hitsAndRatings) {
                    String id = hit.getSearchHit().getId();
                    if (id.equals("1") || id.equals("6")) {
                        assertFalse(hit.getRating().isPresent());
                    } else {
                        assertEquals(TestRatingEnum.RELEVANT.ordinal(), hit.getRating().get().intValue());
                    }
                }
            }
            if (entry.getKey() == "berlin_query") {
                assertEquals(5, filterUnknownDocuments(quality.getHitsAndRatings()).size());
                List<RatedSearchHit> hitsAndRatings = quality.getHitsAndRatings();
                assertEquals(6, hitsAndRatings.size());
                for (RatedSearchHit hit : hitsAndRatings) {
                    String id = hit.getSearchHit().getId();
                    if (id.equals("1")) {
                        assertEquals(TestRatingEnum.RELEVANT.ordinal(), hit.getRating().get().intValue());
                    } else {
                        assertFalse(hit.getRating().isPresent());
                    }
                }
            }
        }

        // test that a different window size k affects the result
        metric = new PrecisionAtK(1, false, 3);
        task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        expectedPrecision = (1.0 / 3.0 + 2.0 / 3.0) / 2.0;
        assertEquals(expectedPrecision, response.getEvaluationResult(), Double.MIN_VALUE);
    }

    public void testNDCGRequest() {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("_id");

        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(new RatedRequest("amsterdam_query", createRelevant("5"), testQuery));
        specifications.add(new RatedRequest("berlin_query", createRelevant("1"), testQuery));

        DiscountedCumulativeGain metric = new DiscountedCumulativeGain(true, null, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(0.6934264036172708, response.getEvaluationResult(), Double.MIN_VALUE);

        // test that a different window size k affects the result
        metric = new DiscountedCumulativeGain(true, null, 3);
        task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(0.5, response.getEvaluationResult(), Double.MIN_VALUE);
    }

    public void testMRRRequest() {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("_id");

        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(new RatedRequest("amsterdam_query", createRelevant("5"), testQuery));
        specifications.add(new RatedRequest("berlin_query", createRelevant("1"), testQuery));

        MeanReciprocalRank metric = new MeanReciprocalRank(1, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        double expectedMRR = (1.0 / 1.0 + 1.0 / 5.0) / 2.0;
        assertEquals(expectedMRR, response.getEvaluationResult(), 0.0);

        // test that a different window size k affects the result
        metric = new MeanReciprocalRank(1, 3);
        task = new RankEvalSpec(specifications, metric);
        task.addIndices(Collections.singletonList("test"));

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        expectedMRR = (1.0 / 1.0) / 2.0;
        assertEquals(expectedMRR, response.getEvaluationResult(), 0.0);
    }

    /**
     * test that running a bad query (e.g. one that will target a non existing
     * field) will produce an error in the response
     */
    public void testBadQuery() {
        List<String> indices = Arrays.asList(new String[] { "test" });

        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder amsterdamQuery = new SearchSourceBuilder();
        amsterdamQuery.query(new MatchAllQueryBuilder());
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query",
                createRelevant("2", "3", "4", "5"), amsterdamQuery);
        specifications.add(amsterdamRequest);

        SearchSourceBuilder brokenQuery = new SearchSourceBuilder();
        brokenQuery.query(QueryBuilders.termQuery("population", "noStringOnNumericFields"));
        RatedRequest brokenRequest = new RatedRequest("broken_query", createRelevant("1"),
                brokenQuery);
        specifications.add(brokenRequest);

        RankEvalSpec task = new RankEvalSpec(specifications, new PrecisionAtK());
        task.addIndices(indices);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(1, response.getFailures().size());
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(response.getFailures().get("broken_query"));
        assertEquals("java.lang.NumberFormatException: For input string: \"noStringOnNumericFields\"", rootCauses[0].getCause().toString());
    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument("test", doc, TestRatingEnum.RELEVANT.ordinal()));
        }
        return relevant;
    }
}
