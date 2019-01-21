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

package org.elasticsearch.client;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.rankeval.DiscountedCumulativeGain;
import org.elasticsearch.index.rankeval.EvalQueryQuality;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.ExpectedReciprocalRank;
import org.elasticsearch.index.rankeval.MeanReciprocalRank;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RatedSearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.rankeval.EvaluationMetric.filterUnratedDocuments;

public class RankEvalIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        Request berlin = new Request("PUT", "/index/_doc/berlin");
        berlin.setJsonEntity("{\"text\":\"berlin\"}");
        client().performRequest(berlin);
        for (int i = 0; i < 6; i++) {
            // add another index to test basic multi index support
            String index = i == 0 ? "index2" : "index";
            Request amsterdam = new Request("PUT", "/" + index + "/_doc/amsterdam" + i);
            amsterdam.setJsonEntity("{\"text\":\"amsterdam\"}");
            client().performRequest(amsterdam);
        }
        client().performRequest(new Request("POST", "/_refresh"));
    }

    /**
     * Test cases retrieves all six documents indexed above and checks the Prec@10
     * calculation where all unlabeled documents are treated as not relevant.
     */
    public void testRankEvalRequest() throws IOException {
        List<RatedRequest> specifications = createTestEvaluationSpec();
        PrecisionAtK metric = new PrecisionAtK(1, false, 10);
        RankEvalSpec spec = new RankEvalSpec(specifications, metric);

        RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, new String[] { "index", "index2" });
        RankEvalResponse response = execute(rankEvalRequest, highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
        // the expected Prec@ for the first query is 5/7 and the expected Prec@ for the second is 1/7, divided by 2 to get the average
        double expectedPrecision = (1.0 / 7.0 + 5.0 / 7.0) / 2.0;
        assertEquals(expectedPrecision, response.getMetricScore(), Double.MIN_VALUE);
        Map<String, EvalQueryQuality> partialResults = response.getPartialResults();
        assertEquals(2, partialResults.size());
        EvalQueryQuality amsterdamQueryQuality = partialResults.get("amsterdam_query");
        assertEquals(2, filterUnratedDocuments(amsterdamQueryQuality.getHitsAndRatings()).size());
        List<RatedSearchHit> hitsAndRatings = amsterdamQueryQuality.getHitsAndRatings();
        assertEquals(7, hitsAndRatings.size());
        for (RatedSearchHit hit : hitsAndRatings) {
            String id = hit.getSearchHit().getId();
            if (id.equals("berlin") || id.equals("amsterdam5")) {
                assertFalse(hit.getRating().isPresent());
            } else {
                assertEquals(1, hit.getRating().getAsInt());
            }
        }
        EvalQueryQuality berlinQueryQuality = partialResults.get("berlin_query");
        assertEquals(6, filterUnratedDocuments(berlinQueryQuality.getHitsAndRatings()).size());
        hitsAndRatings = berlinQueryQuality.getHitsAndRatings();
        assertEquals(7, hitsAndRatings.size());
        for (RatedSearchHit hit : hitsAndRatings) {
            String id = hit.getSearchHit().getId();
            if (id.equals("berlin")) {
                assertEquals(1, hit.getRating().getAsInt());
            } else {
                assertFalse(hit.getRating().isPresent());
            }
        }

        // now try this when test2 is closed
        client().performRequest(new Request("POST", "index2/_close"));
        rankEvalRequest.indicesOptions(IndicesOptions.fromParameters(null, "true", null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = execute(rankEvalRequest, highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
    }

    private static List<RatedRequest> createTestEvaluationSpec() {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        List<RatedDocument> amsterdamRatedDocs = createRelevant("index" , "amsterdam1", "amsterdam2", "amsterdam3", "amsterdam4");
        amsterdamRatedDocs.addAll(createRelevant("index2", "amsterdam0"));
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", amsterdamRatedDocs, testQuery);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("index", "berlin"), testQuery);
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(amsterdamRequest);
        specifications.add(berlinRequest);
        return specifications;
    }

    /**
     * Test case checks that the default metrics are registered and usable
     */
    public void testMetrics() throws IOException {
        List<RatedRequest> specifications = createTestEvaluationSpec();
        List<Supplier<EvaluationMetric>> metrics = Arrays.asList(PrecisionAtK::new, MeanReciprocalRank::new, DiscountedCumulativeGain::new,
                () -> new ExpectedReciprocalRank(1));
        double expectedScores[] = new double[] {0.4285714285714286, 0.75, 1.6408962261063627, 0.4407738095238095};
        int i = 0;
        for (Supplier<EvaluationMetric> metricSupplier : metrics) {
            RankEvalSpec spec = new RankEvalSpec(specifications, metricSupplier.get());

            RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, new String[] { "index", "index2" });
            RankEvalResponse response = execute(rankEvalRequest, highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
            assertEquals(expectedScores[i], response.getMetricScore(), Double.MIN_VALUE);
            i++;
        }
    }

    private static List<RatedDocument> createRelevant(String indexName, String... docs) {
        return Stream.of(docs).map(s -> new RatedDocument(indexName, s, 1)).collect(Collectors.toList());
    }
}
