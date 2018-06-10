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

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.rankeval.EvalQueryQuality;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.rankeval.EvaluationMetric.filterUnknownDocuments;

public class RankEvalIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        StringEntity doc = new StringEntity("{\"text\":\"berlin\"}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/doc/1", Collections.emptyMap(), doc);
        doc = new StringEntity("{\"text\":\"amsterdam\"}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/doc/2", Collections.emptyMap(), doc);
        client().performRequest("PUT", "/index/doc/3", Collections.emptyMap(), doc);
        client().performRequest("PUT", "/index/doc/4", Collections.emptyMap(), doc);
        client().performRequest("PUT", "/index/doc/5", Collections.emptyMap(), doc);
        client().performRequest("PUT", "/index/doc/6", Collections.emptyMap(), doc);
        client().performRequest("POST", "/index/_refresh");

        // add another index to test basic multi index support
        client().performRequest("PUT", "/index2/doc/7", Collections.emptyMap(), doc);
        client().performRequest("POST", "/index2/_refresh");
    }

    /**
     * Test cases retrieves all six documents indexed above and checks the Prec@10
     * calculation where all unlabeled documents are treated as not relevant.
     */
    public void testRankEvalRequest() throws IOException {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        List<RatedDocument> amsterdamRatedDocs = createRelevant("index" , "2", "3", "4", "5");
        amsterdamRatedDocs.addAll(createRelevant("index2", "7"));
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", amsterdamRatedDocs, testQuery);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("index", "1"), testQuery);
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(amsterdamRequest);
        specifications.add(berlinRequest);
        PrecisionAtK metric = new PrecisionAtK(1, false, 10);
        RankEvalSpec spec = new RankEvalSpec(specifications, metric);

        RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, new String[] { "index", "index2" });
        RankEvalResponse response = execute(rankEvalRequest, highLevelClient()::rankEval, highLevelClient()::rankEvalAsync,
                highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
        // the expected Prec@ for the first query is 5/7 and the expected Prec@ for the second is 1/7, divided by 2 to get the average
        double expectedPrecision = (1.0 / 7.0 + 5.0 / 7.0) / 2.0;
        assertEquals(expectedPrecision, response.getEvaluationResult(), Double.MIN_VALUE);
        Map<String, EvalQueryQuality> partialResults = response.getPartialResults();
        assertEquals(2, partialResults.size());
        EvalQueryQuality amsterdamQueryQuality = partialResults.get("amsterdam_query");
        assertEquals(2, filterUnknownDocuments(amsterdamQueryQuality.getHitsAndRatings()).size());
        List<RatedSearchHit> hitsAndRatings = amsterdamQueryQuality.getHitsAndRatings();
        assertEquals(7, hitsAndRatings.size());
        for (RatedSearchHit hit : hitsAndRatings) {
            String id = hit.getSearchHit().getId();
            if (id.equals("1") || id.equals("6")) {
                assertFalse(hit.getRating().isPresent());
            } else {
                assertEquals(1, hit.getRating().get().intValue());
            }
        }
        EvalQueryQuality berlinQueryQuality = partialResults.get("berlin_query");
        assertEquals(6, filterUnknownDocuments(berlinQueryQuality.getHitsAndRatings()).size());
        hitsAndRatings = berlinQueryQuality.getHitsAndRatings();
        assertEquals(7, hitsAndRatings.size());
        for (RatedSearchHit hit : hitsAndRatings) {
            String id = hit.getSearchHit().getId();
            if (id.equals("1")) {
                assertEquals(1, hit.getRating().get().intValue());
            } else {
                assertFalse(hit.getRating().isPresent());
            }
        }

        // now try this when test2 is closed
        client().performRequest("POST", "index2/_close", Collections.emptyMap());
        rankEvalRequest.indicesOptions(IndicesOptions.fromParameters(null, "true", null, SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = execute(rankEvalRequest, highLevelClient()::rankEval, highLevelClient()::rankEvalAsync,
                highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
    }

    private static List<RatedDocument> createRelevant(String indexName, String... docs) {
        return Stream.of(docs).map(s -> new RatedDocument(indexName, s, 1)).collect(Collectors.toList());
    }
}
