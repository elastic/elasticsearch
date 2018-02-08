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
import java.util.Map.Entry;
import java.util.Set;

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
    }

    /**
     * Test cases retrieves all six documents indexed above and checks the Prec@10
     * calculation where all unlabeled documents are treated as not relevant.
     */
    public void testRankEvalRequest() throws IOException {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", createRelevant("index" , "2", "3", "4", "5"), testQuery);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("index", "1"), testQuery);
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(amsterdamRequest);
        specifications.add(berlinRequest);
        PrecisionAtK metric = new PrecisionAtK(1, false, 10);
        RankEvalSpec spec = new RankEvalSpec(specifications, metric);
        spec.addIndices(Collections.singletonList("index"));

        RankEvalResponse response = execute(new RankEvalRequest(spec), highLevelClient()::rankEval, highLevelClient()::rankEvalAsync);
        // the expected Prec@ for the first query is 4/6 and the expected Prec@ for the second is 1/6, divided by 2 to get the average
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
                        assertEquals(1, hit.getRating().get().intValue());
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
                        assertEquals(1, hit.getRating().get().intValue());
                    } else {
                        assertFalse(hit.getRating().isPresent());
                    }
                }
            }
        }
    }

    private static List<RatedDocument> createRelevant(String indexName, String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument(indexName, doc, 1));
        }
        return relevant;
    }
}
