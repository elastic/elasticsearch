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
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionTests.Rating;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static org.elasticsearch.index.rankeval.RankedListQualityMetric.filterUnknownDocuments;

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
                .setSource("text", "berlin", "title", "Berlin, Germany").get();
        client().prepareIndex("test", "testtype").setId("2").setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("3").setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("4").setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("5").setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("6").setSource("text", "amsterdam").get();
        refresh();
    }

    public void testPrecisionAtRequest() {
        List<String> indices = Arrays.asList(new String[] { "test" });
        List<String> types = Arrays.asList(new String[] { "testtype" });

        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query",
                createRelevant("2", "3", "4", "5"), testQuery);
        amsterdamRequest.setIndices(indices);
        amsterdamRequest.setTypes(types);
        amsterdamRequest.setSummaryFields(Arrays.asList(new String[] { "text", "title" }));

        specifications.add(amsterdamRequest);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("1"),
                testQuery);
        berlinRequest.setIndices(indices);
        berlinRequest.setTypes(types);
        berlinRequest.setSummaryFields(Arrays.asList(new String[] { "text", "title" }));

        specifications.add(berlinRequest);

        Precision metric = new Precision();
        metric.setIgnoreUnlabeled(true);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(),
                RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request())
                .actionGet();
        assertEquals(1.0, response.getQualityLevel(), Double.MIN_VALUE);
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
                        assertEquals(Rating.RELEVANT.ordinal(), hit.getRating().get().intValue());
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
                        assertEquals(Rating.RELEVANT.ordinal(), hit.getRating().get().intValue());
                    } else {
                        assertFalse(hit.getRating().isPresent());
                    }
                }
            }
        }
    }

    /**
     * test that running a bad query (e.g. one that will target a non existing
     * field) will produce an error in the response
     */
    public void testBadQuery() {
        List<String> indices = Arrays.asList(new String[] { "test" });
        List<String> types = Arrays.asList(new String[] { "testtype" });

        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder amsterdamQuery = new SearchSourceBuilder();
        amsterdamQuery.query(new MatchAllQueryBuilder());
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query",
                createRelevant("2", "3", "4", "5"), amsterdamQuery);
        amsterdamRequest.setIndices(indices);
        amsterdamRequest.setTypes(types);
        specifications.add(amsterdamRequest);

        SearchSourceBuilder brokenQuery = new SearchSourceBuilder();
        RangeQueryBuilder brokenRangeQuery = new RangeQueryBuilder("text").timeZone("CET");
        brokenQuery.query(brokenRangeQuery);
        RatedRequest brokenRequest = new RatedRequest("broken_query", createRelevant("1"),
                brokenQuery);
        brokenRequest.setIndices(indices);
        brokenRequest.setTypes(types);
        specifications.add(brokenRequest);

        RankEvalSpec task = new RankEvalSpec(specifications, new Precision());

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(),
                RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request())
                .actionGet();
        assertEquals(1, response.getFailures().size());
        ElasticsearchException[] rootCauses = ElasticsearchException
                .guessRootCauses(response.getFailures().get("broken_query"));
        assertEquals("[range] time_zone can not be applied to non date field [text]",
                rootCauses[0].getMessage());

    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument("test", "testtype", doc, Rating.RELEVANT.ordinal()));
        }
        return relevant;
    }
}
