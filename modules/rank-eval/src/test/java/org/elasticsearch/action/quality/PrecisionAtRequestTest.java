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

package org.elasticsearch.action.quality;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionAtN;
import org.elasticsearch.index.rankeval.QuerySpec;
import org.elasticsearch.index.rankeval.RankEvalAction;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalRequestBuilder;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalResult;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedQuery;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0) // NORELEASE need to fix transport client use case
public class PrecisionAtRequestTest  extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(RankEvalPlugin.class);
    }
    
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(RankEvalPlugin.class);
    }

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "berlin").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("3")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("4")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("5")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("6")
                .setSource("text", "amsterdam").get();
        refresh();
    }


    @Test
    public void testPrecisionAtFiveCalculation() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "berlin");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Map<String, Integer> relevant = new HashMap<>();
        relevant.put("1", Rating.RELEVANT.ordinal());
        RatedQuery intent = new RatedQuery(0, new HashMap<>(), relevant);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals(1, (new PrecisionAtN(5)).evaluate(hits, intent).getQualityLevel(), 0.00001);
    }
    
    @Test
    public void testPrecisionAtFiveIgnoreOneResult() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "amsterdam");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Map<String, Integer> relevant = new HashMap<>();
        relevant.put("2", Rating.RELEVANT.ordinal());
        relevant.put("3", Rating.RELEVANT.ordinal());
        relevant.put("4", Rating.RELEVANT.ordinal());
        relevant.put("5", Rating.RELEVANT.ordinal());
        relevant.put("6",  Rating.IRRELEVANT.ordinal());
        RatedQuery intent = new RatedQuery(0, new HashMap<>(), relevant);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals((double) 4 / 5, (new PrecisionAtN(5)).evaluate(hits, intent).getQualityLevel(), 0.00001);
    }
    
    @Test
    public void testPrecisionAction() {
        Collection<RatedQuery> intents = new ArrayList<RatedQuery>();
        RatedQuery intentAmsterdam = new RatedQuery(
                0,
                createParams("var", "amsterdam"),
                createRelevant("2", "3", "4", "5"));
        intents.add(intentAmsterdam);

        RatedQuery intentBerlin = new RatedQuery(
                1,
                createParams("var", "berlin"),
                createRelevant("1"));
        intents.add(intentBerlin);

        Collection<QuerySpec> specs = new ArrayList<QuerySpec>();
        ArrayList<String> indices = new ArrayList<>();
        indices.add("test");
        ArrayList<String> types = new ArrayList<>();
        types.add("testtype");

        Template template = new Template(
                "{\n" + 
                "      \"query\": { \"match\" : { \"text\" : \"{{var}}\" } },\n" + 
                "      \"size\" : 10\n" + 
                "    }",
                ScriptType.INLINE,
                null,
                XContentType.JSON,
                null);
        
        SearchSourceBuilder source = new SearchSourceBuilder();
        QuerySpec spec = new QuerySpec(0, source, indices, types, template);
        specs.add(spec);

        RankEvalSpec task = new RankEvalSpec(intents, specs, new PrecisionAtN(10));
        
        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(
                client(),
                RankEvalAction.INSTANCE,
                new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        RankEvalResult result = response.getRankEvalResults().iterator().next();
        for (Entry<Integer, Collection<String>> entry : result.getUnknownDocs().entrySet()) {
            if (entry.getKey() == 0) {
                assertEquals(1, entry.getValue().size());
            } else {
                assertEquals(0, entry.getValue().size());
            }
        }
    }
    
    private Map<String, Integer> createRelevant(String... docs) {
        Map<String, Integer> relevant = new HashMap<>();
        for (String doc : docs) {
            relevant.put(doc, Rating.RELEVANT.ordinal());
        }
        return relevant;
    }
    
    private Map<String, Object> createParams(String key, String value) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(key, value);
        return parameters;
    }

 }
