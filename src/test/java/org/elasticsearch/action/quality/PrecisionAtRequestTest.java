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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.quality.PrecisionAtN.Rating;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@Seed("4D104273985C6D62")
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class PrecisionAtRequestTest extends ElasticsearchIntegrationTest {

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

        Map<String, Integer> relevant = Maps.newHashMap();
        relevant.put("1", Rating.RELEVANT.ordinal());
        Intent intent = new Intent();
        intent.setRatedDocuments(relevant);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals(1, (new PrecisionAtN(5)).evaluate(intent, hits).getQualityLevel(), 0.00001);
    }
    
    @Test
    public void testPrecisionAtFiveIgnoreOneResult() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "amsterdam");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Map<String, Integer> relevant = Maps.newHashMap();
        relevant.put("2", Rating.RELEVANT.ordinal());
        relevant.put("3", Rating.RELEVANT.ordinal());
        relevant.put("4", Rating.RELEVANT.ordinal());
        relevant.put("5", Rating.RELEVANT.ordinal());
        relevant.put("6",  Rating.IRRELEVANT.ordinal());
        Intent intent = new Intent();
        intent.setRatedDocuments(relevant);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals((double) 4 / 5, (new PrecisionAtN(5)).evaluate(intent, hits).getQualityLevel(), 0.00001);
    }
    
    @Test
    @Repeat(iterations = 1)
    @Seed("56387767345D4ADA")
    public void testPrecisionAction() {
        Collection<Intent> intents = new ArrayList<Intent>();
        Intent intentAmsterdam = new Intent();
        intentAmsterdam.setIntentId(0);
        intentAmsterdam.setRatedDocuments(createRelevant("2", "3", "4", "5"));
        intentAmsterdam.setIntentParameters(createParams("var", "amsterdam"));
        intents.add(intentAmsterdam);

        Intent intentBerlin = new Intent();
        intentBerlin.setIntentId(1);
        intentBerlin.setRatedDocuments(createRelevant("1"));
        intentBerlin.setIntentParameters(createParams("var", "berlin"));
        intents.add(intentBerlin);

        Collection<Specification> specs = new ArrayList<Specification>();
        Specification spec = new Specification();
        spec.setSpecId(0);
        spec.setFilter(null);
        spec.setTargetIndices(Lists.newArrayList("test"));
        spec.setSearchRequestTemplate("{\"match\": {\"text\" : \"{{var}}\" } }");
        specs.add(spec);

        PrecisionAtNContext config = new PrecisionAtNContext();
        config.setN(10);

        QualityTask task = new QualityTask();
        task.setIntents(intents);
        task.setSpecifications(specs);
        task.setConfig(config);
        
        QualityRequestBuilder builder = (new QualityRequestBuilder(client()).setTask(task));
        QualityResponse response = client().execute(QualityAction.INSTANCE, builder.request()).actionGet();
        QualityResult result = response.getPrecision().iterator().next();
        for (Entry<Integer, Collection<String>> entry : result.getUnknownDocs().entrySet()) {
            if (entry.getKey() == 0) {
                assertEquals(1, entry.getValue().size());
            } else {
                assertEquals(0, entry.getValue().size());
            }
        }
    }
    
    private Map<String, Integer> createRelevant(String... docs) {
        Map<String, Integer> relevant = Maps.newHashMap();
        for (String doc : docs) {
            relevant.put(doc, Rating.RELEVANT.ordinal());
        }
        return relevant;
    }
    
    private Map<String, String> createParams(String key, String value) {
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(key, value);
        return parameters;
    }

 }
