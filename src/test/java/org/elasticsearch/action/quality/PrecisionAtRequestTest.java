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

import com.google.common.collect.Maps;
import org.elasticsearch.action.quality.PrecisionAtN.Rating;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class PrecisionAtRequestTest extends ElasticsearchIntegrationTest {

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "value1").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "value2").get();
        client().prepareIndex("test", "testtype").setId("3")
                .setSource("text", "value2").get();
        client().prepareIndex("test", "testtype").setId("4")
                .setSource("text", "value2").get();
        client().prepareIndex("test", "testtype").setId("5")
                .setSource("text", "value2").get();
        client().prepareIndex("test", "testtype").setId("6")
                .setSource("text", "value2").get();
        refresh();
    }

    @Test
    public void testPrecisionAtFiveCalculation() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value1");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Map<String, Rating> relevant = Maps.newHashMap();
        relevant.put("1", Rating.RELEVANT);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals(1, (new PrecisionAtN(5)).evaluate(relevant, hits).getPrecision(), 0.00001);
    }
    
    @Test
    public void testPrecisionAtFiveIgnoreOneResult() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value2");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Map<String, Rating> relevant = Maps.newHashMap();
        relevant.put("2", Rating.RELEVANT);
        relevant.put("3", Rating.RELEVANT);
        relevant.put("4", Rating.RELEVANT);
        relevant.put("5", Rating.RELEVANT);
        relevant.put("6",  Rating.IRRELEVANT);
        SearchHit[] hits = response.getHits().getHits();

        assertEquals((double) 4 / 5, (new PrecisionAtN(5)).evaluate(relevant, hits).getPrecision(), 0.00001);
    }

    @Test
    public void testPrecisionAction() {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value2");
        SearchRequest request = client().prepareSearch("test").setQuery(query.buildAsBytes()).request();
        
        Map<String, Rating> relevant = Maps.newHashMap();
        relevant.put("2", Rating.RELEVANT);
        relevant.put("3", Rating.RELEVANT);
        relevant.put("4", Rating.RELEVANT);
        relevant.put("5", Rating.RELEVANT);

        Collection<Intent<Rating>> intents = new ArrayList<Intent<Rating>>();
        Intent<Rating> intent = new Intent<>();
        intent.setIntentId(0);
        intent.setRatedDocuments(relevant);
        intent.setIntentParameters(new HashMap<String, String>());

        Collection<Specification> specs = new ArrayList<Specification>();
        Specification spec = new Specification();
        spec.setSpecId(0);
        spec.setFilter(null);
        spec.setTargetIndex("test");
        spec.setTemplatedSearchRequest(request);

        PrecisionTask task = new PrecisionTask();
        task.setIntents(intents);
        task.setSpecifications(specs);
        
        PrecisionAtQueryBuilder builder = (new PrecisionAtQueryBuilder(client()).setTask(task));
        client().execute(PrecisionAtAction.INSTANCE, builder.request()).actionGet();
    }
}
