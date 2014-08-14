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

import com.google.common.collect.Sets;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
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

        Set<String> relevant = Sets.newHashSet("1");
        SearchHit[] hits = response.getHits().getHits();

        assertEquals(1, (new PrecisionAtN(5)).evaluate(relevant, hits), 0.00001);
    }
    
    @Test
    public void testPrecisionAtFiveIgnoreOneResult() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value2");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();

        Set<String> relevant = Sets.newHashSet("2", "3", "4", "5");
        SearchHit[] hits = response.getHits().getHits();

        assertEquals((double) 4 / 5, (new PrecisionAtN(5)).evaluate(relevant, hits), 0.00001);
    }


    /** TODO change PrecisionAt bound classes to support generic qa metrics - hint: Naming etc.*/
    @Test
    public void testPrecisionAction() {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value2");
        SearchRequest request = client().prepareSearch("test").setQuery(query.buildAsBytes()).request();

        Set<String> relevant = Sets.newHashSet("2", "3", "4", "5");

        PrecisionAtQueryBuilder builder = (new PrecisionAtQueryBuilder(client()).setSearchRequest(request).addRelevantDocs(relevant));
        client().execute(PrecisionAtAction.INSTANCE, builder.request()).actionGet();
    }
}
