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

package org.elasticsearch.search.timeout;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE)
public class SearchTimeoutTests extends ElasticsearchIntegrationTest {

    
    //Timings in this test rely on having 2 shards
    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    //Timings in this test rely on having 2 shards
    @Override
    protected int maximumNumberOfShards() {
        return 2;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false).build();
    }

    @Test
    public void simpleTimeoutTest() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setTimeout("10ms")
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("Thread.sleep(500); return true;")))
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }
    
    @Test
    public void timedFileAccessTests() throws Exception {
        createIndex("test");

        //Create an index with documents that have test fields that hold delay times required to simulate 
        // processing the documents' content during a search
        int []delays={0,0,0,0,0,10,10,10,10,10,100,100,100,100,100,1000,1000};
        for (int i = 0; i < delays.length; i++) {
            client().prepareIndex("test", "type", Integer.toString(i))
                .setSource("delay", delays[i]).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
       
        //Run a series of searches on docs with different processing delays between doc retrieves 
        // and expected search timeouts
        assertThat(runTimeRestrictedSearch(0,10,1000).isTimedOut(),equalTo(false));
        assertThat(runTimeRestrictedSearch(10,10,1000).isTimedOut(),equalTo(false));
        assertThat(runTimeRestrictedSearch(100,100,200).isTimedOut(),equalTo(true));
        
        //Test partial results
        //One shard should take <250 ms but the other shard should take >250ms
        SearchResponse longResponse = runTimeRestrictedSearch(100,100,250);
        assertThat(longResponse.isTimedOut(),equalTo(true));
        SearchHits hits = longResponse.getHits();
        assertTrue("Must have some hits", hits.getTotalHits()>0);
        for (SearchHit searchHit : hits) {
            Integer delayField = (Integer) searchHit.getSource().get("delay");
            
            assertNotNull(delayField);
            int delay=delayField.intValue();
            //Must have only retrieved docs before the search timeout threshold 
            assertThat(delay, Matchers.lessThan(500));
        }
    }    
    @Test
    public void timedAggScriptTests() throws Exception {
        createIndex("test");
        
        // Create an index with documents that have test fields that hold delay times required to simulate 
        // processing the documents' content during a search
        int []delays={50,50,1000,50,50,2000,10,10};
        for (int i = 0; i < delays.length; i++) {
            client().prepareIndex("test", "type", Integer.toString(i))
                .setSource("delay", delays[i]).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
        
        //One shard should run 50+50+10ms delay and another 50+1000+50+10ms
        SearchResponse oneShardTimedoutResponse = runTimeRestrictedAgg(500, 0, 1000);            
        assertThat(oneShardTimedoutResponse.isTimedOut(),equalTo(true));
        Aggregations aggs = oneShardTimedoutResponse.getAggregations();
        assertNotNull(aggs);
        //One shard should run 50+50+1000+10 delays and another 50+50 + 2000+10 - both > 500ms timeout
        SearchResponse bothShardsTimedoutResponse = runTimeRestrictedAgg(500, 0, 2000);            
        assertThat(bothShardsTimedoutResponse.isTimedOut(),equalTo(true));
        aggs = bothShardsTimedoutResponse.getAggregations();
        assertNull(aggs);        
    }

    private SearchResponse runTimeRestrictedAgg(int overallTimeout, int fromDelay, int toDelay) {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setTimeout(overallTimeout+"ms")
                .setQuery(rangeQuery("delay").from(fromDelay).to(toDelay))
                //Use _source field in script to force slow evaluations that involve disk access
                .addAggregation(new TermsBuilder("delayAgg").script("Integer delay=_source.delay;"
                        + "Thread.sleep(delay);"
                        //If needed, explicit checking of timeouts rather than relying on implicit checks on file accesses..
//                        + "org.elasticsearch.common.util.concurrent.ActivityTimeMonitor.getCurrentThreadMonitor().checkForTimeout();"
                        + "return delay"))
                .execute().actionGet();
        return searchResponse;
    }    
    
    private SearchResponse runTimeRestrictedSearch(int fromDelay, int toDelay, int overallTimeout){
        SearchResponse searchResponse = client().prepareSearch("test")
                .setTimeout(overallTimeout+"ms")
               .setQuery(filteredQuery(rangeQuery("delay").from(fromDelay).to(toDelay),
                        //Use _source field in script to force slow evaluations that involve disk access
                        scriptFilter("Integer delay=_source.delay;"                              
                                + "Thread.sleep(delay);"
                                + "return true;")))
                .execute().actionGet();    
        return searchResponse;
    }
    
}
