/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.basic;

import org.apache.lucene.util.English;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class SearchWithRandomExceptionsTests extends ElasticsearchIntegrationTest {
    
    @Test
    public void testRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        final int numShards = between(1, 5);
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                    startObject("type").
                        startObject("properties").
                            startObject("test")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject().
                        endObject().
                    endObject()
                .endObject().string();
        final double exceptionRate;
        final double exceptionOnOpenRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    exceptionOnOpenRate =  1.0/between(5, 100);
                    exceptionRate = 0.0d;
                } else {
                    exceptionRate =  1.0/between(5, 100);
                    exceptionOnOpenRate = 0.0d;
                }
            } else {
                exceptionOnOpenRate =  1.0/between(5, 100);
                exceptionRate =  1.0/between(5, 100);
            }
        } else {
            // rarely no exception
            exceptionRate = 0d;
            exceptionOnOpenRate = 0d;
        }
        
        Builder settings = settingsBuilder()
        .put("index.number_of_shards", numShards)
        .put("index.number_of_replicas", randomIntBetween(0, 1))
        .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE, exceptionRate)
        .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE_ON_OPEN, exceptionOnOpenRate)
        .put(MockDirectoryHelper.CHECK_INDEX_ON_CLOSE, true);
        logger.info("creating index: [test] using settings: [{}]", settings.build().getAsMap());
        client().admin().indices().prepareCreate("test")
                .setSettings(settings)
                .addMapping("type", mapping).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForYellowStatus().timeout(TimeValue.timeValueSeconds(5))).get(); // it's OK to timeout here 
        final int numDocs;
        final boolean expectAllShardsFailed;
        if (clusterHealthResponse.isTimedOut()) {
            /* some seeds just won't let you create the index at all and we enter a ping-pong mode
             * trying one node after another etc. that is ok but we need to make sure we don't wait
             * forever when indexing documents so we set numDocs = 1 and expecte all shards to fail
             * when we search below.*/
            logger.info("ClusterHealth timed out - only index one doc and expect searches to fail");
            numDocs = 1;
            expectAllShardsFailed = true;
        } else {
            numDocs = between(10, 100);
            expectAllShardsFailed = false;
        }
        long numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs ; i++) {
            try {
                client().prepareIndex("test", "type", "" + i).setTimeout(TimeValue.timeValueSeconds(1)).setSource("test", English.intToEnglish(i)).get();
                numCreated++;
                added[i] = true;
            } catch (ElasticSearchException ex) {
            }
        }
        logger.info("Start Refresh");
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get(); // don't assert on failures here
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info("Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ", refreshFailed, refreshResponse.getFailedShards(), refreshResponse.getShardFailures().length, refreshResponse.getSuccessfulShards(), refreshResponse.getTotalShards());

        final int numSearches = atLeast(10);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs-1);
                long expectedResults = added[docToQuery] ? 1 : 0; 
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery))).get();
                logger.info("Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), numShards);
                if (searchResponse.getSuccessfulShards() == numShards && !refreshFailed) {
                    assertThat(searchResponse.getHits().getTotalHits(), Matchers.equalTo(expectedResults));
                }
                // check match all
                searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                if (searchResponse.getSuccessfulShards() == numShards && !refreshFailed) {
                    assertThat(searchResponse.getHits().getTotalHits(), Matchers.equalTo(numCreated));
                }                
                
            } catch (SearchPhaseExecutionException ex) {
                if (!expectAllShardsFailed) {
                    throw ex;
                } else {
                    logger.info("expected SearchPhaseException: [{}]", ex.getMessage());
                }
            }
            
        }
    }
}
