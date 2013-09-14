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
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.AbstractSharedClusterTest;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.store.mock.MockDirectoryHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class SearchWithRandomExceptionsTests extends AbstractSharedClusterTest {
    
    @Test
    @AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/3694")
    public void testRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        final int numShards = between(1, 5);
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                    startObject("type").
                        startObject("properties").
                            startObject("test").field("type", "string").endObject().
                        endObject().
                    endObject()
                .endObject().string();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", numShards)
                        .put("index.number_of_replicas", randomIntBetween(0, 1))
                        .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE, randomBoolean() ? 1.0/between(10, 100) : 0.0)
                        .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE_ON_OPEN, randomBoolean() ? 1.0/between(10, 100) : 0.0)
                        .put(MockDirectoryHelper.CHECK_INDEX_ON_CLOSE, true))
                .addMapping("type", mapping).execute().actionGet();
        client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().timeout(TimeValue.timeValueMillis(100))).get(); // it's ok to timeout here 
        int numDocs = between(10, 100);
        for (int i = 0; i < numDocs ; i++) {
            try {
                client().prepareIndex("test", "type", "" + i).setSource("test", English.intToEnglish(i)).get();
            } catch (ElasticSearchException ex) {
            }
        }
        client().admin().indices().prepareRefresh("test").execute().get(); // don't assert on failures here
        int numSearches = atLeast(10);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            client().prepareSearch().setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(between(0, numDocs)))).get();
        }
    }
}
