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

package org.elasticsearch.test.integration.search.scan;

import com.google.common.collect.Sets;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

public class SearchScanTests extends AbstractSharedClusterTest {

    @Test
    @Slow 
    // TODO Randomize and reduce execution time
    public void testNarrowingQuery() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 5)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();


        Set<String> ids = Sets.newHashSet();
        Set<String> expectedIds = Sets.newHashSet();

        for (int i = 0; i < 100; i++) {
            expectedIds.add(Integer.toString(i));
            client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy1").field("postDate", System.currentTimeMillis()).field("message", "test").endObject()).execute().actionGet();
            // make some segments
            if (i % 10 == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        for (int i = 100; i < 200; i++) {
            client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy2").field("postDate", System.currentTimeMillis()).field("message", "test").endObject()).execute().actionGet();
            // make some segments
            if (i % 10 == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(termQuery("user", "kimchy1"))
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));

        // start scrolling, until we get not results
        while (true) {
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.id() + "should not exists in the result set", ids.contains(hit.id()), equalTo(false));
                ids.add(hit.id());
            }
            if (searchResponse.getHits().hits().length == 0) {
                break;
            }
        }

        assertThat(expectedIds, equalTo(ids));
    }
}