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

package org.elasticsearch.search.scan;

import com.google.common.collect.Sets;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

public class SearchScanTests extends ElasticsearchIntegrationTest {

    @Test
    @Slow 
    public void testNarrowingQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        Set<String> ids = Sets.newHashSet();
        Set<String> expectedIds = Sets.newHashSet();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[scaledRandomIntBetween(50, 100)];
        for (int i = 0; i < builders.length/2; i++) {
            expectedIds.add(Integer.toString(i));
            builders[i] = client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy1").field("postDate", System.currentTimeMillis()).field("message", "test").endObject());
        }

        for (int i = builders.length/2; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy2").field("postDate", System.currentTimeMillis()).field("message", "test").endObject());
        }
        indexRandom(true, builders);

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(termQuery("user", "kimchy1"))
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo((long)builders.length/2));

        // start scrolling, until we get not results
        while (true) {
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
            assertThat(searchResponse.getHits().totalHits(), equalTo((long)builders.length/2));
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