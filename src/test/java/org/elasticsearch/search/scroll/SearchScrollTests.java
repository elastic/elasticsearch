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

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SearchScrollTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleScrollQueryThenFetch() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();
        try {
            long counter = 0;
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
    
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
    
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    @Test
    public void testSimpleScrollQueryThenFetchSmallSizeUnevenDistribution() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            String routing = "0";
            if (i > 90) {
                routing = "1";
            } else if (i > 60) {
                routing = "2";
            }
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", i).setRouting(routing).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .setSize(3)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();
        try {
            long counter = 0;
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(3));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
    
            for (int i = 0; i < 32; i++) {
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(2))
                        .execute().actionGet();
    
                assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
                assertThat(searchResponse.getHits().hits().length, equalTo(3));
                for (SearchHit hit : searchResponse.getHits()) {
                    assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
                }
            }
    
            // and now, the last one is one
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(1));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
    
            // a the last is zero
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();
    
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, equalTo(0));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
            
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    @Test
    public void testScrollAndUpdateIndex() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 5)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 500; i++) {
            client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy").field("postDate", System.currentTimeMillis()).field("message", "test").endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(500l));
        assertThat(client().prepareCount().setQuery(termQuery("message", "test")).execute().actionGet().getCount(), equalTo(500l));
        assertThat(client().prepareCount().setQuery(termQuery("message", "test")).execute().actionGet().getCount(), equalTo(500l));
        assertThat(client().prepareCount().setQuery(termQuery("message", "update")).execute().actionGet().getCount(), equalTo(0l));
        assertThat(client().prepareCount().setQuery(termQuery("message", "update")).execute().actionGet().getCount(), equalTo(0l));

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryString("user:kimchy"))
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("postDate", SortOrder.ASC)
                .execute().actionGet();
        try {
            do {
                for (SearchHit searchHit : searchResponse.getHits().hits()) {
                    Map<String, Object> map = searchHit.sourceAsMap();
                    map.put("message", "update");
                    client().prepareIndex("test", "tweet", searchHit.id()).setSource(map).execute().actionGet();
                }
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
            } while (searchResponse.getHits().hits().length > 0);
    
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(500l));
            assertThat(client().prepareCount().setQuery(termQuery("message", "test")).execute().actionGet().getCount(), equalTo(0l));
            assertThat(client().prepareCount().setQuery(termQuery("message", "test")).execute().actionGet().getCount(), equalTo(0l));
            assertThat(client().prepareCount().setQuery(termQuery("message", "update")).execute().actionGet().getCount(), equalTo(500l));
            assertThat(client().prepareCount().setQuery(termQuery("message", "update")).execute().actionGet().getCount(), equalTo(500l));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    @Test
    public void testSimpleScrollQueryThenFetch_clearScrollIds() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse1 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();

        SearchResponse searchResponse2 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();

        long counter1 = 0;
        long counter2 = 0;

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .addScrollId(searchResponse1.getScrollId())
                .addScrollId(searchResponse2.getScrollId())
                .execute().actionGet();
        assertThat(clearResponse.isSucceeded(), equalTo(true));

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(0l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(0));

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(0l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(0));
    }

    @Test
    public void testSimpleScrollQueryThenFetch_clearAllScrollIds() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse1 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();

        SearchResponse searchResponse2 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();

        long counter1 = 0;
        long counter2 = 0;

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll().addScrollId("_all")
                .execute().actionGet();
        assertThat(clearResponse.isSucceeded(), equalTo(true));

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(0l));
        assertThat(searchResponse1.getHits().hits().length, equalTo(0));

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(0l));
        assertThat(searchResponse2.getHits().hits().length, equalTo(0));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/4156
    public void testDeepPaginationWithOneDocIndexAndDoNotBlowUp() throws Exception {
        client().prepareIndex("index", "type", "1")
                .setSource("field", "value")
                .setRefresh(true)
                .execute().get();

        for (SearchType searchType : SearchType.values()) {
            SearchRequestBuilder builder = client().prepareSearch("index")
                    .setSearchType(searchType)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(Integer.MAX_VALUE);

            if (searchType == SearchType.SCAN || searchType != SearchType.COUNT && randomBoolean()) {
                builder.setScroll("1m");
            }

            SearchResponse response = builder.execute().actionGet();
            try {
                ElasticsearchAssertions.assertHitCount(response, 1l);
            } finally {
                String scrollId = response.getScrollId();
                if (scrollId != null) {
                    clearScroll(scrollId);
                }
            }
        }

    }

}
