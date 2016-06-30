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

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
public class SearchScrollIT extends ESIntegTestCase {
    public void testSimpleScrollQueryThenFetch() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).execute().actionGet();
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

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testSimpleScrollQueryThenFetchSmallSizeUnevenDistribution() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).execute().actionGet();
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

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(3));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }

            for (int i = 0; i < 32; i++) {
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(2))
                        .execute().actionGet();

                assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
                assertThat(searchResponse.getHits().hits().length, equalTo(3));
                for (SearchHit hit : searchResponse.getHits()) {
                    assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
                }
            }

            // and now, the last one is one
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(1));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }

            // a the last is zero
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().hits().length, equalTo(0));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
            }

        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testScrollAndUpdateIndex() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 5)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 500; i++) {
            client().prepareIndex("test", "tweet", Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy").field("postDate", System.currentTimeMillis()).field("message", "test").endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        assertThat(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).execute().actionGet().getHits().totalHits(), equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).execute().actionGet().getHits().totalHits(), equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).execute().actionGet().getHits().totalHits(), equalTo(0L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).execute().actionGet().getHits().totalHits(), equalTo(0L));

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("user:kimchy"))
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
            assertThat(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(500L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).execute().actionGet().getHits().totalHits(), equalTo(500L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).execute().actionGet().getHits().totalHits(), equalTo(500L));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testSimpleScrollQueryThenFetch_clearScrollIds() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).execute().actionGet();
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

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100L));
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

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .addScrollId(searchResponse1.getScrollId())
                .addScrollId(searchResponse2.getScrollId())
                .execute().actionGet();
        assertThat(clearResponse.isSucceeded(), is(true));
        assertThat(clearResponse.getNumFreed(), greaterThan(0));
        assertThat(clearResponse.status(), equalTo(RestStatus.OK));
        assertToXContentResponse(clearResponse, true, clearResponse.getNumFreed());

        assertThrows(client().prepareSearchScroll(searchResponse1.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
        assertThrows(client().prepareSearchScroll(searchResponse2.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
    }

    public void testClearNonExistentScrollId() throws Exception {
        createIndex("idx");
        ClearScrollResponse response = client().prepareClearScroll()
                .addScrollId("DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAABFnRtLWMyRzBqUUQyNk1uM0xDTjJ4S0EAAAAAAAAAARYzNkhxbWFTYVFVNmgxTGQyYUZVYV9nAAAAAAAAAAEWdVcxNWZmRGZSVFN2V0xMUGF2NGx1Zw==")
                .get();
        // Whether we actually clear a scroll, we can't know, since that information isn't serialized in the
        // free search context response, which is returned from each node we want to clear a particular scroll.
        assertThat(response.isSucceeded(), is(true));
        assertThat(response.getNumFreed(), equalTo(0));
        assertThat(response.status(), equalTo(RestStatus.NOT_FOUND));
        assertToXContentResponse(response, true, response.getNumFreed());
    }

    public void testClearIllegalScrollId() throws Exception {
        createIndex("idx");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().prepareClearScroll().addScrollId("c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1").get());
        assertEquals("Cannot parse scroll id", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                // Fails during base64 decoding (Base64-encoded string must have at least four characters)
                () ->  client().prepareClearScroll().addScrollId("a").get());
        assertEquals("Cannot parse scroll id", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                // Other invalid base64
                () ->  client().prepareClearScroll().addScrollId("abcabc").get());
        assertEquals("Cannot parse scroll id", e.getMessage());
    }

    public void testSimpleScrollQueryThenFetchClearAllScrollIds() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).execute().actionGet();
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

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100L));
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

        assertThat(searchResponse1.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse1.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse2.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll().addScrollId("_all")
                .execute().actionGet();
        assertThat(clearResponse.isSucceeded(), is(true));
        assertThat(clearResponse.getNumFreed(), greaterThan(0));
        assertThat(clearResponse.status(), equalTo(RestStatus.OK));
        assertToXContentResponse(clearResponse, true, clearResponse.getNumFreed());

        assertThrows(internalCluster().transportClient().prepareSearchScroll(searchResponse1.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
        assertThrows(internalCluster().transportClient().prepareSearchScroll(searchResponse2.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
    }

    public void testDeepScrollingDoesNotBlowUp() throws Exception {
        client().prepareIndex("index", "type", "1")
                .setSource("field", "value")
                .setRefreshPolicy(IMMEDIATE)
                .execute().get();

        for (SearchType searchType : SearchType.values()) {
            SearchRequestBuilder builder = client().prepareSearch("index")
                    .setSearchType(searchType)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(Integer.MAX_VALUE)
                    .setScroll("1m");

            SearchResponse response = builder.execute().actionGet();
            try {
                ElasticsearchAssertions.assertHitCount(response, 1L);
            } finally {
                String scrollId = response.getScrollId();
                if (scrollId != null) {
                    clearScroll(scrollId);
                }
            }
        }
    }

    public void testThatNonExistingScrollIdReturnsCorrectException() throws Exception {
        client().prepareIndex("index", "type", "1").setSource("field", "value").execute().get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));

        ClearScrollResponse clearScrollResponse = client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
        assertThat(clearScrollResponse.isSucceeded(), is(true));

        assertThrows(internalCluster().transportClient().prepareSearchScroll(searchResponse.getScrollId()), RestStatus.NOT_FOUND);
    }

    public void testStringSortMissingAscTerminates() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("test", "no_field", "type=keyword", "some_field", "type=keyword"));
        client().prepareIndex("test", "test", "1").setSource("some_field", "test").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setTypes("test")
                .addSort(new FieldSortBuilder("no_field").order(SortOrder.ASC).missing("_last"))
                .setScroll("1m")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        response = client().prepareSearchScroll(response.getScrollId()).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
        assertNoSearchHits(response);

        response = client().prepareSearch("test")
                .setTypes("test")
                .addSort(new FieldSortBuilder("no_field").order(SortOrder.ASC).missing("_first"))
                .setScroll("1m")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        response = client().prepareSearchScroll(response.getScrollId()).get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getHits().length, equalTo(0));
    }

    public void testParseSearchScrollRequest() throws Exception {
        BytesReference content = XContentFactory.jsonBuilder()
            .startObject()
            .field("scroll_id", "SCROLL_ID")
            .field("scroll", "1m")
            .endObject().bytes();

        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        RestSearchScrollAction.buildFromContent(content, searchScrollRequest);

        assertThat(searchScrollRequest.scrollId(), equalTo("SCROLL_ID"));
        assertThat(searchScrollRequest.scroll().keepAlive(), equalTo(TimeValue.parseTimeValue("1m", null, "scroll")));
    }

    public void testParseSearchScrollRequestWithInvalidJsonThrowsException() throws Exception {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        try {
            RestSearchScrollAction.buildFromContent(new BytesArray("{invalid_json}"), searchScrollRequest);
            fail("expected parseContent failure");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("Failed to parse request body"));
        }
    }

    public void testParseSearchScrollRequestWithUnknownParamThrowsException() throws Exception {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        BytesReference invalidContent = XContentFactory.jsonBuilder().startObject()
            .field("scroll_id", "value_2")
            .field("unknown", "keyword")
            .endObject().bytes();

        try {
            RestSearchScrollAction.buildFromContent(invalidContent, searchScrollRequest);
            fail("expected parseContent failure");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
        }
    }

    public void testParseClearScrollRequest() throws Exception {
        BytesReference content = XContentFactory.jsonBuilder().startObject()
            .array("scroll_id", "value_1", "value_2")
            .endObject().bytes();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        RestClearScrollAction.buildFromContent(content, clearScrollRequest);
        assertThat(clearScrollRequest.scrollIds(), contains("value_1", "value_2"));
    }

    public void testParseClearScrollRequestWithInvalidJsonThrowsException() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        try {
            RestClearScrollAction.buildFromContent(new BytesArray("{invalid_json}"), clearScrollRequest);
            fail("expected parseContent failure");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("Failed to parse request body"));
        }
    }

    public void testParseClearScrollRequestWithUnknownParamThrowsException() throws Exception {
        BytesReference invalidContent = XContentFactory.jsonBuilder().startObject()
            .array("scroll_id", "value_1", "value_2")
            .field("unknown", "keyword")
            .endObject().bytes();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        try {
            RestClearScrollAction.buildFromContent(invalidContent, clearScrollRequest);
            fail("expected parseContent failure");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
        }
    }

    public void testCloseAndReopenOrDeleteWithActiveScroll() throws IOException {
        createIndex("test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject()).execute().actionGet();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();
        long counter = 0;
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
        assertThat(searchResponse.getHits().hits().length, equalTo(35));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(((Number) hit.sortValues()[0]).longValue(), equalTo(counter++));
        }
        if (randomBoolean()) {
            client().admin().indices().prepareClose("test").get();
            client().admin().indices().prepareOpen("test").get();
            ensureGreen("test");
        } else {
            client().admin().indices().prepareDelete("test").get();
        }
    }

    private void assertToXContentResponse(ClearScrollResponse response, boolean succeed, int numFreed) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        BytesReference bytesReference = builder.bytes();
        Map<String, Object> map;
        try (XContentParser parser = XContentFactory.xContent(bytesReference).createParser(bytesReference)) {
            map = parser.map();
        }

        assertThat(map.get("succeeded"), is(succeed));
        assertThat(map.get("num_freed"), equalTo(numFreed));
    }
}
