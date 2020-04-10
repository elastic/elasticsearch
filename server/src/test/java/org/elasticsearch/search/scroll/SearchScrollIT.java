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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for scrolling.
 */
public class SearchScrollIT extends ESIntegTestCase {
    @After
    public void cleanup() throws Exception {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull("*"))
            .setTransientSettings(Settings.builder().putNull("*")));
    }

    public void testSimpleScrollQueryThenFetch() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("field", i).endObject()).get();
        }

        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .get();
        try {
            long counter = 0;

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testSimpleScrollQueryThenFetchSmallSizeUnevenDistribution() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 100; i++) {
            String routing = "0";
            if (i > 90) {
                routing = "1";
            } else if (i > 60) {
                routing = "2";
            }
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", i).setRouting(routing).get();
        }

        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .setSize(3)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .get();
        try {
            long counter = 0;

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(3));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            for (int i = 0; i < 32; i++) {
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(2))
                        .get();

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
                assertThat(searchResponse.getHits().getHits().length, equalTo(3));
                for (SearchHit hit : searchResponse.getHits()) {
                    assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
                }
            }

            // and now, the last one is one
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            // a the last is zero
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(0));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testScrollAndUpdateIndex() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 5)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 500; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("user", "kimchy").field("postDate", System.currentTimeMillis())
                    .field("message", "test").endObject()).get();
        }

        client().admin().indices().prepareRefresh().get();

        assertThat(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get().getHits().getTotalHits().value, equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                equalTo(500L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                equalTo(0L));
        assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                equalTo(0L));

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("user:kimchy"))
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("postDate", SortOrder.ASC)
                .get();
        try {
            do {
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    Map<String, Object> map = searchHit.getSourceAsMap();
                    map.put("message", "update");
                    client().prepareIndex("test").setId(searchHit.getId()).setSource(map).get();
                }
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2))
                        .get();
            } while (searchResponse.getHits().getHits().length > 0);

            client().admin().indices().prepareRefresh().get();
            assertThat(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get().getHits().getTotalHits().value, equalTo(500L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                    equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                    equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                    equalTo(500L));
            assertThat(client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                    equalTo(500L));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    public void testSimpleScrollQueryThenFetch_clearScrollIds() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject())
            .get();
        }

        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse1 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .get();

        SearchResponse searchResponse2 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .get();

        long counter1 = 0;
        long counter2 = 0;

        assertThat(searchResponse1.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse1.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse2.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter2++));
        }

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();

        assertThat(searchResponse1.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse1.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse2.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .addScrollId(searchResponse1.getScrollId())
                .addScrollId(searchResponse2.getScrollId())
                .get();
        assertThat(clearResponse.isSucceeded(), is(true));
        assertThat(clearResponse.getNumFreed(), greaterThan(0));
        assertThat(clearResponse.status(), equalTo(RestStatus.OK));
        assertToXContentResponse(clearResponse, true, clearResponse.getNumFreed());

        assertRequestBuilderThrows(client().prepareSearchScroll(searchResponse1.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)),
                RestStatus.NOT_FOUND);
        assertRequestBuilderThrows(client().prepareSearchScroll(searchResponse2.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)),
                RestStatus.NOT_FOUND);
    }

    public void testClearNonExistentScrollId() throws Exception {
        createIndex("idx");
        ClearScrollResponse response = client().prepareClearScroll()
                .addScrollId("DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAABFnRtLWMyRzBqUUQyNk1uM0xDTjJ4S0EAAAAAAAAAARYzNkhxbWFTYVFVNmgxTGQyYUZVYV9nAA" +
                        "AAAAAAAAEWdVcxNWZmRGZSVFN2V0xMUGF2NGx1Zw==")
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
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(jsonBuilder().startObject().field("field", i).endObject())
            .get();
        }

        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse1 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .get();

        SearchResponse searchResponse2 = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort("field", SortOrder.ASC)
                .get();

        long counter1 = 0;
        long counter2 = 0;

        assertThat(searchResponse1.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse1.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse2.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter2++));
        }

        searchResponse1 = client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();

        searchResponse2 = client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();

        assertThat(searchResponse1.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse1.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse1.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter1++));
        }

        assertThat(searchResponse2.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse2.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse2.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter2++));
        }

        ClearScrollResponse clearResponse = client().prepareClearScroll().addScrollId("_all")
                .get();
        assertThat(clearResponse.isSucceeded(), is(true));
        assertThat(clearResponse.getNumFreed(), greaterThan(0));
        assertThat(clearResponse.status(), equalTo(RestStatus.OK));
        assertToXContentResponse(clearResponse, true, clearResponse.getNumFreed());

        assertRequestBuilderThrows(internalCluster().client().prepareSearchScroll(searchResponse1.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
        assertRequestBuilderThrows(internalCluster().client().prepareSearchScroll(searchResponse2.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2)), RestStatus.NOT_FOUND);
    }

    /**
     * Tests that we use an optimization shrinking the batch to the size of the shard. Thus the Integer.MAX_VALUE window doesn't OOM us.
     */
    public void testDeepScrollingDoesNotBlowUp() throws Exception {
        client().prepareIndex("index").setId("1")
                .setSource("field", "value")
                .setRefreshPolicy(IMMEDIATE)
                .execute().get();
        /*
         * Disable the max result window setting for this test because it'll reject the search's unreasonable batch size. We want
         * unreasonable batch sizes to just OOM.
         */
        client().admin().indices().prepareUpdateSettings("index")
                .setSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), Integer.MAX_VALUE)).get();

        for (SearchType searchType : SearchType.values()) {
            SearchRequestBuilder builder = client().prepareSearch("index")
                    .setSearchType(searchType)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(Integer.MAX_VALUE)
                    .setScroll("1m");

            SearchResponse response = builder.get();
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
        client().prepareIndex("index").setId("1").setSource("field", "value").execute().get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));

        ClearScrollResponse clearScrollResponse = client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
        assertThat(clearScrollResponse.isSucceeded(), is(true));

        assertRequestBuilderThrows(internalCluster().client().prepareSearchScroll(searchResponse.getScrollId()), RestStatus.NOT_FOUND);
    }

    public void testStringSortMissingAscTerminates() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("no_field", "type=keyword", "some_field", "type=keyword"));
        client().prepareIndex("test").setId("1").setSource("some_field", "test").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")

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

                .addSort(new FieldSortBuilder("no_field").order(SortOrder.ASC).missing("_first"))
                .setScroll("1m")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        response = client().prepareSearchScroll(response.getScrollId()).get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getHits().length, equalTo(0));
    }

    public void testCloseAndReopenOrDeleteWithActiveScroll() {
        createIndex("test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", i).get();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(35)
                .setScroll(TimeValue.timeValueMinutes(2))
                .addSort("field", SortOrder.ASC)
                .get();
        long counter = 0;
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(35));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
        }
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
            assertAcked(client().admin().indices().prepareOpen("test"));
            ensureGreen("test");
        } else {
            assertAcked(client().admin().indices().prepareDelete("test"));
        }
    }

    public void testScrollInvalidDefaultKeepAlive() throws IOException {
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () ->
            client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.max_keep_alive", "1m").put("search.default_keep_alive", "2m")).get
            ());
        assertThat(exc.getMessage(), containsString("was (2m > 1m)"));

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "5m").put("search.max_keep_alive", "5m")).get());

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "2m")).get());

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.max_keep_alive", "2m")).get());


        exc = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "3m")).get());
        assertThat(exc.getMessage(), containsString("was (3m > 2m)"));

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "1m")).get());

        exc = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.max_keep_alive", "30s")).get());
        assertThat(exc.getMessage(), containsString("was (1m > 30s)"));
    }

    public void testInvalidScrollKeepAlive() throws IOException {
        createIndex("test");
        for (int i = 0; i < 2; i++) {
            client().prepareIndex("test").setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject()).get();
        }
        refresh();
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "5m").put("search.max_keep_alive", "5m")).get());

        Exception exc = expectThrows(Exception.class,
            () -> client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(1)
                .setScroll(TimeValue.timeValueHours(2))
                .get());
        IllegalArgumentException illegalArgumentException =
            (IllegalArgumentException) ExceptionsHelper.unwrap(exc, IllegalArgumentException.class);
        assertNotNull(illegalArgumentException);
        assertThat(illegalArgumentException.getMessage(), containsString("Keep alive for scroll (2h) is too large"));

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(5))
            .get();
        assertNotNull(searchResponse.getScrollId());
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        exc = expectThrows(Exception.class,
            () -> client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueHours(3)).get());
        illegalArgumentException =
            (IllegalArgumentException) ExceptionsHelper.unwrap(exc, IllegalArgumentException.class);
        assertNotNull(illegalArgumentException);
        assertThat(illegalArgumentException.getMessage(), containsString("Keep alive for scroll (3h) is too large"));
    }

    /**
     * Ensures that we always create and retain search contexts on every target shards for a scroll request
     * regardless whether that query can be written to match_no_docs on some target shards or not.
     */
    public void testScrollRewrittenToMatchNoDocs() {
        final int numShards = randomIntBetween(3, 5);
        assertAcked(
            client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards))
                .setMapping("{\"properties\":{\"created_date\":{\"type\": \"date\", \"format\": \"yyyy-MM-dd\"}}}"));
        client().prepareIndex("test").setId("1").setSource("created_date", "2020-01-01").get();
        client().prepareIndex("test").setId("2").setSource("created_date", "2020-01-02").get();
        client().prepareIndex("test").setId("3").setSource("created_date", "2020-01-03").get();
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse resp = null;
        try {
            int totalHits = 0;
            resp = client().prepareSearch("test")
                .setQuery(new RangeQueryBuilder("created_date").gte("2020-01-02").lte("2020-01-03"))
                .setMaxConcurrentShardRequests(randomIntBetween(1, 3)) // sometimes fan out shard requests one by one
                .setSize(randomIntBetween(1, 2))
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
            assertNoFailures(resp);
            while (resp.getHits().getHits().length > 0) {
                totalHits += resp.getHits().getHits().length;
                resp = client().prepareSearchScroll(resp.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
                assertNoFailures(resp);
            }
            assertThat(totalHits, equalTo(2));
        } finally {
            if (resp != null && resp.getScrollId() != null) {
                client().prepareClearScroll().addScrollId(resp.getScrollId()).get();
            }
        }
    }

    public void testRestartDataNodesDuringScrollSearch() throws Exception {
        final String dataNode = internalCluster().startDataOnlyNode();
        createIndex("demo", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", dataNode)
            .build());
        createIndex("prod", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", dataNode)
            .build());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            index("demo", "demo-" + i, Map.of());
            index("prod", "prod-" + i, Map.of());
        }
        client().admin().indices().prepareRefresh().get();
        SearchResponse respFromDemoIndex = client().prepareSearch("demo")
            .setSize(randomIntBetween(1, 10))
            .setQuery(new MatchAllQueryBuilder()).setScroll(TimeValue.timeValueMinutes(5)).get();

        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback());
        ensureGreen("demo", "prod");
        SearchResponse respFromProdIndex = client().prepareSearch("prod")
            .setSize(randomIntBetween(1, 10))
            .setQuery(new MatchAllQueryBuilder()).setScroll(TimeValue.timeValueMinutes(5)).get();
        assertNoFailures(respFromProdIndex);
        SearchPhaseExecutionException error = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearchScroll(respFromDemoIndex.getScrollId()).get());
        for (ShardSearchFailure shardSearchFailure : error.shardFailures()) {
            assertThat(shardSearchFailure.getCause().getMessage(), containsString("No search context found for id [1]"));
        }
        client().prepareSearchScroll(respFromProdIndex.getScrollId()).get();
    }

    private void assertToXContentResponse(ClearScrollResponse response, boolean succeed, int numFreed) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map.get("succeeded"), is(succeed));
        assertThat(map.get("num_freed"), equalTo(numFreed));
    }
}
