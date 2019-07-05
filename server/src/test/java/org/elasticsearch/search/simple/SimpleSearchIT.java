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

package org.elasticsearch.search.simple;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SimpleSearchIT extends ESIntegTestCase {

    public void testSearchNullIndex() {
        expectThrows(NullPointerException.class,
                () -> client().prepareSearch((String) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).get());

        expectThrows(NullPointerException.class,
                () -> client().prepareSearch((String[]) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).get());

    }

    public void testSearchRandomPreference() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("field", "value"),
                client().prepareIndex("test", "type", "2").setSource("field", "value"),
                client().prepareIndex("test", "type", "3").setSource("field", "value"),
                client().prepareIndex("test", "type", "4").setSource("field", "value"),
                client().prepareIndex("test", "type", "5").setSource("field", "value"),
                client().prepareIndex("test", "type", "6").setSource("field", "value"));

        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            String randomPreference = randomUnicodeOfLengthBetween(0, 4);
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery())
                    .setPreference(randomPreference).get();
            assertHitCount(searchResponse, 6L);

        }
    }

    public void testSimpleIp() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("from").field("type", "ip").endObject()
                        .startObject("to").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .get();

        client().prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse search = client().prepareSearch()
                .setQuery(boolQuery().must(rangeQuery("from").lte("192.168.0.7")).must(rangeQuery("to").gte("192.168.0.7")))
                .get();

        assertHitCount(search, 1L);
    }

    public void testIpCidr() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("ip").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .get();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("ip", "192.168.0.1").get();
        client().prepareIndex("test", "type1", "2").setSource("ip", "192.168.0.2").get();
        client().prepareIndex("test", "type1", "3").setSource("ip", "192.168.0.3").get();
        client().prepareIndex("test", "type1", "4").setSource("ip", "192.168.1.4").get();
        client().prepareIndex("test", "type1", "5").setSource("ip", "2001:db8::ff00:42:8329").get();
        refresh();

        SearchResponse search = client().prepareSearch()
            .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1")))
            .get();
        assertHitCount(search, 1L);

        search = client().prepareSearch()
            .setQuery(queryStringQuery("ip: 192.168.0.1"))
            .get();
        assertHitCount(search, 1L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/32")))
                .get();
        assertHitCount(search, 1L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.0/24")))
                .get();
        assertHitCount(search, 3L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.0.0.0/8")))
                .get();
        assertHitCount(search, 4L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0.0.0.0/0")))
                .get();
        assertHitCount(search, 4L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::ff00:42:8329/128")))
                .get();
        assertHitCount(search, 1L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::/64")))
                .get();
        assertHitCount(search, 1L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "::/0")))
                .get();
        assertHitCount(search, 5L);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.1.5/32")))
                .get();
        assertHitCount(search, 0L);

        assertFailures(client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0/0/0/0/0"))),
                RestStatus.BAD_REQUEST,
                containsString("Expected [ip/prefix] but was [0/0/0/0/0]"));
    }

    public void testSimpleId() {
        createIndex("test");

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        // id is not indexed, but lets see that we automatically convert to
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.termQuery("_id", "XXX1")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX1")).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testSimpleDateRange() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").get();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").get();
        ensureGreen();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d")
                .lte("2010-01-04||+2d/d")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00")
                .lte("2010-01-06T02:00")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00")
                .lt("2010-01-06T02:00")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("2010-01-05T02:00")
                .lt("2010-01-06T02:00")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test").setQuery(
                QueryBuilders.queryStringQuery("field:[2010-01-03||+2d TO 2010-01-04||+2d/d]")).get();
        assertHitCount(searchResponse, 2L);
    }

    public void testSimpleTerminateAfterCount() throws Exception {
        prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = 1; i <= max; i++) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test", "type1", id).setSource("field", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        SearchResponse searchResponse;
        for (int i = 1; i < max; i++) {
            searchResponse = client().prepareSearch("test")
                    .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
                    .setTerminateAfter(i).get();
            assertHitCount(searchResponse, i);
            assertTrue(searchResponse.isTerminatedEarly());
        }

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
                .setTerminateAfter(2 * max).get();

        assertHitCount(searchResponse, max);
        assertFalse(searchResponse.isTerminatedEarly());
    }

    public void testSimpleIndexSortEarlyTerminate() throws Exception {
        prepareCreate("test")
            .setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.sort.field", "rank")
            )
            .addMapping("type1", "rank", "type=integer")
            .get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = max-1; i >= 0; i--) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test", "type1", id).setSource("rank", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        SearchResponse searchResponse;
        for (int i = 1; i < max; i++) {
            searchResponse = client().prepareSearch("test")
                .addDocValueField("rank")
                .setTrackTotalHits(false)
                .addSort("rank", SortOrder.ASC)
                .setSize(i).get();
            assertNull(searchResponse.getHits().getTotalHits());
            for (int j = 0; j < i; j++) {
                assertThat(searchResponse.getHits().getAt(j).field("rank").getValue(),
                    equalTo((long) j));
            }
        }
    }

    public void testInsaneFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertWindowFails(client().prepareSearch("idx").setFrom(Integer.MAX_VALUE));
        assertWindowFails(client().prepareSearch("idx").setSize(Integer.MAX_VALUE));
    }

    public void testTooLargeFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertWindowFails(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)));
        assertWindowFails(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1));
        assertWindowFails(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)));
    }

    public void testLargeFromAndSizeSucceeds() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) - 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2 - 1).get(), 1);
    }

    public void testTooLargeFromAndSizeOkBySetting() throws Exception {
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2)).get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
    }

    public void testTooLargeFromAndSizeOkByDynamicSetting() throws Exception {
        createIndex("idx");
        assertAcked(client().admin().indices().prepareUpdateSettings("idx")
                .setSettings(
                        Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                                IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2))
                .get());
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
    }

    public void testTooLargeFromAndSizeBackwardsCompatibilityRecommendation() throws Exception {
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), Integer.MAX_VALUE)).get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10).get(), 1);
    }

    public void testTooLargeRescoreWindow() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertRescoreWindowFails(Integer.MAX_VALUE);
        assertRescoreWindowFails(IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY) + 1);
    }

    public void testTooLargeRescoreOkBySetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
            .get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(
                client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
                1);
    }

    public void testTooLargeRescoreOkByResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(
            Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), // Note that this is the RESULT window.
                defaultMaxWindow * 2)).get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(
                client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
                1);
    }

    public void testTooLargeRescoreOkByDynamicSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        assertAcked(client().admin().indices().prepareUpdateSettings("idx")
                .setSettings(
                        Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
                .get());
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(
                client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
                1);
    }

    public void testTooLargeRescoreOkByDynamicResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        assertAcked(client().admin().indices().prepareUpdateSettings("idx")
                .setSettings(
                        // Note that this is the RESULT window
                        Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
                .get());
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        assertHitCount(
                client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
                1);
    }

    public void testQueryNumericFieldWithRegex() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "num", "type=integer"));
        ensureGreen("idx");

        try {
            client().prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", "34")).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException ex) {
            assertThat(ex.getRootCause().getMessage(),
                containsString("Can only use regexp queries on keyword and text fields"));
        }
    }

    public void testTooLongRegexInRegexpQuery() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}", XContentType.JSON));

        int defaultMaxRegexLength = IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY);
        StringBuilder regexp = new StringBuilder(defaultMaxRegexLength);
        while (regexp.length() <= defaultMaxRegexLength) {
            regexp.append("]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[\\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\");
        }
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", regexp.toString())).get());
        assertThat(e.getRootCause().getMessage(), containsString("The length of regex [" +
            regexp.length() + "] used in the Regexp Query request has exceeded " +
            "the allowed maximum of [" + defaultMaxRegexLength + "]. " +
            "This maximum can be set by changing the [" + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey() +
            "] index level setting."));
    }

    private void assertWindowFails(SearchRequestBuilder search) {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> search.get());
        assertThat(e.toString(), containsString("Result window is too large, from + size must be less than or equal to: ["
                + IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)));
        assertThat(e.toString(), containsString("See the scroll api for a more efficient way to request large data sets"));
    }

    private void assertRescoreWindowFails(int windowSize) {
        SearchRequestBuilder search = client().prepareSearch("idx")
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(windowSize));
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> search.get());
        assertThat(e.toString(), containsString("Rescore window [" + windowSize + "] is too large. It must "
                + "be less than [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY)));
        assertThat(e.toString(), containsString(
                "This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey() + "] index level setting."));
    }
}
