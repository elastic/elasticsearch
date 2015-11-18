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

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

public class SimpleSearchIT extends ESIntegTestCase {
    public void testSearchNullIndex() {
        try {
            client().prepareSearch((String) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            client().prepareSearch((String[]) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
            fail();
        } catch (IllegalArgumentException e) {

        }
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
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomPreference).get();
            assertHitCount(searchResponse, 6l);

        }
    }

    public void testSimpleIp() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("from").field("type", "ip").endObject()
                        .startObject("to").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefresh(true).execute().actionGet();

        SearchResponse search = client().prepareSearch()
                .setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7")))
                .execute().actionGet();

        assertHitCount(search, 1l);
    }

    public void testIpCIDR() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("ip").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("ip", "192.168.0.1").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("ip", "192.168.0.2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("ip", "192.168.0.3").execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource("ip", "192.168.1.4").execute().actionGet();
        refresh();

        SearchResponse search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/32")))
                .execute().actionGet();
        assertHitCount(search, 1l);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/24")))
                .execute().actionGet();
        assertHitCount(search, 3l);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/8")))
                .execute().actionGet();
        assertHitCount(search, 4l);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.1.1/24")))
                .execute().actionGet();
        assertHitCount(search, 1l);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0.0.0.0/0")))
                .execute().actionGet();
        assertHitCount(search, 4l);

        search = client().prepareSearch()
                .setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.1.5/32")))
                .execute().actionGet();
        assertHitCount(search, 0l);

        assertFailures(client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0/0/0/0/0"))),
                RestStatus.BAD_REQUEST,
                containsString("not a valid ip address"));
    }

    public void testSimpleId() {
        createIndex("test");

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefresh(true).execute().actionGet();
        // id is not indexed, but lets see that we automatically convert to
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX1")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        // id is not index, but we can automatically support prefix as well
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.prefixQuery("_id", "XXX")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(searchResponse, 1l);
    }

    public void testSimpleDateRange() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        ensureGreen();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d/d")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lte("2010-01-06T02:00")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lt("2010-01-06T02:00")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("2010-01-05T02:00").lt("2010-01-06T02:00")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("field:[2010-01-03||+2d TO 2010-01-04||+2d/d]")).execute().actionGet();
        assertHitCount(searchResponse, 2l);
    }

    public void testLocaleDependentDate() throws Exception {
        assumeFalse("Locals are buggy on JDK9EA", Constants.JRE_IS_MINIMUM_JAVA9 && systemPropertyAsBoolean("tests.security.manager", false));
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        jsonBuilder().startObject()
                                .startObject("type1")
                                .startObject("properties")
                                .startObject("date_field")
                                .field("type", "date")
                                .field("format", "E, d MMM yyyy HH:mm:ss Z")
                                .field("locale", "de")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject()));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", "" + i).setSource("date_field", "Mi, 06 Dez 2000 02:55:00 -0800").execute().actionGet();
            client().prepareIndex("test", "type1", "" + (10 + i)).setSource("date_field", "Do, 07 Dez 2000 02:55:00 -0800").execute().actionGet();
        }

        refresh();
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Do, 07 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(searchResponse, 10l);


            searchResponse = client().prepareSearch("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Fr, 08 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(searchResponse, 20l);

        }
    }

    public void testSimpleTerminateAfterCount() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 1,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
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

        for (int i = 1; i <= max; i++) {
            searchResponse = client().prepareSearch("test")
                    .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
                    .setTerminateAfter(i).execute().actionGet();
            assertHitCount(searchResponse, (long)i);
            assertTrue(searchResponse.isTerminatedEarly());
        }

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
                .setTerminateAfter(2 * max).execute().actionGet();

        assertHitCount(searchResponse, max);
        assertFalse(searchResponse.isTerminatedEarly());
    }

    public void testInsaneFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertWindowFails(client().prepareSearch("idx").setFrom(Integer.MAX_VALUE));
        assertWindowFails(client().prepareSearch("idx").setSize(Integer.MAX_VALUE));
    }

    public void testTooLargeFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertWindowFails(client().prepareSearch("idx").setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW));
        assertWindowFails(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW + 1));
        assertWindowFails(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW)
                .setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW));
    }

    public void testLargeFromAndSizeSucceeds() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertHitCount(client().prepareSearch("idx").setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW - 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW / 2)
                .setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW / 2 - 1).get(), 1);
    }

    public void testTooLargeFromAndSizeOkBySetting() throws Exception {
        prepareCreate("idx").setSettings(DefaultSearchContext.MAX_RESULT_WINDOW, DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 2).get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertHitCount(client().prepareSearch("idx").setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW + 1).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW)
                .setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW).get(), 1);
    }

    public void testTooLargeFromAndSizeOkByDynamicSetting() throws Exception {
        createIndex("idx");
        assertAcked(client().admin().indices().prepareUpdateSettings("idx")
                .setSettings(
                        Settings.builder().put(DefaultSearchContext.MAX_RESULT_WINDOW, DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 2))
                .get());
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertHitCount(client().prepareSearch("idx").setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW + 1).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW)
                .setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW).get(), 1);
    }

    public void testTooLargeFromAndSizeBackwardsCompatibilityRecommendation() throws Exception {
        prepareCreate("idx").setSettings(DefaultSearchContext.MAX_RESULT_WINDOW, Integer.MAX_VALUE).get();
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        assertHitCount(client().prepareSearch("idx").setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 10)
                .setFrom(DefaultSearchContext.Defaults.MAX_RESULT_WINDOW * 10).get(), 1);
    }

    private void assertWindowFails(SearchRequestBuilder search) {
        try {
            search.get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("Result window is too large, from + size must be less than or equal to: ["
                    + DefaultSearchContext.Defaults.MAX_RESULT_WINDOW));
            assertThat(e.toString(), containsString("See the scroll api for a more efficient way to request large data sets"));
        }
    }
}
