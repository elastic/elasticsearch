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

package org.elasticsearch.count.simple;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

public class SimpleCountIT extends ESIntegTestCase {

    @Test
    public void testCountRandomPreference() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("field", "value"),
                client().prepareIndex("test", "type", "2").setSource("field", "value"),
                client().prepareIndex("test", "type", "3").setSource("field", "value"),
                client().prepareIndex("test", "type", "4").setSource("field", "value"),
                client().prepareIndex("test", "type", "5").setSource("field", "value"),
                client().prepareIndex("test", "type", "6").setSource("field", "value"));

        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {

            String randomPreference = randomUnicodeOfLengthBetween(0, 4);
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomPreference).get();
            assertHitCount(countResponse, 6l);
        }
    }

    @Test
    public void simpleIpTests() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("from").field("type", "ip").endObject()
                        .startObject("to").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefresh(true).execute().actionGet();

        CountResponse countResponse = client().prepareCount()
                .setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7")))
                .execute().actionGet();

        assertHitCount(countResponse, 1l);
    }

    @Test
    public void simpleIdTests() {
        createIndex("test");

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefresh(true).execute().actionGet();
        // id is not indexed, but lets see that we automatically convert to
        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.queryStringQuery("_id:XXX1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        // id is not index, but we can automatically support prefix as well
        countResponse = client().prepareCount().setQuery(QueryBuilders.prefixQuery("_id", "XXX")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.queryStringQuery("_id:XXX*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void simpleCountEarlyTerminationTests() throws Exception {
        // set up one shard only to test early termination
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

        // sanity check
        CountResponse countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max)).execute().actionGet();
        assertHitCount(countResponse, max);

        // threshold <= actual count
        for (int i = 1; i <= max; i++) {
            countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max)).setTerminateAfter(i).execute().actionGet();
            assertHitCount(countResponse, i);
            assertTrue(countResponse.terminatedEarly());
        }

        // threshold > actual count
        countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max)).setTerminateAfter(max + randomIntBetween(1, max)).execute().actionGet();
        assertHitCount(countResponse, max);
        assertFalse(countResponse.terminatedEarly());
    }

    @Test
    public void localDependentDateTests() throws Exception {
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
            CountResponse countResponse = client().prepareCount("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Do, 07 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(countResponse, 10l);

            countResponse = client().prepareCount("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Fr, 08 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(countResponse, 20l);
        }
    }

    @Test
    public void testThatNonEpochDatesCanBeSearched() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                    jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("date_field").field("type", "date").field("format", "yyyyMMddHH").endObject().endObject()
                .endObject().endObject()));
        ensureGreen("test");

        XContentBuilder document = jsonBuilder()
                .startObject()
                .field("date_field", "2015060210")
                .endObject();
        assertThat(client().prepareIndex("test", "type1").setSource(document).get().isCreated(), is(true));

        document = jsonBuilder()
                .startObject()
                .field("date_field", "2014060210")
                .endObject();
        assertThat(client().prepareIndex("test", "type1").setSource(document).get().isCreated(), is(true));

        refresh();

        assertHitCount(client().prepareCount("test").get(), 2);

        CountResponse countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("date_field").from("2015010100").to("2015123123")).get();
        assertHitCount(countResponse, 1);

        countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("date_field").from(2015010100).to(2015123123)).get();
        assertHitCount(countResponse, 1);

        countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("date_field").from(2015010100).to(2015123123).timeZone("UTC")).get();
        assertHitCount(countResponse, 1);
    }
}
