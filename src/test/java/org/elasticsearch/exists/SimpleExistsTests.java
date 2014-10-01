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

package org.elasticsearch.exists;

import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;

public class SimpleExistsTests extends ElasticsearchIntegrationTest {


    @Test
    public void testExistsRandomPreference() throws Exception {
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
            ExistsResponse existsResponse = client().prepareExists().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomPreference).get();
            assertExists(existsResponse, true);
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

        ExistsResponse existsResponse = client().prepareExists()
                .setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7"))).get();

        assertExists(existsResponse, true);

        existsResponse = client().prepareExists().setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.4")).must(rangeQuery("to").gt("192.168.0.11"))).get();

        assertExists(existsResponse, false);
    }

    @Test
    public void simpleIdTests() {
        createIndex("test");

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefresh(true).execute().actionGet();
        // id is not indexed, but lets see that we automatically convert to
        ExistsResponse existsResponse = client().prepareExists().setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
        assertExists(existsResponse, true);

        existsResponse = client().prepareExists().setQuery(QueryBuilders.queryString("_id:XXX1")).execute().actionGet();
        assertExists(existsResponse, true);

        existsResponse = client().prepareExists().setQuery(QueryBuilders.prefixQuery("_id", "XXX")).execute().actionGet();
        assertExists(existsResponse, true);

        existsResponse = client().prepareExists().setQuery(QueryBuilders.queryString("_id:XXX*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertExists(existsResponse, true);
    }

    @Test
    public void simpleDateMathTests() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        ensureGreen();
        refresh();
        ExistsResponse existsResponse = client().prepareExists("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d")).execute().actionGet();
        assertExists(existsResponse, true);

        existsResponse = client().prepareExists("test").setQuery(QueryBuilders.queryString("field:[2010-01-03||+2d TO 2010-01-04||+2d]")).execute().actionGet();
        assertExists(existsResponse, true);
    }

    @Test
    public void simpleNonExistenceTests() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").execute().actionGet();
        ensureGreen();
        refresh();
        ExistsResponse existsResponse = client().prepareExists("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-07||+2d").lte("2010-01-21||+2d")).execute().actionGet();
        assertExists(existsResponse, false);

        existsResponse = client().prepareExists("test").setQuery(QueryBuilders.queryString("_id:XXY*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertExists(existsResponse, false);
    }

}
