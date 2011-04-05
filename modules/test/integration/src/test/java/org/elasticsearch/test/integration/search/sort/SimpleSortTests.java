/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleSortTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        startNode("server1", settings);
        startNode("server2", settings);
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testTrackScores() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .field("ivalue", 100)
                .field("dvalue", 0.1)
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .field("svalue", "bbb")
                .field("ivalue", 200)
                .field("dvalue", 0.2)
                .endObject()).execute().actionGet();

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getMaxScore(), equalTo(Float.NaN));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.getScore(), equalTo(Float.NaN));
        }

        // now check with score tracking
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("svalue", SortOrder.ASC)
                .setTrackScores(true)
                .execute().actionGet();

        assertThat(searchResponse.hits().getMaxScore(), not(equalTo(Float.NaN)));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.getScore(), not(equalTo(Float.NaN)));
        }
    }

    @Test public void testSimpleSortsSingleShard() throws Exception {
        testSimpleSorts(1);
    }

    @Test public void testSimpleSortsTwoShards() throws Exception {
        testSimpleSorts(2);
    }

    @Test public void testSimpleSortsThreeShards() throws Exception {
        testSimpleSorts(3);
    }

    private void testSimpleSorts(int numberOfShards) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", numberOfShards).put("number_of_replicas", 0))
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("str_value").field("type", "string").endObject()
                        .startObject("boolean_value").field("type", "boolean").endObject()
                        .startObject("byte_value").field("type", "byte").endObject()
                        .startObject("short_value").field("type", "short").endObject()
                        .startObject("integer_value").field("type", "integer").endObject()
                        .startObject("long_value").field("type", "long").endObject()
                        .startObject("float_value").field("type", "float").endObject()
                        .startObject("double_value").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("str_value", new String(new char[]{(char) (97 + i), (char) (97 + i)}))
                    .field("boolean_value", true)
                    .field("byte_value", i)
                    .field("short_value", i)
                    .field("integer_value", i)
                    .field("long_value", i)
                    .field("float_value", 0.1 * i)
                    .field("double_value", 0.1 * i)
                    .endObject()).execute().actionGet();
        }

//        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // STRING

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("str_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.hits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[]{(char) (97 + i), (char) (97 + i)})));
        }

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("str_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(searchResponse.hits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[]{(char) (97 + (9 - i)), (char) (97 + (9 - i))})));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // BYTE

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).byteValue(), equalTo((byte) i));
        }

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).byteValue(), equalTo((byte) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // SHORT

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).shortValue(), equalTo((short) i));
        }

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).shortValue(), equalTo((short) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // INTEGER

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("integer_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).intValue(), equalTo((int) i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("integer_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).intValue(), equalTo((int) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // LONG

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("long_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).longValue(), equalTo((long) i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("long_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).longValue(), equalTo((long) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // FLOAT

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // DOUBLE

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_value", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_value", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.hits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
    }

    @Test public void testDocumentsWithNullValue() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .nullField("svalue")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "3")
                .field("svalue", "bbb")
                .endObject()).execute().actionGet();


        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(2).field("id").value(), equalTo("3"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("3"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(2).field("id").value(), equalTo("2"));

        // a query with docs just with null values
        searchResponse = client.prepareSearch()
                .setQuery(termQuery("id", "2"))
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
    }

    @Test public void testSortMissing() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("i_value", -1)
                .field("d_value", -1.1)
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("i_value", 2)
                .field("d_value", 2.2)
                .endObject()).execute().actionGet();

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        logger.info("--> sort with no missing");
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("3"));

        logger.info("--> sort with missing _last");
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_last"))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_first"))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("3"));
    }
}
