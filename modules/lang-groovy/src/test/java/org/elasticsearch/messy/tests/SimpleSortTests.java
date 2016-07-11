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

package org.elasticsearch.messy.tests;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 */
public class SimpleSortTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(GroovyPlugin.class, InternalSettingsPlugin.class);
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("str_value").field("type", "keyword").endObject()
                        .startObject("boolean_value").field("type", "boolean").endObject()
                        .startObject("byte_value").field("type", "byte").endObject()
                        .startObject("short_value").field("type", "short").endObject()
                        .startObject("integer_value").field("type", "integer").endObject()
                        .startObject("long_value").field("type", "long").endObject()
                        .startObject("float_value").field("type", "float").endObject()
                        .startObject("double_value").field("type", "double").endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            IndexRequestBuilder builder = client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("str_value", new String(new char[]{(char) (97 + i), (char) (97 + i)}))
                    .field("boolean_value", true)
                    .field("byte_value", i)
                    .field("short_value", i)
                    .field("integer_value", i)
                    .field("long_value", i)
                    .field("float_value", 0.1 * i)
                    .field("double_value", 0.1 * i)
                    .endObject());
            builders.add(builder);
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.execute().actionGet();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    client().admin().indices().prepareFlush().execute().actionGet();
                }
            }

        }
        refresh();

        // STRING script
        int size = 1 + random.nextInt(10);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort(new ScriptSortBuilder(new Script("doc['str_value'].value"), ScriptSortType.STRING)).execute().actionGet();
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[] { (char) (97 + i),
                    (char) (97 + i) })));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[] { (char) (97 + (9 - i)),
                    (char) (97 + (9 - i)) })));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        assertNoFailures(searchResponse);
    }

    public void testSortMinValueScript() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("lvalue").field("type", "long").endObject()
                .startObject("dvalue").field("type", "double").endObject()
                .startObject("svalue").field("type", "keyword").endObject()
                .startObject("gvalue").field("type", "geo_point").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            IndexRequestBuilder req = client().prepareIndex("test", "type1", "" + i).setSource(jsonBuilder().startObject()
                    .field("ord", i)
                    .field("svalue", new String[]{"" + i, "" + (i + 1), "" + (i + 2)})
                    .field("lvalue", new long[]{i, i + 1, i + 2})
                    .field("dvalue", new double[]{i, i + 1, i + 2})
                    .startObject("gvalue")
                    .field("lat", (double) i + 1)
                    .field("lon", (double) i)
                    .endObject()
                    .endObject());
            req.execute().actionGet();
        }

        for (int i = 10; i < 20; i++) { // add some docs that don't have values in those fields
            client().prepareIndex("test", "type1", "" + i).setSource(jsonBuilder().startObject()
                    .field("ord", i)
                    .endObject()).execute().actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        // test the long values
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Long.MAX_VALUE; for (v in doc['lvalue'].values){ retval = min(v, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20L));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Long) searchResponse.getHits().getAt(i).field("min").value(), equalTo((long) i));
        }
        // test the double values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Double.MAX_VALUE; for (v in doc['dvalue'].values){ retval = min(v, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20L));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Double) searchResponse.getHits().getAt(i).field("min").value(), equalTo((double) i));
        }

        // test the string values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Integer.MAX_VALUE; for (v in doc['svalue'].values){ retval = min(Integer.parseInt(v), retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20L));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Integer) searchResponse.getHits().getAt(i).field("min").value(), equalTo(i));
        }

        // test the geopoint values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Double.MAX_VALUE; for (v in doc['gvalue'].values){ retval = min(v.lon, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20L));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Double) searchResponse.getHits().getAt(i).field("min").value(), closeTo(i, GeoUtils.TOLERANCE));
        }
    }

    public void testDocumentsWithNullValue() throws Exception {
        // TODO: sort shouldn't fail when sort field is mapped dynamically
        // We have to specify mapping explicitly because by the time search is performed dynamic mapping might not
        // be propagated to all nodes yet and sort operation fail when the sort field is not defined
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("id").field("type", "keyword").endObject()
                .startObject("svalue").field("type", "keyword").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .nullField("svalue")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "3")
                .field("svalue", "bbb")
                .endObject()).execute().actionGet();


        flush();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].values[0]"))
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).field("id").value(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        // a query with docs just with null values
        searchResponse = client().prepareSearch()
                .setQuery(termQuery("id", "2"))
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("2"));
    }

    public void test2920() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
                "test",
                jsonBuilder().startObject().startObject("test").startObject("properties").startObject("value").field("type", "keyword")
                        .endObject().endObject().endObject().endObject()));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "test", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("value", "" + i).endObject()).execute().actionGet();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.scriptSort(new Script("\u0027\u0027"), ScriptSortType.STRING)).setSize(10).execute().actionGet();
        assertNoFailures(searchResponse);
    }
}
