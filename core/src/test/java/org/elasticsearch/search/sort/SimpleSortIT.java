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

package org.elasticsearch.search.sort;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.script.ScriptService.ScriptType;
import static org.elasticsearch.search.sort.SortBuilders.scriptSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimpleSortIT extends ESIntegTestCase {

    private static final String DOUBLE_APOSTROPHE = "\u0027\u0027";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomScriptPlugin.class, InternalSettingsPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("doc['str_value'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("str_value")).getValue();
            });

            scripts.put("doc['id'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("id")).getValue();
            });

            scripts.put("doc['id'].values[0]", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("id")).getValues().get(0);
            });

            scripts.put("get min long", vars -> getMinValueScript(vars, Long.MAX_VALUE, "lvalue", l -> (Long) l));
            scripts.put("get min double", vars -> getMinValueScript(vars, Double.MAX_VALUE, "dvalue", d -> (Double) d));
            scripts.put("get min string", vars -> getMinValueScript(vars, Integer.MAX_VALUE, "svalue", s -> Integer.parseInt((String) s)));
            scripts.put("get min geopoint lon", vars -> getMinValueScript(vars, Double.MAX_VALUE, "gvalue", g -> ((GeoPoint) g).getLon()));

            scripts.put(DOUBLE_APOSTROPHE, vars -> DOUBLE_APOSTROPHE);

            return scripts;
        }

        /**
         * Return the minimal value from a set of values.
         */
        @SuppressWarnings("unchecked")
        static <T extends Comparable<T>> T getMinValueScript(Map<String, Object> vars, T initialValue, String fieldName,
                                                             Function<Object, T> converter) {
            T retval = initialValue;
            Map<?, ?> doc = (Map) vars.get("doc");
            ScriptDocValues<?> values = (ScriptDocValues<?>) doc.get(fieldName);
            for (Object v : values.getValues()) {
                T value = converter.apply(v);
                retval = (value.compareTo(retval) < 0) ? value : retval;
            }
            return retval;
        }
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder()
                        .startObject()
                            .startObject("type1")
                                .startObject("properties")
                                    .startObject("str_value")
                                        .field("type", "keyword")
                                    .endObject()
                                    .startObject("boolean_value")
                                        .field("type", "boolean")
                                    .endObject()
                                    .startObject("byte_value")
                                        .field("type", "byte")
                                    .endObject()
                                    .startObject("short_value")
                                        .field("type", "short")
                                    .endObject()
                                    .startObject("integer_value")
                                        .field("type", "integer")
                                    .endObject()
                                    .startObject("long_value")
                                        .field("type", "long")
                                    .endObject()
                                    .startObject("float_value")
                                        .field("type", "float")
                                    .endObject()
                                    .startObject("double_value")
                                        .field("type", "double")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            builders.add(client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder()
                            .startObject()
                                .field("str_value", new String(new char[]{(char) (97 + i), (char) (97 + i)}))
                                .field("boolean_value", true)
                                .field("byte_value", i)
                                .field("short_value", i)
                                .field("integer_value", i)
                                .field("long_value", i)
                                .field("float_value", 0.1 * i)
                                .field("double_value", 0.1 * i)
                            .endObject()
                    ));
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.execute().actionGet();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    client().admin().indices().prepareFlush().get();
                }
            }
        }
        refresh();

        // STRING script
        int size = 1 + random.nextInt(10);

        Script script = new Script("doc['str_value'].value", ScriptType.INLINE, CustomScriptPlugin.NAME, null);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort(new ScriptSortBuilder(script, ScriptSortType.STRING))
                .get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.id(), equalTo(Integer.toString(i)));

            String expected = new String(new char[]{(char) (97 + i), (char) (97 + i)});
            assertThat(searchHit.sortValues()[0].toString(), equalTo(expected));
        }

        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort("str_value", SortOrder.DESC)
                .get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.id(), equalTo(Integer.toString(9 - i)));

            String expected = new String(new char[]{(char) (97 + (9 - i)), (char) (97 + (9 - i))});
            assertThat(searchHit.sortValues()[0].toString(), equalTo(expected));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        assertNoFailures(searchResponse);
    }

    public void testSortMinValueScript() throws IOException {
        String mapping = jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("lvalue")
                                .field("type", "long")
                            .endObject()
                            .startObject("dvalue")
                                .field("type", "double")
                            .endObject()
                            .startObject("svalue")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("gvalue")
                                .field("type", "geo_point")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().string();

        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", "" + i)
                    .setSource(jsonBuilder()
                            .startObject()
                                .field("ord", i)
                                .field("svalue", new String[]{"" + i, "" + (i + 1), "" + (i + 2)})
                                .field("lvalue", new long[]{i, i + 1, i + 2})
                                .field("dvalue", new double[]{i, i + 1, i + 2})
                                .startObject("gvalue")
                                    .field("lat", (double) i + 1)
                                    .field("lon", (double) i)
                                .endObject()
                            .endObject())
                    .get();
        }

        for (int i = 10; i < 20; i++) { // add some docs that don't have values in those fields
            client().prepareIndex("test", "type1", "" + i)
                    .setSource(jsonBuilder()
                            .startObject()
                                .field("ord", i)
                            .endObject())
                    .get();
        }
        client().admin().indices().prepareRefresh("test").get();

        // test the long values
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("get min long", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
                .setSize(10)
                .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").value(), equalTo((long) i));
        }

        // test the double values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("get min double", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
                .setSize(10)
                .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").value(), equalTo((double) i));
        }

        // test the string values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("get min string", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
                .setSize(10)
                .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").value(), equalTo(i));
        }

        // test the geopoint values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("get min geopoint lon", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
                .setSize(10)
                .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").value(), closeTo(i, GeoUtils.TOLERANCE));
        }
    }

    public void testDocumentsWithNullValue() throws Exception {
        // TODO: sort shouldn't fail when sort field is mapped dynamically
        // We have to specify mapping explicitly because by the time search is performed dynamic mapping might not
        // be propagated to all nodes yet and sort operation fail when the sort field is not defined
        String mapping = jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("svalue")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource(jsonBuilder().startObject()
                                            .field("id", "1")
                                            .field("svalue", "aaa")
                                        .endObject())
                .get();

        client().prepareIndex("test", "type1")
                .setSource(jsonBuilder().startObject()
                                            .field("id", "2")
                                            .nullField("svalue")
                                        .endObject())
                .get();

        client().prepareIndex("test", "type1")
                .setSource(jsonBuilder().startObject()
                                            .field("id", "3")
                                            .field("svalue", "bbb")
                                        .endObject())
                .get();

        flush();
        refresh();

        Script scripField = new Script("doc['id'].value", ScriptType.INLINE, CustomScriptPlugin.NAME, null);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", scripField)
                .addSort("svalue", SortOrder.ASC)
                .get();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].values[0]", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .addSort("svalue", SortOrder.ASC)
                .get();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", scripField)
                .addSort("svalue", SortOrder.DESC)
                .get();

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
                .addScriptField("id", scripField)
                .addSort("svalue", SortOrder.DESC)
                .get();

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
        assertAcked(prepareCreate("test")
                .addMapping("test", jsonBuilder()
                        .startObject()
                            .startObject("test")
                                .startObject("properties")
                                    .startObject("value")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "test", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("value", "" + i).endObject()).get();
        }
        refresh();

        Script sortScript = new Script("\u0027\u0027", ScriptType.INLINE, CustomScriptPlugin.NAME, null);
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(scriptSort(sortScript, ScriptSortType.STRING))
                .setSize(10)
                .get();
        assertNoFailures(searchResponse);
    }
}
