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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singleton;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SearchFieldsTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    public void testStoredFields() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                // _timestamp is randomly enabled via templates but we don't want it here to test stored fields behaviour
                .startObject("_timestamp").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("field1").field("type", "string").field("store", "yes").endObject()
                .startObject("field2").field("type", "string").field("store", "no").endObject()
                .startObject("field3").field("type", "string").field("store", "yes").endObject()
                .endObject().endObject().endObject().string();

        client().admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .field("field3", "value3")
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("field1").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));

        // field2 is not stored, check that it is not extracted from source.
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("field2").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(0));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field2"), nullValue());

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("field3").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("*3").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));


        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("*3").addField("field1").addField("field2").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));


        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("field*").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("f*3").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("*").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).source(), nullValue());
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addField("*").addField("_source").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).source(), notNullValue());
        assertThat(searchResponse.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));
    }

    public void testScriptDocAndFields() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num1").field("type", "double").field("store", "yes").endObject()
                .endObject().endObject().endObject().string();

        client().admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).field("date", "1970-01-01T00:00:00").endObject())
                .execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).field("date", "1970-01-01T00:00:25").endObject())
                .execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).field("date", "1970-01-01T00:02:00").endObject())
                .execute().actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running doc['num1'].value");
        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", new Script("doc['num1'].value"))
                .addScriptField("sNum1_field", new Script("_fields['num1'].value"))
                .addScriptField("date1", new Script("doc['date'].date.millis"))
                .execute().actionGet();

        assertNoFailures(response);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getAt(0).isSourceEmpty(), equalTo(true));
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        Set<String> fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1_field").values().get(0), equalTo(1.0));
        assertThat((Long) response.getHits().getAt(0).fields().get("date1").values().get(0), equalTo(0l));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));
        fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1_field").values().get(0), equalTo(2.0));
        assertThat((Long) response.getHits().getAt(1).fields().get("date1").values().get(0), equalTo(25000l));
        assertThat(response.getHits().getAt(2).id(), equalTo("3"));
        fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
        assertThat((Double) response.getHits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
        assertThat((Double) response.getHits().getAt(2).fields().get("sNum1_field").values().get(0), equalTo(3.0));
        assertThat((Long) response.getHits().getAt(2).fields().get("date1").values().get(0), equalTo(120000l));

        logger.info("running doc['num1'].value * factor");
        Map<String, Object> params = MapBuilder.<String, Object>newMapBuilder().put("factor", 2.0).map();
        response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", new Script("doc['num1'].value * factor", ScriptType.INLINE, null, params))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(singleton("sNum1")));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));
        fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(singleton("sNum1")));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(4.0));
        assertThat(response.getHits().getAt(2).id(), equalTo("3"));
        fields = new HashSet<>(response.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(singleton("sNum1")));
        assertThat((Double) response.getHits().getAt(2).fields().get("sNum1").values().get(0), equalTo(6.0));
    }

    public void testUidBasedScriptFields() throws Exception {
        prepareCreate("test").addMapping("type1", "num1", "type=long").execute().actionGet();
        ensureYellow();

        int numDocs = randomIntBetween(1, 30);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("num1", i).endObject());
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery()).addSort("num1", SortOrder.ASC).setSize(numDocs)
                .addScriptField("uid", new Script("_fields._uid.value")).get();

        assertNoFailures(response);

        assertThat(response.getHits().totalHits(), equalTo((long)numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            Set<String> fields = new HashSet<>(response.getHits().getAt(i).fields().keySet());
            fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
            assertThat(fields, equalTo(singleton("uid")));
            assertThat((String)response.getHits().getAt(i).fields().get("uid").value(), equalTo("type1#" + Integer.toString(i)));
        }

        response = client().prepareSearch()
                .setQuery(matchAllQuery()).addSort("num1", SortOrder.ASC).setSize(numDocs)
                .addScriptField("id", new Script("_fields._id.value")).get();

        assertNoFailures(response);

        assertThat(response.getHits().totalHits(), equalTo((long)numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            Set<String> fields = new HashSet<>(response.getHits().getAt(i).fields().keySet());
            fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
            assertThat(fields, equalTo(singleton("id")));
            assertThat((String)response.getHits().getAt(i).fields().get("id").value(), equalTo(Integer.toString(i)));
        }

        response = client().prepareSearch()
                .setQuery(matchAllQuery()).addSort("num1", SortOrder.ASC).setSize(numDocs)
                .addScriptField("type", new Script("_fields._type.value")).get();

        assertNoFailures(response);

        assertThat(response.getHits().totalHits(), equalTo((long)numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            Set<String> fields = new HashSet<>(response.getHits().getAt(i).fields().keySet());
            fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
            assertThat(fields, equalTo(singleton("type")));
            assertThat((String)response.getHits().getAt(i).fields().get("type").value(), equalTo("type1"));
        }

        response = client().prepareSearch()
                .setQuery(matchAllQuery()).addSort("num1", SortOrder.ASC).setSize(numDocs)
                .addScriptField("id", new Script("_fields._id.value")).addScriptField("uid", new Script("_fields._uid.value"))
                .addScriptField("type", new Script("_fields._type.value")).get();

        assertNoFailures(response);

        assertThat(response.getHits().totalHits(), equalTo((long)numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            Set<String> fields = new HashSet<>(response.getHits().getAt(i).fields().keySet());
            fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
            assertThat(fields, equalTo(newHashSet("uid", "type", "id")));
            assertThat((String)response.getHits().getAt(i).fields().get("uid").value(), equalTo("type1#" + Integer.toString(i)));
            assertThat((String)response.getHits().getAt(i).fields().get("type").value(), equalTo("type1"));
            assertThat((String)response.getHits().getAt(i).fields().get("id").value(), equalTo(Integer.toString(i)));
        }
    }

    public void testScriptFieldUsingSource() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .startObject("obj1").field("test", "something").endObject()
                        .startObject("obj2").startArray("arr2").value("arr_value1").value("arr_value2").endArray().endObject()
                        .startArray("arr3").startObject().field("arr3_field1", "arr3_value1").endObject().endArray()
                        .endObject())
                .execute().actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
.addScriptField("s_obj1", new Script("_source.obj1"))
                .addScriptField("s_obj1_test", new Script("_source.obj1.test")).addScriptField("s_obj2", new Script("_source.obj2"))
                .addScriptField("s_obj2_arr2", new Script("_source.obj2.arr2")).addScriptField("s_arr3", new Script("_source.arr3"))
                .execute().actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        Map<String, Object> sObj1 = response.getHits().getAt(0).field("s_obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.getHits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        Map<String, Object> sObj2 = response.getHits().getAt(0).field("s_obj2").value();
        List sObj2Arr2 = (List) sObj2.get("arr2");
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        sObj2Arr2 = response.getHits().getAt(0).field("s_obj2_arr2").values();
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        List sObj2Arr3 = response.getHits().getAt(0).field("s_arr3").values();
        assertThat(((Map) sObj2Arr3.get(0)).get("arr3_field1").toString(), equalTo("arr3_value1"));
    }

    public void testPartialFields() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();


        client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .field("field1", "value1")
                .startObject("obj1")
                .startArray("arr1")
                .startObject().startObject("obj2").field("field2", "value21").endObject().endObject()
                .startObject().startObject("obj2").field("field2", "value22").endObject().endObject()
                .endArray()
                .endObject()
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    public void testStoredFieldsWithoutSource() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_source").field("enabled", false).endObject().startObject("properties")
                .startObject("byte_field").field("type", "byte").field("store", "yes").endObject()
                .startObject("short_field").field("type", "short").field("store", "yes").endObject()
                .startObject("integer_field").field("type", "integer").field("store", "yes").endObject()
                .startObject("long_field").field("type", "long").field("store", "yes").endObject()
                .startObject("float_field").field("type", "float").field("store", "yes").endObject()
                .startObject("double_field").field("type", "double").field("store", "yes").endObject()
                .startObject("date_field").field("type", "date").field("store", "yes").endObject()
                .startObject("boolean_field").field("type", "boolean").field("store", "yes").endObject()
                .startObject("binary_field").field("type", "binary").field("store", "yes").endObject()
                .endObject().endObject().endObject().string();

        client().admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("byte_field", (byte) 1)
                .field("short_field", (short) 2)
                .field("integer_field", 3)
                .field("long_field", 4l)
                .field("float_field", 5.0f)
                .field("double_field", 6.0d)
                .field("date_field", Joda.forPattern("dateOptionalTime").printer().print(new DateTime(2012, 3, 22, 0, 0, DateTimeZone.UTC)))
                .field("boolean_field", true)
                .field("binary_field", Base64.encodeBytes("testing text".getBytes("UTF8")))
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addField("byte_field")
                .addField("short_field")
                .addField("integer_field")
                .addField("long_field")
                .addField("float_field")
                .addField("double_field")
                .addField("date_field")
                .addField("boolean_field")
                .addField("binary_field")
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        Set<String> fields = new HashSet<>(searchResponse.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(newHashSet("byte_field", "short_field", "integer_field", "long_field",
                "float_field", "double_field", "date_field", "boolean_field", "binary_field")));


        assertThat(searchResponse.getHits().getAt(0).fields().get("byte_field").value().toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("short_field").value().toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("integer_field").value(), equalTo((Object) 3));
        assertThat(searchResponse.getHits().getAt(0).fields().get("long_field").value(), equalTo((Object) 4l));
        assertThat(searchResponse.getHits().getAt(0).fields().get("float_field").value(), equalTo((Object) 5.0f));
        assertThat(searchResponse.getHits().getAt(0).fields().get("double_field").value(), equalTo((Object) 6.0d));
        String dateTime = Joda.forPattern("dateOptionalTime").printer().print(new DateTime(2012, 3, 22, 0, 0, DateTimeZone.UTC));
        assertThat(searchResponse.getHits().getAt(0).fields().get("date_field").value(), equalTo((Object) dateTime));
        assertThat(searchResponse.getHits().getAt(0).fields().get("boolean_field").value(), equalTo((Object) Boolean.TRUE));
        assertThat(((BytesReference) searchResponse.getHits().getAt(0).fields().get("binary_field").value()).toBytesArray(), equalTo((BytesReference) new BytesArray("testing text".getBytes("UTF8"))));

    }

    public void testSearchFieldsMetaData() throws Exception {
        client().prepareIndex("my-index", "my-type1", "1")
                .setRouting("1")
                .setSource(jsonBuilder().startObject().field("field1", "value").endObject())
                .setRefresh(true)
                .get();

        SearchResponse searchResponse = client().prepareSearch("my-index")
                .setTypes("my-type1")
                .addField("field1").addField("_routing")
                .get();

        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).field("field1"), nullValue());
        assertThat(searchResponse.getHits().getAt(0).field("_routing").isMetadataField(), equalTo(true));
        assertThat(searchResponse.getHits().getAt(0).field("_routing").getValue().toString(), equalTo("1"));
    }

    public void testSearchFieldsNonLeafField() throws Exception {
        client().prepareIndex("my-index", "my-type1", "1")
                .setSource(jsonBuilder().startObject().startObject("field1").field("field2", "value1").endObject().endObject())
                .setRefresh(true)
                .get();

        assertFailures(client().prepareSearch("my-index").setTypes("my-type1").addField("field1"),
                RestStatus.BAD_REQUEST,
                containsString("field [field1] isn't a leaf field"));
    }

    public void testGetFieldsComplexField() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .setSettings(Settings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("my-type2", jsonBuilder().startObject().startObject("my-type2").startObject("properties")
                        .startObject("field1").field("type", "object").startObject("properties")
                        .startObject("field2").field("type", "object").startObject("properties")
                        .startObject("field3").field("type", "object").startObject("properties")
                        .startObject("field4").field("type", "string").field("store", "yes")
                        .endObject().endObject()
                        .endObject().endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .get();

        BytesReference source = jsonBuilder().startObject()
                .startArray("field1")
                    .startObject()
                        .startObject("field2")
                            .startArray("field3")
                                .startObject()
                                    .field("field4", "value1")
                                .endObject()
                            .endArray()
                        .endObject()
                    .endObject()
                    .startObject()
                        .startObject("field2")
                            .startArray("field3")
                                .startObject()
                                    .field("field4", "value2")
                                .endObject()
                            .endArray()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().bytes();

        client().prepareIndex("my-index", "my-type1", "1").setSource(source).get();
        client().prepareIndex("my-index", "my-type2", "1").setRefresh(true).setSource(source).get();


        String field = "field1.field2.field3.field4";
        SearchResponse searchResponse = client().prepareSearch("my-index").setTypes("my-type1").addField(field).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).field(field).isMetadataField(), equalTo(false));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().get(1).toString(), equalTo("value2"));

        searchResponse = client().prepareSearch("my-index").setTypes("my-type2").addField(field).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).field(field).isMetadataField(), equalTo(false));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().size(), equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).field(field).getValues().get(1).toString(), equalTo("value2"));
    }

    // see #8203
    public void testSingleValueFieldDatatField() throws ExecutionException, InterruptedException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("test_field", "foobar"));
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").setSource(
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).fieldDataField("test_field")).get();
        assertHitCount(searchResponse, 1);
        Map<String,SearchHitField> fields = searchResponse.getHits().getHits()[0].getFields();
        assertThat((String)fields.get("test_field").value(), equalTo("foobar"));
    }

    public void testFieldsPulledFromFieldData() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_source").field("enabled", false).endObject().startObject("properties")
                .startObject("string_field").field("type", "string").endObject()
                .startObject("byte_field").field("type", "byte").endObject()
                .startObject("short_field").field("type", "short").endObject()
                .startObject("integer_field").field("type", "integer").endObject()
                .startObject("long_field").field("type", "long").endObject()
                .startObject("float_field").field("type", "float").endObject()
                .startObject("double_field").field("type", "double").endObject()
                .startObject("date_field").field("type", "date").endObject()
                .startObject("boolean_field").field("type", "boolean").endObject()
                .startObject("binary_field").field("type", "binary").endObject()
                .endObject().endObject().endObject().string();

        client().admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("string_field", "foo")
                .field("byte_field", (byte) 1)
                .field("short_field", (short) 2)
                .field("integer_field", 3)
                .field("long_field", 4l)
                .field("float_field", 5.0f)
                .field("double_field", 6.0d)
                .field("date_field", Joda.forPattern("dateOptionalTime").printer().print(new DateTime(2012, 3, 22, 0, 0, DateTimeZone.UTC)))
                .field("boolean_field", true)
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchRequestBuilder builder = client().prepareSearch().setQuery(matchAllQuery())
                .addFieldDataField("string_field")
                .addFieldDataField("byte_field")
                .addFieldDataField("short_field")
                .addFieldDataField("integer_field")
                .addFieldDataField("long_field")
                .addFieldDataField("float_field")
                .addFieldDataField("double_field")
                .addFieldDataField("date_field")
                .addFieldDataField("boolean_field");
        SearchResponse searchResponse = builder.execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        Set<String> fields = new HashSet<>(searchResponse.getHits().getAt(0).fields().keySet());
        fields.remove(TimestampFieldMapper.NAME); // randomly enabled via templates
        assertThat(fields, equalTo(newHashSet("byte_field", "short_field", "integer_field", "long_field",
                "float_field", "double_field", "date_field", "boolean_field", "string_field")));

        assertThat(searchResponse.getHits().getAt(0).fields().get("byte_field").value().toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("short_field").value().toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).fields().get("integer_field").value(), equalTo((Object) 3l));
        assertThat(searchResponse.getHits().getAt(0).fields().get("long_field").value(), equalTo((Object) 4l));
        assertThat(searchResponse.getHits().getAt(0).fields().get("float_field").value(), equalTo((Object) 5.0));
        assertThat(searchResponse.getHits().getAt(0).fields().get("double_field").value(), equalTo((Object) 6.0d));
        assertThat(searchResponse.getHits().getAt(0).fields().get("date_field").value(), equalTo((Object) 1332374400000L));
        assertThat(searchResponse.getHits().getAt(0).fields().get("boolean_field").value(), equalTo((Object) 1L));

    }

    public void testScriptFields() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type",
                "s", "type=string,index=not_analyzed",
                "l", "type=long",
                "d", "type=double",
                "ms", "type=string,index=not_analyzed",
                "ml", "type=long",
                "md", "type=double").get());
        final int numDocs = randomIntBetween(3, 8);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            reqs.add(client().prepareIndex("index", "type", Integer.toString(i)).setSource(
                    "s", Integer.toString(i),
                    "ms", new String[] {Integer.toString(i), Integer.toString(i+1)},
                    "l", i,
                    "ml", new long[] {i, i+1},
                    "d", i,
                    "md", new double[] {i, i+1}));
        }
        indexRandom(true, reqs);
        ensureSearchable();
        SearchRequestBuilder req = client().prepareSearch("index");
        for (String field : Arrays.asList("s", "ms", "l", "ml", "d", "md")) {
            req.addScriptField(field, new Script("doc['" + field + "'].values"));
        }
        SearchResponse resp = req.get();
        assertSearchResponse(resp);
        for (SearchHit hit : resp.getHits().getHits()) {
            final int id = Integer.parseInt(hit.getId());
            Map<String, SearchHitField> fields = hit.getFields();
            assertThat(fields.get("s").getValues(), equalTo(Collections.<Object> singletonList(Integer.toString(id))));
            assertThat(fields.get("l").getValues(), equalTo(Collections.<Object> singletonList((long) id)));
            assertThat(fields.get("d").getValues(), equalTo(Collections.<Object> singletonList((double) id)));
            assertThat(fields.get("ms").getValues(), equalTo(Arrays.<Object> asList(Integer.toString(id), Integer.toString(id + 1))));
            assertThat(fields.get("ml").getValues(), equalTo(Arrays.<Object> asList((long) id, id + 1L)));
            assertThat(fields.get("md").getValues(), equalTo(Arrays.<Object> asList((double) id, id + 1d)));
        }
    }

    public void testLoadMetadata() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("my-type1", "_timestamp", "enabled=true", "_ttl", "enabled=true", "_parent", "type=parent"));

        indexRandom(true,
                client().prepareIndex("test", "my-type1", "1")
                    .setRouting("1")
                    .setTimestamp("205097")
                    .setTTL(10000000000000L)
                    .setParent("parent_1")
                    .setSource(jsonBuilder().startObject().field("field1", "value").endObject()));

        SearchResponse response = client().prepareSearch("test").addField("field1").get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        Map<String, SearchHitField> fields = response.getHits().getAt(0).getFields();

        assertThat(fields.get("field1"), nullValue());
        assertThat(fields.get("_routing").isMetadataField(), equalTo(true));
        assertThat(fields.get("_routing").getValue().toString(), equalTo("1"));
        assertThat(fields.get("_timestamp").isMetadataField(), equalTo(true));
        assertThat(fields.get("_timestamp").getValue().toString(), equalTo("205097"));
        assertThat(fields.get("_ttl").isMetadataField(), equalTo(true));
        // TODO: _ttl should return the original value, but it does not work today because
        // it would use now() instead of the value of _timestamp to rebase
        // assertThat(fields.get("_ttl").getValue().toString(), equalTo("10000000205097"));
        assertThat(fields.get("_parent").isMetadataField(), equalTo(true));
        assertThat(fields.get("_parent").getValue().toString(), equalTo("parent_1"));
    }
}
