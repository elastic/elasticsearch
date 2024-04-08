/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fields;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchFieldsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("doc['num1'].value", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                return num1.getValue();
            });

            scripts.put("doc['num1'].value * factor", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                @SuppressWarnings("unchecked")
                Map<String, Object> params = (Map<String, Object>) vars.get("params");
                Double factor = (Double) params.get("factor");
                return num1.getValue() * factor;
            });

            scripts.put("doc['date'].date.millis", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Dates dates = (ScriptDocValues.Dates) doc.get("date");
                return dates.getValue().toInstant().toEpochMilli();
            });

            scripts.put("doc['date'].date.nanos", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Dates dates = (ScriptDocValues.Dates) doc.get("date");
                return DateUtils.toLong(dates.getValue().toInstant());
            });

            scripts.put("_fields['num1'].value", vars -> fieldsScript(vars, "num1"));
            scripts.put("_fields._uid.value", vars -> fieldsScript(vars, "_uid"));
            scripts.put("_fields._id.value", vars -> fieldsScript(vars, "_id"));
            scripts.put("_fields._type.value", vars -> fieldsScript(vars, "_type"));

            scripts.put("_source.obj1", vars -> sourceScript(vars, "obj1"));
            scripts.put("_source.obj1.test", vars -> sourceScript(vars, "obj1.test"));
            scripts.put("_source.obj1.test", vars -> sourceScript(vars, "obj1.test"));
            scripts.put("_source.obj2", vars -> sourceScript(vars, "obj2"));
            scripts.put("_source.obj2.arr2", vars -> sourceScript(vars, "obj2.arr2"));
            scripts.put("_source.arr3", vars -> sourceScript(vars, "arr3"));

            scripts.put("return null", vars -> null);

            scripts.put("doc['l']", vars -> docScript(vars, "l"));
            scripts.put("doc['ml']", vars -> docScript(vars, "ml"));
            scripts.put("doc['d']", vars -> docScript(vars, "d"));
            scripts.put("doc['md']", vars -> docScript(vars, "md"));
            scripts.put("doc['s']", vars -> docScript(vars, "s"));
            scripts.put("doc['ms']", vars -> docScript(vars, "ms"));

            return scripts;
        }

        static Object fieldsScript(Map<String, Object> vars, String fieldName) {
            Map<?, ?> fields = (Map<?, ?>) vars.get("_fields");
            FieldLookup fieldLookup = (FieldLookup) fields.get(fieldName);
            return fieldLookup.getValue();
        }

        static Object sourceScript(Map<String, Object> vars, String path) {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = ((Supplier<Source>) vars.get("_source")).get().source();
            return XContentMapValues.extractValue(path, source);
        }

        static Object docScript(Map<String, Object> vars, String fieldName) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            ScriptDocValues<?> values = (ScriptDocValues<?>) doc.get(fieldName);
            return values;
        }
    }

    public void testStoredFields() throws Exception {
        createIndex("test");

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field1")
                .field("type", "text")
                .field("store", true)
                .endObject()
                .startObject("field2")
                .field("type", "text")
                .field("store", false)
                .endObject()
                .startObject("field3")
                .field("type", "text")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().preparePutMapping().setSource(mapping, XContentType.JSON).get();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").field("field3", "value3").endObject()
            )
            .get();

        indicesAdmin().prepareRefresh().get();

        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("field1"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().get("field1").getValue().toString(), equalTo("value1"));
        });
        // field2 is not stored, check that it is not extracted from source.
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("field2"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(0));
            assertThat(response.getHits().getAt(0).getFields().get("field2"), nullValue());
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("field3"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("*3"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
        });
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addStoredField("*3").addStoredField("field1").addStoredField("field2"),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
                assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
                assertThat(response.getHits().getAt(0).getFields().get("field1").getValue().toString(), equalTo("value1"));
            }
        );
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("field*"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
            assertThat(response.getHits().getAt(0).getFields().get("field1").getValue().toString(), equalTo("value1"));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("f*3"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("*"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap(), nullValue());
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
            assertThat(response.getHits().getAt(0).getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("*").addStoredField("_source"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap(), notNullValue());
            assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
            assertThat(response.getHits().getAt(0).getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertThat(response.getHits().getAt(0).getFields().get("field3").getValue().toString(), equalTo("value3"));
        });
    }

    public void testScriptDocAndFields() throws Exception {
        createIndex("test");

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("num1")
                .field("type", "double")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().preparePutMapping().setSource(mapping, XContentType.JSON).get();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).field("date", "1970-01-01T00:00:00").endObject()
            )
            .get();
        indicesAdmin().prepareFlush().get();
        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).field("date", "1970-01-01T00:00:25").endObject()
            )
            .get();
        indicesAdmin().prepareFlush().get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).field("date", "1970-01-01T00:02:00").endObject()
            )
            .get();
        indicesAdmin().refresh(new RefreshRequest()).actionGet();

        logger.info("running doc['num1'].value");
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField(
                    "sNum1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap())
                )
                .addScriptField(
                    "sNum1_field",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_fields['num1'].value", Collections.emptyMap())
                )
                .addScriptField(
                    "date1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['date'].date.millis", Collections.emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertFalse(response.getHits().getAt(0).hasSource());
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(1.0));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1_field").getValues().get(0), equalTo(1.0));
                assertThat(response.getHits().getAt(0).getFields().get("date1").getValues().get(0), equalTo(0L));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
                assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
                assertThat(response.getHits().getAt(1).getFields().get("sNum1_field").getValues().get(0), equalTo(2.0));
                assertThat(response.getHits().getAt(1).getFields().get("date1").getValues().get(0), equalTo(25000L));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(newHashSet("sNum1", "sNum1_field", "date1")));
                assertThat(response.getHits().getAt(2).getFields().get("sNum1").getValues().get(0), equalTo(3.0));
                assertThat(response.getHits().getAt(2).getFields().get("sNum1_field").getValues().get(0), equalTo(3.0));
                assertThat(response.getHits().getAt(2).getFields().get("date1").getValues().get(0), equalTo(120000L));
            }
        );
        logger.info("running doc['num1'].value * factor");
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField(
                    "sNum1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value * factor", Map.of("factor", 2.0))
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(singleton("sNum1")));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(singleton("sNum1")));
                assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(4.0));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(fields, equalTo(singleton("sNum1")));
                assertThat(response.getHits().getAt(2).getFields().get("sNum1").getValues().get(0), equalTo(6.0));
            }
        );
    }

    public void testScriptFieldWithNanos() throws Exception {
        createIndex("test");

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("date")
                .field("type", "date_nanos")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().preparePutMapping().setSource(mapping, XContentType.JSON).get();
        String date = "2019-01-31T10:00:00.123456789Z";
        indexRandom(
            true,
            false,
            prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("date", "1970-01-01T00:00:00.000Z").endObject()),
            prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("date", date).endObject())
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort("date", SortOrder.ASC)
                .addScriptField(
                    "date1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['date'].date.millis", Collections.emptyMap())
                )
                .addScriptField(
                    "date2",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['date'].date.nanos", Collections.emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getAt(0).getId(), is("1"));
                assertThat(response.getHits().getAt(0).getFields().get("date1").getValues().get(0), equalTo(0L));
                assertThat(response.getHits().getAt(0).getFields().get("date2").getValues().get(0), equalTo(0L));
                assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(0L));

                assertThat(response.getHits().getAt(1).getId(), is("2"));
                Instant instant = ZonedDateTime.parse(date).toInstant();
                long dateAsNanos = DateUtils.toLong(instant);
                long dateAsMillis = instant.toEpochMilli();
                assertThat(response.getHits().getAt(1).getFields().get("date1").getValues().get(0), equalTo(dateAsMillis));
                assertThat(response.getHits().getAt(1).getFields().get("date2").getValues().get(0), equalTo(dateAsNanos));
                assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo(dateAsNanos));
            }
        );
    }

    public void testIdBasedScriptFields() throws Exception {
        prepareCreate("test").setMapping("num1", "type=long").get();

        int numDocs = randomIntBetween(1, 30);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = prepareIndex("test").setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("num1", i).endObject());
        }
        indexRandom(true, indexRequestBuilders);

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .setSize(numDocs)
                .addScriptField("id", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_fields._id.value", Collections.emptyMap())),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo((long) numDocs));
                for (int i = 0; i < numDocs; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    Set<String> fields = new HashSet<>(response.getHits().getAt(i).getFields().keySet());
                    assertThat(fields, equalTo(singleton("id")));
                    assertThat(response.getHits().getAt(i).getFields().get("id").getValue(), equalTo(Integer.toString(i)));
                }
            }
        );
    }

    public void testScriptFieldUsingSource() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject("obj1")
                    .field("test", "something")
                    .endObject()
                    .startObject("obj2")
                    .startArray("arr2")
                    .value("arr_value1")
                    .value("arr_value2")
                    .endArray()
                    .endObject()
                    .startArray("arr3")
                    .startObject()
                    .field("arr3_field1", "arr3_value1")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        indicesAdmin().refresh(new RefreshRequest()).actionGet();

        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addScriptField("s_obj1", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.obj1", Collections.emptyMap()))
                .addScriptField(
                    "s_obj1_test",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.obj1.test", Collections.emptyMap())
                )
                .addScriptField("s_obj2", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.obj2", Collections.emptyMap()))
                .addScriptField(
                    "s_obj2_arr2",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.obj2.arr2", Collections.emptyMap())
                )
                .addScriptField("s_arr3", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.arr3", Collections.emptyMap())),
            response -> {

                assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

                assertThat(response.getHits().getAt(0).field("s_obj1_test").getValue().toString(), equalTo("something"));

                Map<String, Object> sObj1 = response.getHits().getAt(0).field("s_obj1").getValue();
                assertThat(sObj1.get("test").toString(), equalTo("something"));
                assertThat(response.getHits().getAt(0).field("s_obj1_test").getValue().toString(), equalTo("something"));

                Map<String, Object> sObj2 = response.getHits().getAt(0).field("s_obj2").getValue();
                List<?> sObj2Arr2 = (List<?>) sObj2.get("arr2");
                assertThat(sObj2Arr2.size(), equalTo(2));
                assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
                assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

                sObj2Arr2 = response.getHits().getAt(0).field("s_obj2_arr2").getValues();
                assertThat(sObj2Arr2.size(), equalTo(2));
                assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
                assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

                List<?> sObj2Arr3 = response.getHits().getAt(0).field("s_arr3").getValues();
                assertThat(((Map<?, ?>) sObj2Arr3.get(0)).get("arr3_field1").toString(), equalTo("arr3_value1"));
            }
        );
    }

    public void testScriptFieldsForNullReturn() throws Exception {
        prepareIndex("test").setId("1").setSource("foo", "bar").setRefreshPolicy("true").get();

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addScriptField(
                    "test_script_1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return null", Collections.emptyMap())
                ),
            response -> {
                DocumentField fieldObj = response.getHits().getAt(0).field("test_script_1");
                assertThat(fieldObj, notNullValue());
                List<?> fieldValues = fieldObj.getValues();
                assertThat(fieldValues, hasSize(1));
                assertThat(fieldValues.get(0), nullValue());
            }
        );
    }

    public void testPartialFields() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .startObject("obj1")
                    .startArray("arr1")
                    .startObject()
                    .startObject("obj2")
                    .field("field2", "value21")
                    .endObject()
                    .endObject()
                    .startObject()
                    .startObject("obj2")
                    .field("field2", "value22")
                    .endObject()
                    .endObject()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .get();

        indicesAdmin().prepareRefresh().get();
    }

    public void testStoredFieldsWithoutSource() throws Exception {
        createIndex("test");

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("_source")
                .field("enabled", false)
                .endObject()
                .startObject("properties")
                .startObject("byte_field")
                .field("type", "byte")
                .field("store", true)
                .endObject()
                .startObject("short_field")
                .field("type", "short")
                .field("store", true)
                .endObject()
                .startObject("integer_field")
                .field("type", "integer")
                .field("store", true)
                .endObject()
                .startObject("long_field")
                .field("type", "long")
                .field("store", true)
                .endObject()
                .startObject("float_field")
                .field("type", "float")
                .field("store", true)
                .endObject()
                .startObject("double_field")
                .field("type", "double")
                .field("store", true)
                .endObject()
                .startObject("date_field")
                .field("type", "date")
                .field("store", true)
                .endObject()
                .startObject("boolean_field")
                .field("type", "boolean")
                .field("store", true)
                .endObject()
                .startObject("binary_field")
                .field("type", "binary")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().preparePutMapping().setSource(mapping, XContentType.JSON).get();

        ZonedDateTime date = ZonedDateTime.of(2012, 3, 22, 0, 0, 0, 0, ZoneOffset.UTC);
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("byte_field", (byte) 1)
                    .field("short_field", (short) 2)
                    .field("integer_field", 3)
                    .field("long_field", 4L)
                    .field("float_field", 5.0f)
                    .field("double_field", 6.0d)
                    .field("date_field", DateFormatter.forPattern("date_optional_time").format(date))
                    .field("boolean_field", true)
                    .field("binary_field", Base64.getEncoder().encodeToString("testing text".getBytes("UTF-8")))
                    .endObject()
            )
            .get();

        indicesAdmin().prepareRefresh().get();

        assertCheckedResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addStoredField("byte_field")
                .addStoredField("short_field")
                .addStoredField("integer_field")
                .addStoredField("long_field")
                .addStoredField("float_field")
                .addStoredField("double_field")
                .addStoredField("date_field")
                .addStoredField("boolean_field")
                .addStoredField("binary_field"),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(
                    fields,
                    equalTo(
                        newHashSet(
                            "byte_field",
                            "short_field",
                            "integer_field",
                            "long_field",
                            "float_field",
                            "double_field",
                            "date_field",
                            "boolean_field",
                            "binary_field"
                        )
                    )
                );

                SearchHit searchHit = response.getHits().getAt(0);
                assertThat(searchHit.getFields().get("byte_field").getValue().toString(), equalTo("1"));
                assertThat(searchHit.getFields().get("short_field").getValue().toString(), equalTo("2"));
                assertThat(searchHit.getFields().get("integer_field").getValue(), equalTo((Object) 3));
                assertThat(searchHit.getFields().get("long_field").getValue(), equalTo((Object) 4L));
                assertThat(searchHit.getFields().get("float_field").getValue(), equalTo((Object) 5.0f));
                assertThat(searchHit.getFields().get("double_field").getValue(), equalTo((Object) 6.0d));
                String dateTime = DateFormatter.forPattern("date_optional_time").format(date);
                assertThat(searchHit.getFields().get("date_field").getValue(), equalTo((Object) dateTime));
                assertThat(searchHit.getFields().get("boolean_field").getValue(), equalTo((Object) Boolean.TRUE));
                assertThat(searchHit.getFields().get("binary_field").getValue(), equalTo(new BytesArray("testing text".getBytes("UTF8"))));
            }
        );
    }

    public void testSearchFieldsMetadata() throws Exception {
        prepareIndex("my-index").setId("1")
            .setRouting("1")
            .setSource(jsonBuilder().startObject().field("field1", "value").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        assertResponse(prepareSearch("my-index").addStoredField("field1").addStoredField("_routing"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(0).field("field1"), nullValue());
            assertThat(response.getHits().getAt(0).field("_routing").getValue().toString(), equalTo("1"));
        });
    }

    public void testGetFieldsComplexField() throws Exception {
        indicesAdmin().prepareCreate("my-index")
            .setSettings(Settings.builder().put("index.refresh_interval", -1))
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("field2")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("field3")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("field4")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
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
                .endObject()
        );

        prepareIndex("my-index").setId("1").setRefreshPolicy(IMMEDIATE).setSource(source, XContentType.JSON).get();

        String field = "field1.field2.field3.field4";

        assertResponse(prepareSearch("my-index").addStoredField(field), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(0).field(field).getValues().size(), equalTo(2));
            assertThat(response.getHits().getAt(0).field(field).getValues().get(0).toString(), equalTo("value1"));
            assertThat(response.getHits().getAt(0).field(field).getValues().get(1).toString(), equalTo("value2"));
        });
    }

    // see #8203
    public void testSingleValueFieldDatatField() throws ExecutionException, InterruptedException {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("test_field", "type=keyword").get());
        indexRandom(true, prepareIndex("test").setId("1").setSource("test_field", "foobar"));
        refresh();
        assertResponse(
            prepareSearch("test").setSource(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).docValueField("test_field")),
            response -> {
                assertHitCount(response, 1);
                Map<String, DocumentField> fields = response.getHits().getHits()[0].getFields();
                assertThat(fields.get("test_field").getValue(), equalTo("foobar"));
            }
        );
    }

    public void testDocValueFields() throws Exception {
        createIndex("test");

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("_source")
                .field("enabled", false)
                .endObject()
                .startObject("properties")
                .startObject("text_field")
                .field("type", "text")
                .field("fielddata", true)
                .endObject()
                .startObject("keyword_field")
                .field("type", "keyword")
                .endObject()
                .startObject("byte_field")
                .field("type", "byte")
                .endObject()
                .startObject("short_field")
                .field("type", "short")
                .endObject()
                .startObject("integer_field")
                .field("type", "integer")
                .endObject()
                .startObject("long_field")
                .field("type", "long")
                .endObject()
                .startObject("float_field")
                .field("type", "float")
                .endObject()
                .startObject("double_field")
                .field("type", "double")
                .endObject()
                .startObject("date_field")
                .field("type", "date")
                .endObject()
                .startObject("boolean_field")
                .field("type", "boolean")
                .endObject()
                .startObject("binary_field")
                .field("type", "binary")
                .field("doc_values", true) // off by default on binary fields
                .endObject()
                .startObject("ip_field")
                .field("type", "ip")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().preparePutMapping().setSource(mapping, XContentType.JSON).get();

        ZonedDateTime date = ZonedDateTime.of(2012, 3, 22, 0, 0, 0, 0, ZoneOffset.UTC);
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("text_field", "foo")
                    .field("keyword_field", "foo")
                    .field("byte_field", (byte) 1)
                    .field("short_field", (short) 2)
                    .field("integer_field", 3)
                    .field("long_field", 4L)
                    .field("float_field", 5.0f)
                    .field("double_field", 6.0d)
                    .field("date_field", DateFormatter.forPattern("date_optional_time").format(date))
                    .field("boolean_field", true)
                    .field("binary_field", new byte[] { 42, 100 })
                    .field("ip_field", "::1")
                    .endObject()
            )
            .get();

        indicesAdmin().prepareRefresh().get();

        SearchRequestBuilder builder = prepareSearch().setQuery(matchAllQuery())
            .addDocValueField("text_field")
            .addDocValueField("keyword_field")
            .addDocValueField("byte_field")
            .addDocValueField("short_field")
            .addDocValueField("integer_field")
            .addDocValueField("long_field")
            .addDocValueField("float_field")
            .addDocValueField("double_field")
            .addDocValueField("date_field")
            .addDocValueField("boolean_field")
            .addDocValueField("binary_field")
            .addDocValueField("ip_field");
        if (randomBoolean()) {
            builder.addDocValueField("*_field");
        }
        assertResponse(builder, response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
            assertThat(
                fields,
                equalTo(
                    newHashSet(
                        "byte_field",
                        "short_field",
                        "integer_field",
                        "long_field",
                        "float_field",
                        "double_field",
                        "date_field",
                        "boolean_field",
                        "text_field",
                        "keyword_field",
                        "binary_field",
                        "ip_field"
                    )
                )
            );

            assertThat(response.getHits().getAt(0).getFields().get("byte_field").getValues(), equalTo(List.of(1L)));
            assertThat(response.getHits().getAt(0).getFields().get("short_field").getValues(), equalTo(List.of(2L)));
            assertThat(response.getHits().getAt(0).getFields().get("integer_field").getValues(), equalTo(List.of(3L)));
            assertThat(response.getHits().getAt(0).getFields().get("long_field").getValues(), equalTo(List.of(4L)));
            assertThat(response.getHits().getAt(0).getFields().get("float_field").getValues(), equalTo(List.of(5.0)));
            assertThat(response.getHits().getAt(0).getFields().get("double_field").getValues(), equalTo(List.of(6.0d)));
            assertThat(
                response.getHits().getAt(0).getFields().get("date_field").getValue(),
                equalTo(DateFormatter.forPattern("date_optional_time").format(date))
            );
            assertThat(response.getHits().getAt(0).getFields().get("boolean_field").getValues(), equalTo(List.of(true)));
            assertThat(response.getHits().getAt(0).getFields().get("text_field").getValues(), equalTo(List.of("foo")));
            assertThat(response.getHits().getAt(0).getFields().get("keyword_field").getValues(), equalTo(List.of("foo")));
            assertThat(response.getHits().getAt(0).getFields().get("binary_field").getValues(), equalTo(List.of("KmQ=")));
            assertThat(response.getHits().getAt(0).getFields().get("ip_field").getValues(), equalTo(List.of("::1")));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addDocValueField("*field"), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits().length, equalTo(1));
            Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
            assertThat(
                fields,
                equalTo(
                    newHashSet(
                        "byte_field",
                        "short_field",
                        "integer_field",
                        "long_field",
                        "float_field",
                        "double_field",
                        "date_field",
                        "boolean_field",
                        "text_field",
                        "keyword_field",
                        "binary_field",
                        "ip_field"
                    )
                )
            );

            assertThat(response.getHits().getAt(0).getFields().get("byte_field").getValues(), equalTo(List.of(1L)));
            assertThat(response.getHits().getAt(0).getFields().get("short_field").getValues(), equalTo(List.of(2L)));
            assertThat(response.getHits().getAt(0).getFields().get("integer_field").getValues(), equalTo(List.of(3L)));
            assertThat(response.getHits().getAt(0).getFields().get("long_field").getValues(), equalTo(List.of(4L)));
            assertThat(response.getHits().getAt(0).getFields().get("float_field").getValues(), equalTo(List.of(5.0)));
            assertThat(response.getHits().getAt(0).getFields().get("double_field").getValues(), equalTo(List.of(6.0d)));
            assertThat(
                response.getHits().getAt(0).getFields().get("date_field").getValue(),
                equalTo(DateFormatter.forPattern("date_optional_time").format(date))
            );
            assertThat(response.getHits().getAt(0).getFields().get("boolean_field").getValues(), equalTo(List.of(true)));
            assertThat(response.getHits().getAt(0).getFields().get("text_field").getValues(), equalTo(List.of("foo")));
            assertThat(response.getHits().getAt(0).getFields().get("keyword_field").getValues(), equalTo(List.of("foo")));
            assertThat(response.getHits().getAt(0).getFields().get("binary_field").getValues(), equalTo(List.of("KmQ=")));
            assertThat(response.getHits().getAt(0).getFields().get("ip_field").getValues(), equalTo(List.of("::1")));
        });
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addDocValueField("byte_field", "#.0")
                .addDocValueField("short_field", "#.0")
                .addDocValueField("integer_field", "#.0")
                .addDocValueField("long_field", "#.0")
                .addDocValueField("float_field", "#.0")
                .addDocValueField("double_field", "#.0")
                .addDocValueField("date_field", "epoch_millis"),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                Set<String> fields = new HashSet<>(response.getHits().getAt(0).getFields().keySet());
                assertThat(
                    fields,
                    equalTo(
                        newHashSet("byte_field", "short_field", "integer_field", "long_field", "float_field", "double_field", "date_field")
                    )
                );
                assertThat(response.getHits().getAt(0).getFields().get("byte_field").getValues(), equalTo(List.of("1.0")));
                assertThat(response.getHits().getAt(0).getFields().get("short_field").getValues(), equalTo(List.of("2.0")));
                assertThat(response.getHits().getAt(0).getFields().get("integer_field").getValues(), equalTo(List.of("3.0")));
                assertThat(response.getHits().getAt(0).getFields().get("long_field").getValues(), equalTo(List.of("4.0")));
                assertThat(response.getHits().getAt(0).getFields().get("float_field").getValues(), equalTo(List.of("5.0")));
                assertThat(response.getHits().getAt(0).getFields().get("double_field").getValues(), equalTo(List.of("6.0")));
                assertThat(
                    response.getHits().getAt(0).getFields().get("date_field").getValue(),
                    equalTo(DateFormatter.forPattern("epoch_millis").format(date))
                );
            }
        );
    }

    public void testScriptFields() throws Exception {
        assertAcked(
            prepareCreate("index").setMapping(
                "s",
                "type=keyword",
                "l",
                "type=long",
                "d",
                "type=double",
                "ms",
                "type=keyword",
                "ml",
                "type=long",
                "md",
                "type=double"
            )
        );
        final int numDocs = randomIntBetween(3, 8);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            reqs.add(
                prepareIndex("index").setId(Integer.toString(i))
                    .setSource(
                        "s",
                        Integer.toString(i),
                        "ms",
                        new String[] { Integer.toString(i), Integer.toString(i + 1) },
                        "l",
                        i,
                        "ml",
                        new long[] { i, i + 1 },
                        "d",
                        i,
                        "md",
                        new double[] { i, i + 1 }
                    )
            );
        }
        indexRandom(true, reqs);
        ensureSearchable();
        SearchRequestBuilder req = prepareSearch("index");
        for (String field : Arrays.asList("s", "ms", "l", "ml", "d", "md")) {
            req.addScriptField(
                field,
                new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['" + field + "']", Collections.emptyMap())
            );
        }
        assertNoFailuresAndResponse(req, response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                final int id = Integer.parseInt(hit.getId());
                Map<String, DocumentField> fields = hit.getFields();
                assertThat(fields.get("s").getValues(), equalTo(Collections.<Object>singletonList(Integer.toString(id))));
                assertThat(fields.get("l").getValues(), equalTo(Collections.<Object>singletonList((long) id)));
                assertThat(fields.get("d").getValues(), equalTo(Collections.<Object>singletonList((double) id)));
                assertThat(fields.get("ms").getValues(), equalTo(Arrays.<Object>asList(Integer.toString(id), Integer.toString(id + 1))));
                assertThat(fields.get("ml").getValues(), equalTo(Arrays.<Object>asList((long) id, id + 1L)));
                assertThat(fields.get("md").getValues(), equalTo(Arrays.<Object>asList((double) id, id + 1d)));
            }
        });
    }

    public void testDocValueFieldsWithFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_source")
            .field("enabled", false)
            .endObject()
            .startObject("properties")
            .startObject("text_field")
            .field("type", "text")
            .field("fielddata", true)
            .endObject()
            .startObject("date_field")
            .field("type", "date")
            .field("format", "yyyy-MM-dd")
            .endObject()
            .startObject("text_field_alias")
            .field("type", "alias")
            .field("path", "text_field")
            .endObject()
            .startObject("date_field_alias")
            .field("type", "alias")
            .field("path", "date_field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen("test");

        ZonedDateTime date = ZonedDateTime.of(1990, 12, 29, 0, 0, 0, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ROOT);

        indexDoc("test", "1", "text_field", "foo", "date_field", formatter.format(date));
        refresh("test");

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addDocValueField("text_field_alias")
                .addDocValueField("date_field_alias")
                .addDocValueField("date_field"),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);

                Map<String, DocumentField> fields = hit.getFields();
                assertThat(fields.keySet(), equalTo(newHashSet("text_field_alias", "date_field_alias", "date_field")));

                DocumentField textFieldAlias = fields.get("text_field_alias");
                assertThat(textFieldAlias.getName(), equalTo("text_field_alias"));
                assertThat(textFieldAlias.getValue(), equalTo("foo"));

                DocumentField dateFieldAlias = fields.get("date_field_alias");
                assertThat(dateFieldAlias.getName(), equalTo("date_field_alias"));
                assertThat(dateFieldAlias.getValue(), equalTo("1990-12-29"));

                DocumentField dateField = fields.get("date_field");
                assertThat(dateField.getName(), equalTo("date_field"));
                assertThat(dateField.getValue(), equalTo("1990-12-29"));
            }
        );
    }

    public void testWildcardDocValueFieldsWithFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_source")
            .field("enabled", false)
            .endObject()
            .startObject("properties")
            .startObject("text_field")
            .field("type", "text")
            .field("fielddata", true)
            .endObject()
            .startObject("date_field")
            .field("type", "date")
            .field("format", "yyyy-MM-dd")
            .endObject()
            .startObject("text_field_alias")
            .field("type", "alias")
            .field("path", "text_field")
            .endObject()
            .startObject("date_field_alias")
            .field("type", "alias")
            .field("path", "date_field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen("test");

        ZonedDateTime date = ZonedDateTime.of(1990, 12, 29, 0, 0, 0, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ROOT);

        indexDoc("test", "1", "text_field", "foo", "date_field", formatter.format(date));
        refresh("test");

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery()).addDocValueField("*alias").addDocValueField("date_field"),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);

                Map<String, DocumentField> fields = hit.getFields();
                assertThat(fields.keySet(), equalTo(newHashSet("text_field_alias", "date_field_alias", "date_field")));

                DocumentField textFieldAlias = fields.get("text_field_alias");
                assertThat(textFieldAlias.getName(), equalTo("text_field_alias"));
                assertThat(textFieldAlias.getValue(), equalTo("foo"));

                DocumentField dateFieldAlias = fields.get("date_field_alias");
                assertThat(dateFieldAlias.getName(), equalTo("date_field_alias"));
                assertThat(dateFieldAlias.getValue(), equalTo("1990-12-29"));

                DocumentField dateField = fields.get("date_field");
                assertThat(dateField.getName(), equalTo("date_field"));
                assertThat(dateField.getValue(), equalTo("1990-12-29"));
            }
        );
    }

    public void testStoredFieldsWithFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("store", true)
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .field("store", false)
            .endObject()
            .startObject("field1-alias")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("field2-alias")
            .field("type", "alias")
            .field("path", "field2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));

        indexDoc("test", "1", "field1", "value1", "field2", "value2");
        refresh("test");

        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addStoredField("field1-alias").addStoredField("field2-alias"),
            response -> {
                assertHitCount(response, 1L);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getFields().size());
                assertTrue(hit.getFields().containsKey("field1-alias"));

                DocumentField field = hit.getFields().get("field1-alias");
                assertThat(field.getValue().toString(), equalTo("value1"));
            }
        );
    }

    public void testWildcardStoredFieldsWithFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("store", true)
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .field("store", false)
            .endObject()
            .startObject("field1-alias")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("field2-alias")
            .field("type", "alias")
            .field("path", "field2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));

        indexDoc("test", "1", "field1", "value1", "field2", "value2");
        refresh("test");

        assertResponse(prepareSearch().setQuery(matchAllQuery()).addStoredField("field*"), response -> {
            assertHitCount(response, 1L);

            SearchHit hit = response.getHits().getAt(0);
            assertEquals(2, hit.getFields().size());
            assertTrue(hit.getFields().containsKey("field1"));
            assertTrue(hit.getFields().containsKey("field1-alias"));

            DocumentField field = hit.getFields().get("field1");
            assertThat(field.getValue().toString(), equalTo("value1"));

            DocumentField fieldAlias = hit.getFields().get("field1-alias");
            assertThat(fieldAlias.getValue().toString(), equalTo("value1"));
        });
    }

    public void testLoadMetadata() throws Exception {
        assertAcked(prepareCreate("test"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setRouting("1").setSource(jsonBuilder().startObject().field("field1", "value").endObject())
        );

        assertNoFailuresAndResponse(prepareSearch("test").addStoredField("field1"), response -> {
            assertHitCount(response, 1);

            Map<String, DocumentField> fields = response.getHits().getAt(0).getMetadataFields();

            assertThat(fields.get("field1"), nullValue());
            assertThat(fields.get("_routing").getValue().toString(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getDocumentFields().size(), equalTo(0));
        });
    }
}
