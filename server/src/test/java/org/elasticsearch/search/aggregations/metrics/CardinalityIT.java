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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class CardinalityIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value", vars -> vars.get("_value"));

            scripts.put("doc['str_value'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get("str_value");
            });

            scripts.put("doc['str_values'].values", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Strings strValue = (ScriptDocValues.Strings) doc.get("str_values");
                return strValue.getValues();
            });

            scripts.put("doc[' + singleNumericField() + '].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get(singleNumericField());
            });

            scripts.put("doc[' + multiNumericField(false) + '].values", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues<?>) doc.get(multiNumericField(false))).getValues();
            });

            return scripts;
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", numberOfReplicas())
                .build();
    }

    static long numDocs;
    static long precisionThreshold;

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        prepareCreate("idx").addMapping("type",
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("str_value")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("str_values")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("l_value")
                        .field("type", "long")
                    .endObject()
                    .startObject("l_values")
                        .field("type", "long")
                    .endObject()
                    .startObject("d_value")
                        .field("type", "double")
                    .endObject()
                    .startObject("d_values")
                        .field("type", "double")
                    .endObject()
                    .endObject().endObject().endObject()).execute().actionGet();

        numDocs = randomIntBetween(2, 100);
        precisionThreshold = randomIntBetween(0, 1 << randomInt(20));
        IndexRequestBuilder[] builders = new IndexRequestBuilder[(int) numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                        .field("str_value", "s" + i)
                        .array("str_values", new String[]{"s" + (i * 2), "s" + (i * 2 + 1)})
                        .field("l_value", i)
                        .array("l_values", new int[] {i * 2, i * 2 + 1})
                        .field("d_value", i)
                        .array("d_values", new double[]{i * 2, i * 2 + 1})
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");

        IndexRequestBuilder[] dummyDocsBuilder = new IndexRequestBuilder[10];
        for (int i = 0; i < dummyDocsBuilder.length; i++) {
            dummyDocsBuilder[i] = client().prepareIndex("idx", "type").setSource("a_field", "1");
        }
        indexRandom(true, dummyDocsBuilder);

        ensureSearchable();
    }

    private void assertCount(Cardinality count, long value) {
        if (value <= precisionThreshold) {
            // linear counting should be picked, and should be accurate
            assertEquals(value, count.getValue());
        } else {
            // error is not bound, so let's just make sure it is > 0
            assertThat(count.getValue(), greaterThan(0L));
        }
    }
     private static String singleNumericField() {
        return randomBoolean() ? "l_value" : "d_value";
    }

    private static String multiNumericField(boolean hash) {
        return randomBoolean() ? "l_values" : "d_values";
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, 0);
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedNumericGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                        global("global").subAggregation(
                                cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField())))
                .execute().actionGet();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        // assertThat(global.getDocCount(), equalTo(numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Cardinality cardinality = global.getAggregations().get("cardinality");
        assertThat(cardinality, notNullValue());
        assertThat(cardinality.getName(), equalTo("cardinality"));
        long expectedValue = numDocs;
        assertCount(cardinality, expectedValue);
        assertThat(((InternalAggregation)global).getProperty("cardinality"), equalTo(cardinality));
        assertThat(((InternalAggregation)global).getProperty("cardinality.value"), equalTo((double) cardinality.getValue()));
        assertThat((double) ((InternalAggregation)cardinality).getProperty("value"), equalTo((double) cardinality.getValue()));
    }

    public void testSingleValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(false)))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(true)))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['str_value'].value", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['str_values'].values", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericScript() throws Exception {
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc[' + singleNumericField() + '].value", emptyMap());
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).script(script))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericScript() throws Exception {
        Script script =
            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc[' + multiNumericField(false) + '].values", Collections.emptyMap());
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).script(script))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .field("str_value")
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .field("str_values")
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .field(singleNumericField())
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality")
                                .precisionThreshold(precisionThreshold)
                                .field(multiNumericField(false))
                                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap())))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms").field("str_value")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Cardinality count = bucket.getAggregations().get("cardinality");
            assertThat(count, notNullValue());
            assertThat(count.getName(), equalTo("cardinality"));
            assertCount(count, 2);
        }
    }

    /**
     * Make sure that a request using a script does not get cached and a request
     * not using a script does get cached.
     */
    public void testDontCacheScripts() throws Exception {
        assertAcked(prepareCreate("cache_test_idx").addMapping("type", "d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get());
        indexRandom(true, client().prepareIndex("cache_test_idx", "type", "1").setSource("s", 1),
                client().prepareIndex("cache_test_idx", "type", "2").setSource("s", 2));

        // Make sure we are starting with a clear cache
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // Test that a request using a script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                        cardinality("foo").field("d").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap())))
                .get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // To make sure that the cache is working test that a request not using
        // a script is cached
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(cardinality("foo").field("d")).get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(1L));
    }
}
