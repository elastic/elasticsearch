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
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.scriptedMetric;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE)
@ElasticsearchIntegrationTest.SuiteScopeTest
public class ScriptedMetricTests extends ElasticsearchIntegrationTest {

    private static long numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field("value", randomAsciiOfLengthBetween(5, 15)).endObject()));
        }
        indexRandom(true, builders);

        // creating an index to test the empty buckets functionality. The way it
        // works is by indexing
        // two docs {value: 0} and {value : 2}, then building a histogram agg
        // with interval 1 and with empty
        // buckets computed.. the empty bucket is the one associated with key
        // "1". then each test will have
        // to check that this bucket exists with the appropriate sub
        // aggregations.
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field("value", i * 2).endObject()));
        }

        PutIndexedScriptResponse indexScriptResponse = client().preparePutIndexedScript("groovy", "initScript_indexed", "{\"script\":\"vars.multiplier = 3\"}").get();
        assertThat(indexScriptResponse.isCreated(), equalTo(true));
        indexScriptResponse = client().preparePutIndexedScript("groovy", "mapScript_indexed", "{\"script\":\"_agg.add(vars.multiplier)\"}").get();
        assertThat(indexScriptResponse.isCreated(), equalTo(true));
        indexScriptResponse = client().preparePutIndexedScript("groovy", "combineScript_indexed",
                "{\"script\":\"newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation\"}")
                .get();
        assertThat(indexScriptResponse.isCreated(), equalTo(true));
        indexScriptResponse = client().preparePutIndexedScript(
                "groovy",
                "reduceScript_indexed",
                "{\"script\":\"newaggregation = []; sum = 0;for (agg in _aggs) { for (a in agg) { sum += a} }; newaggregation.add(sum); return newaggregation\"}")
                .get();
        assertThat(indexScriptResponse.isCreated(), equalTo(true));
        
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", getResource("/org/elasticsearch/search/aggregations/metrics/scripted/conf"))
                .build();
        return settings;
    }

    @Test
    public void testMap() {
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(scriptedMetric("scripted").mapScript("_agg['count'] = 1")).execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(getNumShards("idx").numPrimaries));
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(Map.class));
            Map<String, Object> map = (Map<String, Object>) object;
            assertThat(map.size(), equalTo(1));
            assertThat(map.get("count"), notNullValue());
            assertThat(map.get("count"), instanceOf(Number.class));
            assertThat((Number) map.get("count"), equalTo((Number) 1));
        }
    }

    @Test
    public void testMap_withParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());

        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(scriptedMetric("scripted").params(params).mapScript("_agg.add(1)")).execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                assertThat(numberValue, equalTo((Number) 1));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs));
    }

    @Test
    public void testInitMap_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted").params(params).initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")).execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                assertThat(numberValue, equalTo((Number) 3));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs * 3));
    }

    @Test
    public void testMapCombine_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .mapScript("_agg.add(1)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                assertThat(numberValue.longValue(), allOf(greaterThanOrEqualTo(1l), lessThanOrEqualTo(numDocs)));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs));
    }

    @Test
    public void testInitMapCombine_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                assertThat(numberValue.longValue(), allOf(greaterThanOrEqualTo(3l), lessThanOrEqualTo(numDocs * 3)));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs * 3));
    }

    @Test
    public void testInitMapCombineReduce_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    @Test
    public void testMapCombineReduce_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .mapScript("_agg.add(vars.multiplier)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs));
    }

    @Test
    public void testInitMapReduce_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    @Test
    public void testMapReduce_withParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .mapScript("_agg.add(vars.multiplier)")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs));
    }

    @Test
    public void testInitMapCombineReduce_withParamsAndReduceParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);
        Map<String, Object> reduceParams = new HashMap<>();
        reduceParams.put("multiplier", 4);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .reduceParams(reduceParams)
                                .initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum * multiplier); return newaggregation"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 12));
    }

    @Test
    public void testInitMapCombineReduce_withParams_Indexed() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted").params(params).scriptType(ScriptType.INDEXED).initScript("initScript_indexed")
                                .mapScript("mapScript_indexed").combineScript("combineScript_indexed").reduceScript("reduceScript_indexed"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    @Test
    public void testInitMapCombineReduce_withParams_File() {

        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        scriptedMetric("scripted").params(params).scriptType(ScriptType.FILE).initScript("init_script")
                                .mapScript("map_script").combineScript("combine_script").reduceScript("reduce_script"))
                .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    @Test
    public void testEmptyAggregation() throws Exception {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("_agg", new ArrayList<>());
        params.put("vars", varsMap);

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(
                        scriptedMetric("scripted")
                                .params(params)
                                .initScript("vars.multiplier = 3")
                                .mapScript("_agg.add(vars.multiplier)")
                                .combineScript(
                                        "newaggregation = []; sum = 0;for (a in _agg) { sum += a}; newaggregation.add(sum); return newaggregation")
                                .reduceScript(
                                        "newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, notNullValue());

        ScriptedMetric scriptedMetric = bucket.getAggregations().get("scripted");
        assertThat(scriptedMetric, notNullValue());
        assertThat(scriptedMetric.getName(), equalTo("scripted"));
        assertThat(scriptedMetric.aggregation(), nullValue());
    }

}
