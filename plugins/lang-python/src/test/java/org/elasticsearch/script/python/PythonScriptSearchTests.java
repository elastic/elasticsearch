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

package org.elasticsearch.script.python;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class PythonScriptSearchTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @After
    public void close() {
        // We need to clear some system properties
        System.clearProperty("python.cachedir.skip");
        System.clearProperty("python.console.encoding");
    }

    @Test
    public void testPythonFilter() throws Exception {
        createIndex("test");
        index("test", "type1", "1", jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject());
        flush();
        index("test", "type1", "2", jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject());
        flush();
        index("test", "type1", "3", jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject());
        refresh();

        logger.info(" --> running doc['num1'].value > 1");
        SearchResponse response = client().prepareSearch()
                .setQuery(scriptQuery(new Script("doc['num1'].value > 1", ScriptService.ScriptType.INLINE, "python", null)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", new Script("doc['num1'].value", ScriptService.ScriptType.INLINE, "python", null))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info(" --> running doc['num1'].value > param1");
        response = client().prepareSearch()
                .setQuery(scriptQuery(new Script("doc['num1'].value > param1", ScriptService.ScriptType.INLINE, "python", Collections.singletonMap("param1", 2))))
                        .addSort("num1", SortOrder.ASC)
                        .addScriptField("sNum1", new Script("doc['num1'].value", ScriptService.ScriptType.INLINE, "python", null))
                                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info(" --> running doc['num1'].value > param1");
        response = client().prepareSearch()
                .setQuery(scriptQuery(new Script("doc['num1'].value > param1", ScriptService.ScriptType.INLINE, "python", Collections.singletonMap("param1", -1))))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", new Script("doc['num1'].value", ScriptService.ScriptType.INLINE, "python", null))
                        .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
    }

    @Test
    public void testScriptFieldUsingSource() throws Exception {
        createIndex("test");
        index("test", "type1", "1",
                jsonBuilder().startObject()
                        .startObject("obj1").field("test", "something").endObject()
                        .startObject("obj2").startArray("arr2").value("arr_value1").value("arr_value2").endArray().endObject()
                        .endObject());
        refresh();

        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("s_obj1", new Script("_source['obj1']", ScriptService.ScriptType.INLINE,  "python", null))
                .addScriptField("s_obj1_test", new Script("_source['obj1']['test']", ScriptService.ScriptType.INLINE,  "python", null))
                .addScriptField("s_obj2", new Script("_source['obj2']", ScriptService.ScriptType.INLINE, "python", null))
                .addScriptField("s_obj2_arr2", new Script("_source['obj2']['arr2']", ScriptService.ScriptType.INLINE,  "python", null))
                .execute().actionGet();

        Map<String, Object> sObj1 = (Map<String, Object>) response.getHits().getAt(0).field("s_obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.getHits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        Map<String, Object> sObj2 = (Map<String, Object>) response.getHits().getAt(0).field("s_obj2").value();
        List sObj2Arr2 = (List) sObj2.get("arr2");
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        sObj2Arr2 = (List) response.getHits().getAt(0).field("s_obj2_arr2").values();
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));
    }

    @Test
    public void testCustomScriptBoost() throws Exception {
        createIndex("test");
        index("test", "type1", "1", jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject());
        index("test", "type1", "2", jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject());
        refresh();

        logger.info("--- QUERY_THEN_FETCH");

        logger.info(" --> running doc['num1'].value");
        SearchResponse response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                                .add(ScoreFunctionBuilders.scriptFunction(new Script("doc['num1'].value", ScriptService.ScriptType.INLINE, "python", null)))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info(" --> running -doc['num1'].value");
        response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                                .add(ScoreFunctionBuilders.scriptFunction(new Script("-doc['num1'].value",  ScriptService.ScriptType.INLINE, "python", null)))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));


        logger.info(" --> running doc['num1'].value * _score");
        response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                                .add(ScoreFunctionBuilders.scriptFunction(new Script("doc['num1'].value * _score.doubleValue()",  ScriptService.ScriptType.INLINE, "python", null)))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info(" --> running param1 * param2 * _score");
        response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                                .add(ScoreFunctionBuilders.scriptFunction(new Script("param1 * param2 * _score.doubleValue()", ScriptService.ScriptType.INLINE, "python", MapBuilder.<String, Object>newMapBuilder().put("param1", 2).put("param2", 2).immutableMap())))))
                ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
    }

    /**
     * Test case for #4: https://github.com/elasticsearch/elasticsearch-lang-python/issues/4
     * Update request that uses python script with no parameters fails with NullPointerException
     * @throws Exception
     */
    @Test
    public void testPythonEmptyParameters() throws Exception {
        createIndex("test");
        index("test", "type1", "1", jsonBuilder().startObject().field("myfield", "foo").endObject());
        refresh();

        client().prepareUpdate("test", "type1", "1").setScript(new Script("ctx[\"_source\"][\"myfield\"]=\"bar\"", ScriptService.ScriptType.INLINE, "python", null))
            .execute().actionGet();
        refresh();

        Object value = get("test", "type1", "1").getSourceAsMap().get("myfield");
        assertThat(value instanceof String, is(true));

        assertThat((String) value, CoreMatchers.equalTo("bar"));
    }

    @Test
    public void testScriptScoresNested() throws IOException {
        createIndex("index");
        ensureYellow();
        index("index", "testtype", "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(
                                        functionScoreQuery(
                                                functionScoreQuery().add(scriptFunction(new Script("1",  ScriptService.ScriptType.INLINE, "python", null))))
                                                .add(scriptFunction(new Script("_score.doubleValue()",  ScriptService.ScriptType.INLINE, "python", null)))
                                                .add(scriptFunction(new Script("_score.doubleValue()",  ScriptService.ScriptType.INLINE, "python", null)))
                                )
                        )
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
    }

    @Test
    public void testScriptScoresWithAgg() throws IOException {
        createIndex("index");
        ensureYellow();
        index("index", "testtype", "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery()
                                        .add(scriptFunction(new Script("_score.doubleValue()",  ScriptService.ScriptType.INLINE, "python", null))
                                        )
                        ).aggregation(terms("score_agg").script(new Script("_score.doubleValue()",  ScriptService.ScriptType.INLINE, "python", null)))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsNumber().floatValue(), Matchers.is(1f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), Matchers.is(1l));
    }

    /**
     * Test case for #19: https://github.com/elasticsearch/elasticsearch-lang-python/issues/19
     * Multi-line or multi-statement Python scripts raise NullPointerException
     */
    @Test
    public void testPythonMultiLines() throws Exception {
        createIndex("test");
        index("test", "type1", "1", jsonBuilder().startObject().field("myfield", "foo").endObject());
        refresh();

        client().prepareUpdate("test", "type1", "1")
                .setScript(new Script("a=42; ctx[\"_source\"][\"myfield\"]=\"bar\"", ScriptService.ScriptType.INLINE, "python", null))
                .execute().actionGet();
        refresh();

        Object value = get("test", "type1", "1").getSourceAsMap().get("myfield");
        assertThat(value instanceof String, is(true));

        assertThat((String) value, CoreMatchers.equalTo("bar"));
    }

}
