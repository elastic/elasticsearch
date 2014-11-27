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

package org.elasticsearch.script.javascript;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class JavaScriptScriptSearchTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void testJavaScriptFilter() throws Exception {
        createIndex("test");
        index("test", "type1", "1", jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject());
        flush();
        index("test", "type1", "2", jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject());
        flush();
        index("test", "type1", "3", jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject());
        refresh();

        logger.info(" --> running doc['num1'].value > 1");
        SearchResponse response = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > 1").lang("js")))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "js", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info(" --> running doc['num1'].value > param1");
        response = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("js").addParam("param1", 2)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "js", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info(" --> running doc['num1'].value > param1");
        response = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("js").addParam("param1", -1)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "js", "doc['num1'].value", null)
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
                .addScriptField("s_obj1", "js", "_source.obj1", null)
                .addScriptField("s_obj1_test", "js", "_source.obj1.test", null)
                .addScriptField("s_obj2", "js", "_source.obj2", null)
                .addScriptField("s_obj2_arr2", "js", "_source.obj2.arr2", null)
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
                        .add(ScoreFunctionBuilders.scriptFunction("doc['num1'].value").lang("js"))))
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
                        .add(ScoreFunctionBuilders.scriptFunction("-doc['num1'].value").lang("js"))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));


        logger.info(" --> running pow(doc['num1'].value, 2)");
        response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                        .add(ScoreFunctionBuilders.scriptFunction("Math.pow(doc['num1'].value, 2)").lang("js"))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info(" --> running max(doc['num1'].value, 1)");
        response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                        .add(ScoreFunctionBuilders.scriptFunction("Math.max(doc['num1'].value, 1)").lang("js"))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info(" --> running doc['num1'].value * _score");
        response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value"))
                        .add(ScoreFunctionBuilders.scriptFunction("doc['num1'].value * _score").lang("js"))))
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
                        .add(ScoreFunctionBuilders.scriptFunction("param1 * param2 * _score").param("param1", 2).param("param2", 2).lang("js"))))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info(" --> Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info(" --> Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
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
                                                functionScoreQuery().add(scriptFunction("1").lang("js")))
                                                .add(scriptFunction("_score.doubleValue()").lang("js")))
                                        .add(scriptFunction("_score.doubleValue()").lang("js")
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
                                        .add(scriptFunction("_score.doubleValue()").lang("js")
                                        )
                        ).aggregation(terms("score_agg").script("_score.doubleValue()").lang("js"))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsNumber().floatValue(), is(1f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), is(1l));
    }

    @Test
    public void testUseListLengthInScripts() throws Exception {
        createIndex("index");
        index("index", "testtype", "1", jsonBuilder().startObject().field("f", 42).endObject());
        ensureSearchable("index");
        refresh();
        SearchResponse response = client().prepareSearch().addScriptField("foobar", "js", "doc['f'].values.length", null).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
        assertThat((Integer) response.getHits().getAt(0).getFields().get("foobar").value(), equalTo(1));
    }
}
