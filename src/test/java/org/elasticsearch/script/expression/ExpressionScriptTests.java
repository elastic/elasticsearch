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

package org.elasticsearch.script.expression;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ExpressionScriptTests extends ElasticsearchIntegrationTest {

    private SearchRequestBuilder buildRequest(String script, Object... params) {
        ensureGreen("test");

        Map<String, Object> paramsMap = new HashMap<>();
        assert(params.length % 2 == 0);
        for (int i = 0; i < params.length; i += 2) {
            paramsMap.put(params[i].toString(), params[i + 1]);
        }

        SearchRequestBuilder req = new SearchRequestBuilder(client()).setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
           .addSort(SortBuilders.fieldSort("_uid")
                                .order(SortOrder.ASC)).addScriptField("foo", "expression", script, paramsMap);
        return req;
    }

    public void testBasic() throws Exception {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex("test", "doc", "1").setSource("foo", 4).setRefresh(true).get();
        SearchResponse rsp = buildRequest("doc['foo'].value + 1").get();
        assertEquals(1, rsp.getHits().getTotalHits());
        assertEquals(5.0, rsp.getHits().getAt(0).field("foo").getValue());
    }

    public void testScore() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(true,
            client().prepareIndex("test", "doc", "1").setSource("text", "hello goodbye"),
            client().prepareIndex("test", "doc", "2").setSource("text", "hello hello hello goodbye"),
            client().prepareIndex("test", "doc", "3").setSource("text", "hello hello goodebye"));
        ScoreFunctionBuilder score = ScoreFunctionBuilders.scriptFunction("1 / _score", "expression");
        SearchRequestBuilder req = new SearchRequestBuilder(client()).setIndices("test");
        req.setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("text", "hello"), score).boostMode("replace"));
        req.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // make sure DF is consistent
        SearchResponse rsp = req.get();
        SearchHits hits = rsp.getHits();
        assertEquals(3, hits.getTotalHits());
        assertEquals("1", hits.getAt(0).getId());
        assertEquals("3", hits.getAt(1).getId());
        assertEquals("2", hits.getAt(2).getId());
    }

    public void testSparseField() throws Exception {
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("doc", "x", "type=long", "y", "type=long"));
        ensureGreen("test");
        indexRandom(true,
                client().prepareIndex("test", "doc", "1").setSource("x", 4),
                client().prepareIndex("test", "doc", "2").setSource("y", 2));
        SearchResponse rsp = buildRequest("doc['x'].value + 1").get();
        ElasticsearchAssertions.assertSearchResponse(rsp);
        SearchHits hits = rsp.getHits();
        assertEquals(2, rsp.getHits().getTotalHits());
        assertEquals(5.0, hits.getAt(0).field("foo").getValue());
        assertEquals(1.0, hits.getAt(1).field("foo").getValue());
    }

    public void testMissingField() throws Exception {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex("test", "doc", "1").setSource("x", 4).setRefresh(true).get();
        try {
            buildRequest("doc['bogus'].value").get();
            fail("Expected missing field to cause failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                    ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained missing field error",
                    ExceptionsHelper.detailedMessage(e).contains("does not exist in mappings"), equalTo(true));
        }
    }

    public void testParams() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(true,
                    client().prepareIndex("test", "doc", "1").setSource("x", 10),
                    client().prepareIndex("test", "doc", "2").setSource("x", 3),
                    client().prepareIndex("test", "doc", "3").setSource("x", 5));
        // a = int, b = double, c = long
        String script = "doc['x'].value * a + b + ((c + doc['x'].value) > 5000000009 ? 1 : 0)";
        SearchResponse rsp = buildRequest(script, "a", 2, "b", 3.5, "c", 5000000000L).get();
        SearchHits hits = rsp.getHits();
        assertEquals(3, hits.getTotalHits());
        assertEquals(24.5, hits.getAt(0).field("foo").getValue());
        assertEquals(9.5, hits.getAt(1).field("foo").getValue());
        assertEquals(13.5, hits.getAt(2).field("foo").getValue());
    }

    public void testCompileFailure() {
        client().prepareIndex("test", "doc", "1").setSource("x", 1).setRefresh(true).get();
        try {
            buildRequest("garbage%@#%@").get();
            fail("Expected expression compilation failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                       ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained compilation failure",
                       ExceptionsHelper.detailedMessage(e).contains("Failed to parse expression"), equalTo(true));
        }
    }

    public void testNonNumericParam() {
        client().prepareIndex("test", "doc", "1").setSource("x", 1).setRefresh(true).get();
        try {
            buildRequest("a", "a", "astring").get();
            fail("Expected string parameter to cause failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                       ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained non-numeric parameter error",
                       ExceptionsHelper.detailedMessage(e).contains("must be a numeric type"), equalTo(true));
        }
    }

    public void testNonNumericField() {
        client().prepareIndex("test", "doc", "1").setSource("text", "this is not a number").setRefresh(true).get();
        try {
            buildRequest("doc['text'].value").get();
            fail("Expected text field to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                       ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained non-numeric field error",
                       ExceptionsHelper.detailedMessage(e).contains("must be numeric"), equalTo(true));
        }
    }

    public void testInvalidGlobalVariable() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();
        try {
            buildRequest("bogus").get();
            fail("Expected bogus variable to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                       ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained unknown variable error",
                       ExceptionsHelper.detailedMessage(e).contains("Unknown variable"), equalTo(true));
        }
    }

    public void testDocWithoutField() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();
        try {
            buildRequest("doc").get();
            fail("Expected doc variable without field to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                    ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained a missing specific field error",
                    ExceptionsHelper.detailedMessage(e).contains("must be used with a specific field"), equalTo(true));
        }
    }

    public void testInvalidFieldMember() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();
        try {
            buildRequest("doc['foo'].bogus").get();
            fail("Expected bogus field member to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained ExpressionScriptCompilationException",
                    ExceptionsHelper.detailedMessage(e).contains("ExpressionScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained field member error",
                    ExceptionsHelper.detailedMessage(e).contains("Invalid member for field"), equalTo(true));
        }
    }

    public void testSpecialValueVariable() throws Exception {
        // i.e. _value for aggregations
        createIndex("test");
        ensureGreen("test");
        indexRandom(true,
            client().prepareIndex("test", "doc", "1").setSource("x", 5, "y", 1.2),
            client().prepareIndex("test", "doc", "2").setSource("x", 10, "y", 1.4),
            client().prepareIndex("test", "doc", "3").setSource("x", 13, "y", 1.8));

        SearchRequestBuilder req = new SearchRequestBuilder(client()).setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
           .addAggregation(AggregationBuilders.stats("int_agg").field("x").script("_value * 3").lang("expression"))
           .addAggregation(AggregationBuilders.stats("double_agg").field("y").script("_value - 1.1").lang("expression"));

        SearchResponse rsp = req.get();
        assertEquals(3, rsp.getHits().getTotalHits());

        Stats stats = rsp.getAggregations().get("int_agg");
        assertEquals(39.0, stats.getMax(), 0.0001);
        assertEquals(15.0, stats.getMin(), 0.0001);

        stats = rsp.getAggregations().get("double_agg");
        assertEquals(0.7, stats.getMax(), 0.0001);
        assertEquals(0.1, stats.getMin(), 0.0001);
    }

    public void testStringSpecialValueVariable() throws Exception {
        // i.e. expression script for term aggregations, which is not allowed
        createIndex("test");
        ensureGreen("test");
        indexRandom(true,
                client().prepareIndex("test", "doc", "1").setSource("text", "hello"),
                client().prepareIndex("test", "doc", "2").setSource("text", "goodbye"),
                client().prepareIndex("test", "doc", "3").setSource("text", "hello"));

        SearchRequestBuilder req = new SearchRequestBuilder(client()).setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
           .addAggregation(AggregationBuilders.terms("term_agg").field("text").script("_value").lang("expression"));

        String message;
        try {
            // shards that don't have docs with the "text" field will not fail,
            // so we may or may not get a total failure
            SearchResponse rsp = req.get();
            assertThat(rsp.getShardFailures().length, greaterThan(0)); // at least the shards containing the docs should have failed
            message = rsp.getShardFailures()[0].reason();
        } catch (SearchPhaseExecutionException e) {
            message = ExceptionsHelper.detailedMessage(e);
        }
        assertThat(message + "should have contained ExpressionScriptExecutionException",
                   message.contains("ExpressionScriptExecutionException"), equalTo(true));
        assertThat(message + "should have contained text variable error",
                   message.contains("text variable"), equalTo(true));
    }
}
