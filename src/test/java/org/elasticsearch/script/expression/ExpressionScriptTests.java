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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ExpressionScriptTests extends ElasticsearchIntegrationTest {

    private SearchResponse runScript(String script, Object... params) {
        ensureGreen("test");
        StringBuilder buf = new StringBuilder();
        buf.append("{query: {match_all:{}}, sort: [{\"_uid\": {\"order\":\"asc\"}}], script_fields: {foo: {lang: \"expression\", script: \"" + script + "\"");
        if (params.length > 0) {
            assert(params.length % 2 == 0);
            buf.append(",params:{");
            for (int i = 0; i < params.length; i += 2) {
                if (i != 0) buf.append(",");
                buf.append("\"" + params[i] + "\":");
                Object v = params[i + 1];
                if (v instanceof String) {
                    buf.append("\"" + v + "\"");
                } else {
                    buf.append(v);
                }
            }
            buf.append("}");
        }
        buf.append("}}}");
        return client().prepareSearch("test").setSource(buf.toString()).get();
    }

    public void testBasic() throws Exception {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex("test", "doc", "1").setSource("foo", 4).setRefresh(true).get();
        SearchResponse rsp = runScript("doc['foo'].value + 1");
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
        String req = "{query: {function_score: {query:{term:{text:\"hello\"}}," +
                "boost_mode: \"replace\"," +
                "script_score: {" +
                    "script: \"1 / _score\"," + // invert the order given by score
                    "lang: \"expression\"" +
                "}}}}";
        SearchResponse rsp = client().prepareSearch("test").setSource(req).get();
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
        SearchResponse rsp = runScript("doc['x'].value + 1");
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
            runScript("doc['bogus'].value");
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
        SearchResponse rsp = runScript("doc['x'].value * a + b + ((c + doc['x'].value) > 5000000009 ? 1 : 0)", "a", 2, "b", 3.5, "c", 5000000000L);
        SearchHits hits = rsp.getHits();
        assertEquals(3, hits.getTotalHits());
        assertEquals(24.5, hits.getAt(0).field("foo").getValue());
        assertEquals(9.5, hits.getAt(1).field("foo").getValue());
        assertEquals(13.5, hits.getAt(2).field("foo").getValue());
    }

    public void testCompileFailure() {
        client().prepareIndex("test", "doc", "1").setSource("x", 1).setRefresh(true).get();
        try {
            runScript("garbage%@#%@");
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
            runScript("a", "a", "astring");
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
            runScript("doc['text'].value");
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
            runScript("bogus");
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
            runScript("doc");
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
            runScript("doc['foo'].bogus");
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
        String req = "{query: {match_all:{}}, aggs: {" +
                          "int_agg: {stats: {" +
                              "field: \"x\"," +
                              "script: \"_value * 3\"," +
                              "lang: \"expression\"" +
                           "}},double_agg: {stats: {" +
                              "field: \"y\"," +
                              "script: \"_value - 1.1\"," +
                              "lang: \"expression\"" +
                     "}}}}";
        SearchResponse rsp = client().prepareSearch("test").setSource(req).get();
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
        String req = "{query: {match_all:{}}, aggs: {term_agg: {terms: {" +
                        "field: \"text\"," +
                        "script: \"_value\"," +
                        "lang: \"expression\"" +
                     "}}}}";

        String message;
        try {
            // shards that don't have docs with the "text" field will not fail,
            // so we may or may not get a total failure
            SearchResponse rsp = client().prepareSearch("test").setSource(req).get();
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
