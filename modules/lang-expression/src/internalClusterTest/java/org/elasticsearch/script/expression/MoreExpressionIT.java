/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.expression;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.bucketScript;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

// TODO: please convert to unit tests!
public class MoreExpressionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExpressionPlugin.class);
    }

    private SearchRequestBuilder buildRequest(String script, Object... params) {
        ensureGreen("test");

        Map<String, Object> paramsMap = new HashMap<>();
        assert (params.length % 2 == 0);
        for (int i = 0; i < params.length; i += 2) {
            paramsMap.put(params[i].toString(), params[i + 1]);
        }

        SearchRequestBuilder req = prepareSearch().setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
            .addSort(SortBuilders.fieldSort("id").order(SortOrder.ASC).unmappedType("long"))
            .addScriptField("foo", new Script(ScriptType.INLINE, "expression", script, paramsMap));
        return req;
    }

    public void testBasic() throws Exception {
        createIndex("test");
        ensureGreen("test");
        prepareIndex("test").setId("1").setSource("foo", 4).setRefreshPolicy(IMMEDIATE).get();
        assertResponse(buildRequest("doc['foo'] + 1"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(5.0, rsp.getHits().getAt(0).field("foo").getValue(), 0.0D);
        });
    }

    public void testFunction() throws Exception {
        createIndex("test");
        ensureGreen("test");
        prepareIndex("test").setId("1").setSource("foo", 4).setRefreshPolicy(IMMEDIATE).get();
        assertNoFailuresAndResponse(buildRequest("doc['foo'] + abs(1)"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(5.0, rsp.getHits().getAt(0).field("foo").getValue(), 0.0D);
        });
    }

    public void testBasicUsingDotValue() throws Exception {
        createIndex("test");
        ensureGreen("test");

        prepareIndex("test").setId("1").setSource("foo", 4).setRefreshPolicy(IMMEDIATE).get();
        assertResponse(buildRequest("doc['foo'].value + 1"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(5.0, rsp.getHits().getAt(0).field("foo").getValue(), 0.0D);
        });
    }

    public void testScore() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("text", "hello goodbye"),
            prepareIndex("test").setId("2").setSource("text", "hello hello hello goodbye"),
            prepareIndex("test").setId("3").setSource("text", "hello hello goodebye")
        );
        ScriptScoreFunctionBuilder score = ScoreFunctionBuilders.scriptFunction(
            new Script(ScriptType.INLINE, "expression", "1 / _score", Collections.emptyMap())
        );
        SearchRequestBuilder req = prepareSearch().setIndices("test");
        req.setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("text", "hello"), score).boostMode(CombineFunction.REPLACE));
        req.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // make sure DF is consistent
        assertResponse(req, rsp -> {
            assertNoFailures(rsp);
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals("1", hits.getAt(0).getId());
            assertEquals("3", hits.getAt(1).getId());
            assertEquals("2", hits.getAt(2).getId());
        });

        req = prepareSearch().setIndices("test");
        req.setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("text", "hello"), score).boostMode(CombineFunction.REPLACE));
        score = ScoreFunctionBuilders.scriptFunction(new Script(ScriptType.INLINE, "expression", "1 / _score", Collections.emptyMap()));
        req.addAggregation(AggregationBuilders.max("max_score").script((score).getScript()));
        req.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // make sure DF is consistent
        assertNoFailures(req);
    }

    public void testDateMethods() throws Exception {
        ElasticsearchAssertions.assertAcked(prepareCreate("test").setMapping("date0", "type=date", "date1", "type=date"));
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "date0", "2015-04-28T04:02:07Z", "date1", "1985-09-01T23:11:01Z"),
            prepareIndex("test").setId("2").setSource("id", 2, "date0", "2013-12-25T11:56:45Z", "date1", "1983-10-13T23:15:00Z")
        );
        assertResponse(buildRequest("doc['date0'].getSeconds() - doc['date0'].getMinutes()"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(-11.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date0'].getHourOfDay() + doc['date1'].getDayOfMonth()"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(24.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date1'].getMonth() + 1"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(9.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(10.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date1'].getYear()"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(1985.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(1983.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
    }

    public void testDateObjectMethods() throws Exception {
        ElasticsearchAssertions.assertAcked(prepareCreate("test").setMapping("date0", "type=date", "date1", "type=date"));
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "date0", "2015-04-28T04:02:07Z", "date1", "1985-09-01T23:11:01Z"),
            prepareIndex("test").setId("2").setSource("id", 2, "date0", "2013-12-25T11:56:45Z", "date1", "1983-10-13T23:15:00Z")
        );
        assertResponse(buildRequest("doc['date0'].date.secondOfMinute - doc['date0'].date.minuteOfHour"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(-11.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date0'].date.getHourOfDay() + doc['date1'].date.dayOfMonth"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(24.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date1'].date.monthOfYear + 1"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(10.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(11.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
        assertResponse(buildRequest("doc['date1'].date.year"), rsp -> {
            assertEquals(2, rsp.getHits().getTotalHits().value());
            SearchHits hits = rsp.getHits();
            assertEquals(1985.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(1983.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
    }

    public void testMultiValueMethods() throws Exception {
        ElasticsearchAssertions.assertAcked(
            prepareCreate("test").setMapping("double0", "type=double", "double1", "type=double", "double2", "type=double")
        );
        ensureGreen("test");

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("id", 1);
        doc1.put("double0", new Double[] { 5.0d, 1.0d, 1.5d });
        doc1.put("double1", new Double[] { 1.2d, 2.4d });
        doc1.put("double2", 3.0d);

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("id", 2);
        doc2.put("double0", 5.0d);
        doc2.put("double1", 3.0d);

        Map<String, Object> doc3 = new HashMap<>();
        doc3.put("id", 3);
        doc3.put("double0", new Double[] { 5.0d, 1.0d, 1.5d, -1.5d });
        doc3.put("double1", 4.0d);

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource(doc1),
            prepareIndex("test").setId("2").setSource(doc2),
            prepareIndex("test").setId("3").setSource(doc3)
        );

        assertNoFailuresAndResponse(buildRequest("doc['double0'].count() + doc['double1'].count()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(2.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].sum()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(7.5, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(6.0, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].avg() + doc['double1'].avg()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(4.3, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(8.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(5.5, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].median()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(1.5, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(1.25, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].min()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(1.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(-1.5, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].max()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        assertNoFailuresAndResponse(buildRequest("doc['double0'].sum()/doc['double0'].count()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(2.5, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(1.5, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        // make sure count() works for missing
        assertNoFailuresAndResponse(buildRequest("doc['double2'].count()"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(1.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(0.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(0.0, hits.getAt(2).field("foo").getValue(), 0.0D);
        });

        // make sure .empty works in the same way
        assertNoFailuresAndResponse(buildRequest("doc['double2'].empty ? 5.0 : 2.0"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(2.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(5.0, hits.getAt(2).field("foo").getValue(), 0.0D);
        });
    }

    public void testInvalidDateMethodCall() throws Exception {
        ElasticsearchAssertions.assertAcked(prepareCreate("test").setMapping("double", "type=double"));
        ensureGreen("test");
        indexRandom(true, prepareIndex("test").setId("1").setSource("double", "178000000.0"));
        try {
            buildRequest("doc['double'].getYear()").get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(
                e.toString() + "should have contained IllegalArgumentException",
                e.toString().contains("IllegalArgumentException"),
                equalTo(true)
            );
            assertThat(
                e.toString() + "should have contained does not exist for numeric field",
                e.toString().contains("does not exist for numeric field"),
                equalTo(true)
            );
        }
    }

    public void testSparseField() throws Exception {
        ElasticsearchAssertions.assertAcked(prepareCreate("test").setMapping("x", "type=long", "y", "type=long"));
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "x", 4),
            prepareIndex("test").setId("2").setSource("id", 2, "y", 2)
        );
        assertNoFailuresAndResponse(buildRequest("doc['x'] + 1"), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(2, rsp.getHits().getTotalHits().value());
            assertEquals(5.0, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(1.0, hits.getAt(1).field("foo").getValue(), 0.0D);
        });
    }

    public void testMissingField() throws Exception {
        createIndex("test");
        ensureGreen("test");
        prepareIndex("test").setId("1").setSource("x", 4).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("doc['bogus']").get();
            fail("Expected missing field to cause failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained missing field error",
                e.toString().contains("does not exist in mappings"),
                equalTo(true)
            );
        }
    }

    public void testParams() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "x", 10),
            prepareIndex("test").setId("2").setSource("id", 2, "x", 3),
            prepareIndex("test").setId("3").setSource("id", 3, "x", 5)
        );
        // a = int, b = double, c = long
        String script = "doc['x'] * a + b + ((c + doc['x']) > 5000000009 ? 1 : 0)";
        assertResponse(buildRequest(script, "a", 2, "b", 3.5, "c", 5000000000L), rsp -> {
            SearchHits hits = rsp.getHits();
            assertEquals(3, hits.getTotalHits().value());
            assertEquals(24.5, hits.getAt(0).field("foo").getValue(), 0.0D);
            assertEquals(9.5, hits.getAt(1).field("foo").getValue(), 0.0D);
            assertEquals(13.5, hits.getAt(2).field("foo").getValue(), 0.0D);
        });
    }

    public void testCompileFailure() {
        prepareIndex("test").setId("1").setSource("x", 1).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("garbage%@#%@").get();
            fail("Expected expression compilation failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(e.toString() + "should have contained compilation failure", e.toString().contains("compile error"), equalTo(true));
        }
    }

    public void testNonNumericParam() {
        prepareIndex("test").setId("1").setSource("x", 1).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("a", "a", "astring").get();
            fail("Expected string parameter to cause failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained non-numeric parameter error",
                e.toString().contains("must be a numeric type"),
                equalTo(true)
            );
        }
    }

    public void testNonNumericField() {
        prepareIndex("test").setId("1").setSource("text", "this is not a number").setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("doc['text.keyword']").get();
            fail("Expected text field to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained non-numeric field error",
                e.toString().contains("must be numeric"),
                equalTo(true)
            );
        }
    }

    public void testInvalidGlobalVariable() {
        prepareIndex("test").setId("1").setSource("foo", 5).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("bogus").get();
            fail("Expected bogus variable to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained unknown variable error",
                e.toString().contains("Unknown variable"),
                equalTo(true)
            );
        }
    }

    public void testDocWithoutField() {
        prepareIndex("test").setId("1").setSource("foo", 5).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("doc").get();
            fail("Expected doc variable without field to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained a missing specific field error",
                e.toString().contains("must be used with a specific field"),
                equalTo(true)
            );
        }
    }

    public void testInvalidFieldMember() {
        prepareIndex("test").setId("1").setSource("foo", 5).setRefreshPolicy(IMMEDIATE).get();
        try {
            buildRequest("doc['foo'].bogus").get();
            fail("Expected bogus field member to cause execution failure");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString() + "should have contained ScriptException", e.toString().contains("ScriptException"), equalTo(true));
            assertThat(
                e.toString() + "should have contained member variable [bogus] does not exist",
                e.toString().contains("Member variable [bogus] does not exist"),
                equalTo(true)
            );
        }
    }

    public void testSpecialValueVariable() throws Exception {
        // i.e. _value for aggregations
        createIndex("test");
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("x", 5, "y", 1.2),
            prepareIndex("test").setId("2").setSource("x", 10, "y", 1.4),
            prepareIndex("test").setId("3").setSource("x", 13, "y", 1.8)
        );

        SearchRequestBuilder req = prepareSearch().setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                AggregationBuilders.stats("int_agg")
                    .field("x")
                    .script(new Script(ScriptType.INLINE, ExpressionScriptEngine.NAME, "_value * 3", Collections.emptyMap()))
            )
            .addAggregation(
                AggregationBuilders.stats("double_agg")
                    .field("y")
                    .script(new Script(ScriptType.INLINE, ExpressionScriptEngine.NAME, "_value - 1.1", Collections.emptyMap()))
            )
            .addAggregation(
                AggregationBuilders.stats("const_agg")
                    .field("x") // specifically to test a script w/o _value
                    .script(new Script(ScriptType.INLINE, ExpressionScriptEngine.NAME, "3.0", Collections.emptyMap()))
            );

        assertResponse(req, rsp -> {
            assertEquals(3, rsp.getHits().getTotalHits().value());

            Stats stats = rsp.getAggregations().get("int_agg");
            assertEquals(39.0, stats.getMax(), 0.0001);
            assertEquals(15.0, stats.getMin(), 0.0001);

            stats = rsp.getAggregations().get("double_agg");
            assertEquals(0.7, stats.getMax(), 0.0001);
            assertEquals(0.1, stats.getMin(), 0.0001);

            stats = rsp.getAggregations().get("const_agg");
            assertThat(stats.getMax(), equalTo(3.0));
            assertThat(stats.getMin(), equalTo(3.0));
            assertThat(stats.getAvg(), equalTo(3.0));
        });
    }

    public void testStringSpecialValueVariable() throws Exception {
        // i.e. expression script for term aggregations, which is not allowed
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("text", "type=keyword").get());
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("text", "hello"),
            prepareIndex("test").setId("2").setSource("text", "goodbye"),
            prepareIndex("test").setId("3").setSource("text", "hello")
        );

        SearchRequestBuilder req = prepareSearch().setIndices("test");
        req.setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                AggregationBuilders.terms("term_agg")
                    .field("text")
                    .script(new Script(ScriptType.INLINE, ExpressionScriptEngine.NAME, "_value", Collections.emptyMap()))
            );

        AtomicReference<String> message = new AtomicReference<>();
        try {
            // shards that don't have docs with the "text" field will not fail,
            // so we may or may not get a total failure
            assertResponse(req, rsp -> {
                assertThat(rsp.getShardFailures().length, greaterThan(0)); // at least the shards containing the docs should have failed
                message.set(rsp.getShardFailures()[0].reason());
            });
        } catch (SearchPhaseExecutionException e) {
            message.set(e.toString());
        }
        assertThat(message + "should have contained ScriptException", message.get().contains("ScriptException"), equalTo(true));
        assertThat(message + "should have contained text variable error", message.get().contains("text variable"), equalTo(true));
    }

    // test to make sure expressions are not allowed to be used as update scripts
    public void testInvalidUpdateScript() throws Exception {
        try {
            createIndex("test_index");
            ensureGreen("test_index");
            indexRandom(true, prepareIndex("test_index").setId("1").setSource("text_field", "text"));
            UpdateRequestBuilder urb = client().prepareUpdate().setIndex("test_index");
            urb.setId("1");
            urb.setScript(new Script(ScriptType.INLINE, ExpressionScriptEngine.NAME, "0", Collections.emptyMap()));
            urb.get();
            fail("Expression scripts should not be allowed to run as update scripts.");
        } catch (Exception e) {
            String message = e.getMessage();
            assertThat(message + " should have contained failed to execute", message.contains("failed to execute"), equalTo(true));
            message = e.getCause().getMessage();
            assertThat(message, equalTo("Failed to compile inline script [0] using lang [expression]"));
        }
    }

    // test to make sure expressions are allowed to be used for reduce in pipeline aggregations
    public void testPipelineAggregationScript() throws Exception {
        createIndex("agg_index");
        ensureGreen("agg_index");
        indexRandom(
            true,
            prepareIndex("agg_index").setId("1").setSource("one", 1.0, "two", 2.0, "three", 3.0, "four", 4.0),
            prepareIndex("agg_index").setId("2").setSource("one", 2.0, "two", 2.0, "three", 3.0, "four", 4.0),
            prepareIndex("agg_index").setId("3").setSource("one", 3.0, "two", 2.0, "three", 3.0, "four", 4.0),
            prepareIndex("agg_index").setId("4").setSource("one", 4.0, "two", 2.0, "three", 3.0, "four", 4.0),
            prepareIndex("agg_index").setId("5").setSource("one", 5.0, "two", 2.0, "three", 3.0, "four", 4.0)
        );
        assertResponse(
            prepareSearch("agg_index").addAggregation(
                histogram("histogram").field("one")
                    .interval(2)
                    .subAggregation(sum("twoSum").field("two"))
                    .subAggregation(sum("threeSum").field("three"))
                    .subAggregation(sum("fourSum").field("four"))
                    .subAggregation(
                        bucketScript(
                            "totalSum",
                            new Script(
                                ScriptType.INLINE,
                                ExpressionScriptEngine.NAME,
                                "_value0 + _value1 + _value2",
                                Collections.emptyMap()
                            ),
                            "twoSum",
                            "threeSum",
                            "fourSum"
                        )
                    )
            ),
            response -> {
                Histogram histogram = response.getAggregations().get("histogram");
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histogram"));
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();

                for (int bucketCount = 0; bucketCount < buckets.size(); ++bucketCount) {
                    Histogram.Bucket bucket = buckets.get(bucketCount);
                    if (bucket.getDocCount() == 1) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("totalSum");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertEquals(9.0, seriesArithmeticValue, 0.001);
                    } else if (bucket.getDocCount() == 2) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("totalSum");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertEquals(18.0, seriesArithmeticValue, 0.001);
                    } else {
                        fail("Incorrect number of documents in a bucket in the histogram.");
                    }
                }
            }
        );
    }

    public void testGeo() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setMapping(xContentBuilder));
        ensureGreen();
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "test")
                    .startObject("location")
                    .field("lat", 61.5240)
                    .field("lon", 105.3188)
                    .endObject()
                    .endObject()
            )
            .get();
        refresh();
        // access .lat
        assertNoFailuresAndResponse(buildRequest("doc['location'].lat"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(61.5240, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
        });
        // access .lon
        assertNoFailuresAndResponse(buildRequest("doc['location'].lon"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(105.3188, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
        });
        // access .empty
        assertNoFailuresAndResponse(buildRequest("doc['location'].empty ? 1 : 0"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(0, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
        });
        // call haversin
        assertNoFailuresAndResponse(buildRequest("haversin(38.9072, 77.0369, doc['location'].lat, doc['location'].lon)"), rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(3170D, rsp.getHits().getAt(0).field("foo").getValue(), 50D);
        });
    }

    public void testBoolean() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("vip")
            .field("type", "boolean");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setMapping(xContentBuilder));
        ensureGreen();
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "price", 1.0, "vip", true),
            prepareIndex("test").setId("2").setSource("id", 2, "price", 2.0, "vip", false),
            prepareIndex("test").setId("3").setSource("id", 3, "price", 2.0, "vip", false)
        );
        // access .value
        assertNoFailuresAndResponse(buildRequest("doc['vip'].value"), rsp -> {
            assertEquals(3, rsp.getHits().getTotalHits().value());
            assertEquals(1.0D, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
            assertEquals(0.0D, rsp.getHits().getAt(1).field("foo").getValue(), 1.0D);
            assertEquals(0.0D, rsp.getHits().getAt(2).field("foo").getValue(), 1.0D);
        });
        // access .empty
        assertNoFailuresAndResponse(buildRequest("doc['vip'].empty ? 1 : 0"), rsp -> {
            assertEquals(3, rsp.getHits().getTotalHits().value());
            assertEquals(0.0D, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
            assertEquals(0.0D, rsp.getHits().getAt(1).field("foo").getValue(), 1.0D);
            assertEquals(1.0D, rsp.getHits().getAt(2).field("foo").getValue(), 1.0D);
        });
        // ternary operator
        // vip's have a 50% discount
        assertNoFailuresAndResponse(buildRequest("doc['vip'] ? doc['price']/2 : doc['price']"), rsp -> {
            assertEquals(3, rsp.getHits().getTotalHits().value());
            assertEquals(0.5D, rsp.getHits().getAt(0).field("foo").getValue(), 1.0D);
            assertEquals(2.0D, rsp.getHits().getAt(1).field("foo").getValue(), 1.0D);
            assertEquals(2.0D, rsp.getHits().getAt(2).field("foo").getValue(), 1.0D);
        });
    }

    public void testFilterScript() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("id", 1, "foo", 1.0),
            prepareIndex("test").setId("2").setSource("id", 2, "foo", 0.0)
        );
        SearchRequestBuilder builder = buildRequest("doc['foo'].value");
        Script script = new Script(ScriptType.INLINE, "expression", "doc['foo'].value", Collections.emptyMap());
        builder.setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.scriptQuery(script)));
        assertNoFailuresAndResponse(builder, rsp -> {
            assertEquals(1, rsp.getHits().getTotalHits().value());
            assertEquals(1.0D, rsp.getHits().getAt(0).field("foo").getValue(), 0.0D);
        });
    }
}
