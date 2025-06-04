/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptScoreQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThirdHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;

public class ScriptScoreQueryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['field2'].value * param1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles field2Values = (ScriptDocValues.Doubles) doc.get("field2");
                Double param1 = (Double) vars.get("param1");
                return field2Values.getValue() * param1;
            });
            return scripts;
        }
    }

    // test that script_score works as expected:
    // 1) only matched docs retrieved
    // 2) score is calculated based on a script with params
    // 3) min score applied
    public void testScriptScore() {
        assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            prepareIndex("test-index").setId("" + i).setSource("field1", "text" + (i % 2), "field2", i).get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        assertNoFailuresAndResponse(
            prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)),
            response -> {
                assertOrderedSearchHits(response, "10", "8", "6", "4", "2");
                assertFirstHit(response, hasScore(1.0f));
                assertSecondHit(response, hasScore(0.8f));
                assertThirdHit(response, hasScore(0.6f));
            }
        );

        // applying min score
        assertNoFailuresAndResponse(
            prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script).setMinScore(0.6f)),
            response -> assertOrderedSearchHits(response, "10", "8", "6")
        );
    }

    public void testScriptScoreBoolQuery() {
        assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            prepareIndex("test-index").setId("" + i).setSource("field1", "text" + i, "field2", i).get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        QueryBuilder boolQuery = boolQuery().should(matchQuery("field1", "text1")).should(matchQuery("field1", "text10"));
        assertNoFailuresAndResponse(prepareSearch("test-index").setQuery(scriptScoreQuery(boolQuery, script)), response -> {
            assertOrderedSearchHits(response, "10", "1");
            assertFirstHit(response, hasScore(1.0f));
            assertSecondHit(response, hasScore(0.1f));
        });
    }

    // test that when the internal query is rewritten script_score works well
    public void testRewrittenQuery() {
        assertAcked(
            prepareCreate("test-index2").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=date", "field2", "type=double")
        );
        prepareIndex("test-index2").setId("1").setSource("field1", "2019-09-01", "field2", 1).get();
        prepareIndex("test-index2").setId("2").setSource("field1", "2019-10-01", "field2", 2).get();
        prepareIndex("test-index2").setId("3").setSource("field1", "2019-11-01", "field2", 3).get();
        refresh();

        RangeQueryBuilder rangeQB = new RangeQueryBuilder("field1").from("2019-01-01"); // the query should be rewritten to from:null
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", Map.of("param1", 0.1));
        assertNoFailuresAndResponse(
            prepareSearch("test-index2").setQuery(scriptScoreQuery(rangeQB, script)),
            response -> assertOrderedSearchHits(response, "3", "2", "1")
        );
    }

    public void testDisallowExpensiveQueries() {
        try {
            assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
            int docCount = 10;
            for (int i = 1; i <= docCount; i++) {
                prepareIndex("test-index").setId("" + i).setSource("field1", "text" + (i % 2), "field2", i).get();
            }
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = true => success
            Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", Map.of("param1", 0.1));
            assertNoFailures(prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)));

            // Set search.allow_expensive_queries to "false" => assert failure
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", false));

            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
            );
            assertEquals(
                "[script score] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getCause().getMessage()
            );

            // Set search.allow_expensive_queries to "true" => success
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertNoFailures(prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)));
        } finally {
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
        }
    }
}
