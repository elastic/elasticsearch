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


package org.elasticsearch.script.groovy;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GroovyIndexedScriptTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put("script.engine.groovy.stored.update", "false");
        builder.put("script.engine.groovy.stored.search", "true");
        builder.put("script.engine.groovy.stored.aggs", "true");
        builder.put("script.engine.groovy.inline.aggs", "false");
        return builder.build();
    }

    public void testFieldIndexedScript()  throws ExecutionException, InterruptedException {
        client().admin().cluster().preparePutStoredScript()
                .setId("script1")
                .setScriptLang("groovy")
                .setSource(new BytesArray("{ \"script\" : \"2\"}"))
                .get();
        client().admin().cluster().preparePutStoredScript()
                .setId("script2")
                .setScriptLang("groovy")
                .setSource(new BytesArray("{ \"script\" : \"factor * 2\"}"))
                .get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "2").setSource("{\"theField\":\"foo 2\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "3").setSource("{\"theField\":\"foo 3\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "4").setSource("{\"theField\":\"foo 4\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "5").setSource("{\"theField\":\"bar\"}"));

        indexRandom(true, builders);
        Map<String, Object> script2Params = new HashMap<>();
        script2Params.put("factor", 3);
        SearchResponse searchResponse = client()
                .prepareSearch()
                .setSource(
                        new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1)
                                .scriptField("test1", new Script("script1", ScriptType.STORED, "groovy", null))
                                .scriptField("test2", new Script("script2", ScriptType.STORED, "groovy", script2Params)))
                .setIndices("test").setTypes("scriptTest").get();
        assertHitCount(searchResponse, 5);
        assertTrue(searchResponse.getHits().hits().length == 1);
        SearchHit sh = searchResponse.getHits().getAt(0);
        assertThat(sh.field("test1").getValue(), equalTo(2));
        assertThat(sh.field("test2").getValue(), equalTo(6));
    }

    // Relates to #10397
    public void testUpdateScripts() {
        createIndex("test_index");
        ensureGreen("test_index");
        client().prepareIndex("test_index", "test_type", "1").setSource("{\"foo\":\"bar\"}").get();
        flush("test_index");

        int iterations = randomIntBetween(2, 11);
        for (int i = 1; i < iterations; i++) {
            assertAcked(client().admin().cluster().preparePutStoredScript()
                    .setScriptLang(GroovyScriptEngineService.NAME)
                    .setId("script1")
                    .setSource(new BytesArray("{\"script\":\"" + i + "\"}")));
            SearchResponse searchResponse = client()
                    .prepareSearch()
                    .setSource(
                            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).scriptField("test_field",
                                    new Script("script1", ScriptType.STORED, "groovy", null))).setIndices("test_index")
                    .setTypes("test_type").get();
            assertHitCount(searchResponse, 1);
            SearchHit sh = searchResponse.getHits().getAt(0);
            assertThat(sh.field("test_field").getValue(), equalTo(i));
        }
    }

    public void testDisabledUpdateIndexedScriptsOnly() {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setScriptLang(GroovyScriptEngineService.NAME)
                .setId("script1")
                .setSource(new BytesArray("{\"script\":\"2\"}")));
        client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}").get();
        try {
            client().prepareUpdate("test", "scriptTest", "1")
                    .setScript(new Script("script1", ScriptService.ScriptType.STORED, GroovyScriptEngineService.NAME, null)).get();
            fail("update script should have been rejected");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("failed to execute script"));
            assertThat(ExceptionsHelper.detailedMessage(e),
                    containsString("scripts of type [stored], operation [update] and lang [groovy] are disabled"));
        }
    }

    public void testDisabledAggsDynamicScripts() {
        //dynamic scripts don't need to be enabled for an indexed script to be indexed and later on executed
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setScriptLang(GroovyScriptEngineService.NAME)
                .setId("script1")
                .setSource(new BytesArray("{\"script\":\"2\"}")));
        client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}").get();
        refresh();
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setSource(
                        new SearchSourceBuilder().aggregation(AggregationBuilders.terms("test").script(
                                new Script("script1", ScriptType.STORED, null, null)))).get();
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getAggregations().get("test"), notNullValue());
    }
}
