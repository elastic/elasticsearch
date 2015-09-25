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
package org.elasticsearch.script;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

//Use Suite scope so that paths get set correctly
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class OnDiskScriptIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        //Set path so ScriptService will pick up the test scripts
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", this.getDataPath("config"))
                .put("script.engine.expression.file.aggs", "off")
                .put("script.engine.mustache.file.aggs", "off")
                .put("script.engine.mustache.file.search", "off")
                .put("script.engine.mustache.file.mapping", "off")
                .put("script.engine.mustache.file.update", "off").build();
    }

    @Test
    public void testFieldOnDiskScript()  throws ExecutionException, InterruptedException {
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
                                .scriptField("test1", new Script("script1", ScriptType.FILE, null, null))
                                .scriptField("test2", new Script("script2", ScriptType.FILE, null, script2Params))).setIndices("test")
                .setTypes("scriptTest").get();
        assertHitCount(searchResponse, 5);
        assertTrue(searchResponse.getHits().hits().length == 1);
        SearchHit sh = searchResponse.getHits().getAt(0);
        assertThat((Integer) sh.field("test1").getValue(), equalTo(2));
        assertThat((Integer) sh.field("test2").getValue(), equalTo(6));
    }

    @Test
    public void testAllOpsDisabledOnDiskScripts() {
        //whether we even compile or cache the on disk scripts doesn't change the end result (the returned error)
        client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}").get();
        refresh();
        try {
            client().prepareSearch("test")
                    .setSource(
                            new SearchSourceBuilder().aggregation(AggregationBuilders.terms("test").script(
                                    new Script("script1", ScriptType.FILE, MustacheScriptEngineService.NAME, null)))).get();
            fail("aggs script should have been rejected");
        } catch (Exception e) {
            assertThat(e.toString(), containsString("scripts of type [file], operation [aggs] and lang [mustache] are disabled"));
        }
        try {
            client().prepareSearch()
                    .setSource(
                            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1)
                                    .scriptField("test1", new Script("script1", ScriptType.FILE, MustacheScriptEngineService.NAME, null)))
                    .setIndices("test").setTypes("scriptTest").get();
            fail("search script should have been rejected");
        } catch (Exception e) {
            assertThat(e.toString(), containsString("scripts of type [file], operation [search] and lang [mustache] are disabled"));
        }
        try {
            client().prepareUpdate("test", "scriptTest", "1")
                    .setScript(new Script("script1", ScriptService.ScriptType.FILE, MustacheScriptEngineService.NAME, null)).get();
            fail("update script should have been rejected");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("failed to execute script"));
            assertThat(e.getCause().getMessage(),
                    containsString("scripts of type [file], operation [update] and lang [mustache] are disabled"));
        }
    }

}
