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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

//TODO: please convert to unit tests!
public class IndexedExpressionTests extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put("script.engine.expression.stored.update", "false");
        builder.put("script.engine.expression.stored.search", "false");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExpressionPlugin.class);
    }

    public void testAllOpsDisabledIndexedScripts() throws IOException {
        client().admin().cluster().preparePutStoredScript()
                .setScriptLang(ExpressionScriptEngineService.NAME)
                .setId("script1")
                .setSource(new BytesArray("{\"script\":\"2\"}"))
                .get();
        client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}").get();
        try {
            client().prepareUpdate("test", "scriptTest", "1")
                    .setScript(new Script("script1", ScriptService.ScriptType.STORED, ExpressionScriptEngineService.NAME, null)).get();
            fail("update script should have been rejected");
        } catch(Exception e) {
            assertThat(e.getMessage(), containsString("failed to execute script"));
            assertThat(e.getCause().getMessage(), containsString("scripts of type [stored], operation [update] and lang [expression] are disabled"));
        }
        try {
            client().prepareSearch()
                    .setSource(
                            new SearchSourceBuilder().scriptField("test1", new Script("script1", ScriptType.STORED, "expression", null)))
                    .setIndices("test").setTypes("scriptTest").get();
            fail("search script should have been rejected");
        } catch(Exception e) {
            assertThat(e.toString(), containsString("scripts of type [stored], operation [search] and lang [expression] are disabled"));
        }
        try {
            client().prepareSearch("test")
                    .setSource(
                            new SearchSourceBuilder().aggregation(AggregationBuilders.terms("test").script(
                                    new Script("script1", ScriptType.STORED, "expression", null)))).get();
        } catch (Exception e) {
            assertThat(e.toString(), containsString("scripts of type [stored], operation [aggs] and lang [expression] are disabled"));
        }
    }
}
