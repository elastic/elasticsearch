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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.MigrateIndexTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class MigrateIndexWithReindexTests extends MigrateIndexTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CustomScriptPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    public void testMigrateNonEmptyIndex() throws InterruptedException, ExecutionException, TimeoutException {
        int docCount = between(1, 1000);
        Script script = new Script("ctx._source.foo += ' cat'", ScriptType.INLINE, CustomScriptPlugin.NAME, emptyMap());
        migrateIndexTestCase(docCount, script);
        
        // And now the script is applied to the alias! Huzzah!
        SearchResponse searchResponse = client().prepareSearch("test").setSize(0).setQuery(matchQuery("foo", "cat")).get();
        assertHitCount(searchResponse, docCount);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("ctx._source.foo += ' cat'", vars -> {
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                source.put("foo", source.get("foo") + " cat");
                return null;
            });

            return scripts;
        }
    }
}
