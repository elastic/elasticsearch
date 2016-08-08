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

import org.elasticsearch.action.admin.indices.migrate.MigrateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
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
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class MigrateIndexTests extends ReindexTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CustomScriptPlugin.class);
        return plugins;
    }

    public void testMigrateIndex() throws InterruptedException, ExecutionException {
        // Index a pile of documents
        int docCount = between(10, 10000);
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex("test_0", "test").setSource("foo", "bar", "i", i));
        }
        indexRandom(true, docs);
        client().admin().indices().prepareAliases().addAlias("test_0", "test").get();

        // They are there, great
        SearchResponse searchResponse = client().prepareSearch("test_0").setSize(0).get();
        assertHitCount(searchResponse, docCount);
        // And you can search for them by alias
        searchResponse = client().prepareSearch("test").setSize(0).get();
        assertHitCount(searchResponse, docCount);

        // Migrate them from "test_0" to "test_1"
        assertFalse(client().admin().indices().prepareMigrateIndex("test_0", "test_1").setAliases("test").get().isNoop());
        // That removes the source index
        assertFalse(client().admin().indices().prepareExists("test_0").get().isExists());

        // But the documents are still there, just in the new index
        searchResponse = client().prepareSearch("test_1").setSize(0).get();
        assertHitCount(searchResponse, docCount);
        searchResponse = client().prepareSearch("test").setSize(0).get();
        assertHitCount(searchResponse, docCount);

        // Doing it again from and to the same index does nothing
        assertTrue(client().admin().indices().prepareMigrateIndex("test_0", "test_1").setAliases("test").get().isNoop());

        // But we can migrate to a new index and actually apply a script
        MigrateIndexRequestBuilder migrate = client().admin().indices().prepareMigrateIndex("test_1", "test_2").setAliases("test");
        migrate.setScript(new Script("ctx._source.foo += ' cat'", ScriptType.INLINE, CustomScriptPlugin.NAME, emptyMap()));
        assertFalse(migrate.get().isNoop());
        
        // And now the script is applied to the alias! Huzzah!
        searchResponse = client().prepareSearch("test").setSize(0).setQuery(matchQuery("foo", "cat")).get();
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
