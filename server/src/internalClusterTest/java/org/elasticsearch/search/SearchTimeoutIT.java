/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.search.SearchTimeoutIT.ScriptedTimeoutPlugin.SCRIPT_NAME;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchTimeoutIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    public void testSimpleTimeout() throws Exception {
        for (int i = 0; i < 32; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        refresh("test");

        SearchResponse searchResponse = client().prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(
                    new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .setAllowPartialSearchResults(true)
                .get();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }

    public void testPartialResultsIntolerantTimeout() throws Exception {
        client().prepareIndex("test").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () ->
            client().prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                    .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
                    .get()
                );
        assertTrue(ex.toString().contains("Time exceeded"));
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
