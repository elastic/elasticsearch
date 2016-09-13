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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchTimeoutIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testSimpleTimeout() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(new Script(NativeTestScriptedTimeout.TEST_NATIVE_SCRIPT_TIMEOUT, ScriptType.INLINE, "native", null)))
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }

    public static class ScriptedTimeoutPlugin extends Plugin implements ScriptPlugin {
        @Override
        public List<NativeScriptFactory> getNativeScripts() {
            return Collections.singletonList(new NativeTestScriptedTimeout.Factory());
        }
    }

    public static class NativeTestScriptedTimeout extends AbstractSearchScript {

        public static final String TEST_NATIVE_SCRIPT_TIMEOUT = "native_test_search_timeout_script";

        public static class Factory implements NativeScriptFactory {

            @Override
            public ExecutableScript newScript(Map<String, Object> params) {
                return new NativeTestScriptedTimeout();
            }

            @Override
            public boolean needsScores() {
                return false;
            }

            @Override
            public String getName() {
                return TEST_NATIVE_SCRIPT_TIMEOUT;
            }
        }

        @Override
        public Object run() {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

}
