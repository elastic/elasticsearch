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
package org.elasticsearch.update;

import com.google.common.collect.Maps;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class UpdateByNativeScriptTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("script.native.custom.type", CustomNativeScriptFactory.class.getName())
                .build();
    }

    @Test
    public void testThatUpdateUsingNativeScriptWorks() throws Exception {
        createIndex("test");
        ensureYellow();

        index("test", "type", "1", "text", "value");

        Map<String, Object> params = Maps.newHashMap();
        params.put("foo", "SETVALUE");
        client().prepareUpdate("test", "type", "1")
                .setScript("custom", ScriptService.ScriptType.INLINE)
                .setScriptLang(NativeScriptEngineService.NAME).setScriptParams(params).get();

        Map<String, Object> data = client().prepareGet("test", "type", "1").get().getSource();
        assertThat(data, hasKey("foo"));
        assertThat(data.get("foo").toString(), is("SETVALUE"));
    }

    static class CustomNativeScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new CustomScript(params);
        }
    }

    static class CustomScript extends AbstractExecutableScript {
        private Map<String, Object> params;
        private Map<String, Object> vars = Maps.newHashMapWithExpectedSize(2);

        public CustomScript(Map<String, Object> params) {
            this.params = params;
        }

        @Override
        public Object run() {
            if (vars.containsKey("ctx") && vars.get("ctx") instanceof Map) {
                Map ctx = (Map) vars.get("ctx");
                if (ctx.containsKey("_source") && ctx.get("_source") instanceof Map) {
                    Map source = (Map) ctx.get("_source");
                    source.putAll(params);
                }
            }
            // return value does not matter, the UpdateHelper class
            return null;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

    }

}
