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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test that a system where the sandbox is disabled while dynamic scripting is
 * also disabled does not allow a script to be sent
 */
@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE)
public class SandboxDisabledTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false)
                .put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, true)
                .build();
    }

    @Test
    public void testScriptingDisabledWhileSandboxDisabled() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();
        try {
            client().prepareSearch("test")
                    .setSource("{\"query\": {\"match_all\": {}}," +
                            "\"sort\":{\"_script\": {\"script\": \"doc['foo'].value + 2\", \"type\": \"number\", \"lang\": \"groovy\"}}}").get();
            fail("shards should fail because the sandbox and dynamic scripting are disabled");
        } catch (Exception e) {
            assertThat(e.getMessage().contains("dynamic scripting for [groovy] disabled"), equalTo(true));
        }
    }

}
