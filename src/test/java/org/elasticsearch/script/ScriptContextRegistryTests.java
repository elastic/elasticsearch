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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

public class ScriptContextRegistryTests extends ElasticsearchTestCase {

    @Test
    public void testValidateCustomScriptContextsOperation() throws IOException {
        for (final String rejectedContext : ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS) {
            try {
                //try to register a prohibited script context
                new ScriptContextRegistry(Lists.newArrayList(new ScriptContext.Plugin("test", rejectedContext)));
                fail("ScriptContextRegistry initialization should have failed");
            } catch(ElasticsearchIllegalArgumentException e) {
                assertThat(e.getMessage(), Matchers.containsString("[" + rejectedContext + "] is a reserved name, it cannot be registered as a custom script context"));
            }
        }
    }

    @Test
    public void testValidateCustomScriptContextsPluginName() throws IOException {
        for (final String rejectedContext : ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS) {
            try {
                //try to register a prohibited script context
                new ScriptContextRegistry(Lists.newArrayList(new ScriptContext.Plugin(rejectedContext, "test")));
                fail("ScriptContextRegistry initialization should have failed");
            } catch(ElasticsearchIllegalArgumentException e) {
                assertThat(e.getMessage(), Matchers.containsString("[" + rejectedContext + "] is a reserved name, it cannot be registered as a custom script context"));
            }
        }
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testValidateCustomScriptContextsEmptyPluginName() throws IOException {
        new ScriptContext.Plugin(randomBoolean() ? null : "", "test");
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testValidateCustomScriptContextsEmptyOperation() throws IOException {
        new ScriptContext.Plugin("test", randomBoolean() ? null : "");
    }

    @Test
    public void testDuplicatedPluginScriptContexts() throws IOException {
        try {
            //try to register a prohibited script context
            new ScriptContextRegistry(Lists.newArrayList(new ScriptContext.Plugin("testplugin", "test"), new ScriptContext.Plugin("testplugin", "test")));
            fail("ScriptContextRegistry initialization should have failed");
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.containsString("script context [testplugin_test] cannot be registered twice"));
        }
    }

    @Test
    public void testNonDuplicatedPluginScriptContexts() throws IOException {
        new ScriptContextRegistry(Lists.newArrayList(new ScriptContext.Plugin("testplugin1", "test"), new ScriptContext.Plugin("testplugin2", "test")));
    }
}
