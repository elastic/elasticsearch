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

package org.elasticsearch.painless;

import org.elasticsearch.script.ScriptContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class FactoryTests extends ScriptTestCase {
    public abstract static class FactoryTestScript {
        private final Map<String, Object> params;

        public FactoryTestScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public interface Factory {
            FactoryTestScript newInstance(Map<String, Object> params);
        }

        public static final ScriptContext<FactoryTests.FactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTests.FactoryTestScript.Factory.class);
    }

    protected Collection<ScriptContext<?>> scriptContexts() {
        Collection<ScriptContext<?>> contexts = super.scriptContexts();
        contexts.add(FactoryTestScript.CONTEXT);

        return contexts;
    }

    public void testFactory() {
        FactoryTestScript.Factory factory =
            scriptEngine.compile("factory_test", "test + params.get('test')", FactoryTestScript.CONTEXT, Collections.EMPTY_MAP);
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("test", 2));
        assertEquals(4, script.execute(2));
    }
}
