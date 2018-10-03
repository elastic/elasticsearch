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

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BindingsTests extends ScriptTestCase {

    public abstract static class BindingsTestScript {
        public static final String[] PARAMETERS = { "test", "bound" };
        public abstract int execute(int test, int bound);
        public interface Factory {
            BindingsTestScript newInstance();
        }
        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("bindings_test", Factory.class);
    }

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        contexts.put(BindingsTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public void testBasicBinding() {
        assertEquals(15, exec("testAddWithState(4, 5, 6, 0.0)"));
    }

    public void testRepeatedBinding() {
        String script = "testAddWithState(4, 5, test, 0.0)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(14, executableScript.execute(5, 0));
        assertEquals(13, executableScript.execute(4, 0));
        assertEquals(16, executableScript.execute(7, 0));
    }

    public void testBoundBinding() {
        String script = "testAddWithState(4, bound, test, 0.0)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(10, executableScript.execute(5, 1));
        assertEquals(9, executableScript.execute(4, 2));
    }
}
