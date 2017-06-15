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
import org.elasticsearch.script.TemplateScript;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class FactoryTests extends ScriptTestCase {

    protected Collection<ScriptContext<?>> scriptContexts() {
        Collection<ScriptContext<?>> contexts = super.scriptContexts();
        contexts.add(StatefulFactoryTestScript.CONTEXT);
        contexts.add(FactoryTestScript.CONTEXT);
        contexts.add(EmptyTestScript.CONTEXT);
        contexts.add(TemplateScript.CONTEXT);

        return contexts;
    }

    public abstract static class StatefulFactoryTestScript {
        private final int x;
        private final int y;

        public StatefulFactoryTestScript(int x, int y, int a, int b) {
            this.x = x*a;
            this.y = y*b;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y*2;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public interface StatefulFactory {
            StatefulFactoryTestScript newInstance(int a, int b);
        }

        public interface Factory {
            StatefulFactory newFactory(int x, int y);
        }

        public static final ScriptContext<StatefulFactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", StatefulFactoryTestScript.Factory.class);
    }

    public void testStatefulFactory() {
        StatefulFactoryTestScript.Factory factory = scriptEngine.compile(
            "stateful_factory_test", "test + x + y", StatefulFactoryTestScript.CONTEXT, Collections.emptyMap());
        StatefulFactoryTestScript.StatefulFactory statefulFactory = factory.newFactory(1, 2);
        StatefulFactoryTestScript script = statefulFactory.newInstance(3, 4);
        assertEquals(22, script.execute(3));
        statefulFactory.newInstance(5, 6);
        assertEquals(26, script.execute(7));
    }

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

        public static final ScriptContext<FactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTestScript.Factory.class);
    }

    public void testFactory() {
        FactoryTestScript.Factory factory =
            scriptEngine.compile("factory_test", "test + params.get('test')", FactoryTestScript.CONTEXT, Collections.emptyMap());
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("test", 2));
        assertEquals(4, script.execute(2));
        assertEquals(5, script.execute(3));
        script = factory.newInstance(Collections.singletonMap("test", 3));
        assertEquals(5, script.execute(2));
        assertEquals(2, script.execute(-1));
    }

    public abstract static class EmptyTestScript {
        public static final String[] PARAMETERS = {};
        public abstract Object execute();

        public interface Factory {
            EmptyTestScript newInstance();
        }

        public static final ScriptContext<EmptyTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", EmptyTestScript.Factory.class);
    }

    public void testEmpty() {
        EmptyTestScript.Factory factory = scriptEngine.compile("empty_test", "1", EmptyTestScript.CONTEXT, Collections.emptyMap());
        EmptyTestScript script = factory.newInstance();
        assertEquals(1, script.execute());
        assertEquals(1, script.execute());
        script = factory.newInstance();
        assertEquals(1, script.execute());
        assertEquals(1, script.execute());
    }

    public void testTemplate() {
        TemplateScript.Factory factory =
            scriptEngine.compile("template_test", "params['test']", TemplateScript.CONTEXT, Collections.emptyMap());
        TemplateScript script = factory.newInstance(Collections.singletonMap("test", "abc"));
        assertEquals("abc", script.execute());
        assertEquals("abc", script.execute());
        script = factory.newInstance(Collections.singletonMap("test", "def"));
        assertEquals("def", script.execute());
        assertEquals("def", script.execute());
    }
}
