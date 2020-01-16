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
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.TemplateScript;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FactoryTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        contexts.put(StatefulFactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(FactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(DeterministicFactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(EmptyTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(TemplateScript.CONTEXT, Whitelist.BASE_WHITELISTS);

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

        public int getC() {
            return -1;
        }

        public int getD() {
            return 2;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public abstract boolean needsTest();
        public abstract boolean needsNothing();
        public abstract boolean needsX();
        public abstract boolean needsC();
        public abstract boolean needsD();

        public interface StatefulFactory {
            StatefulFactoryTestScript newInstance(int a, int b);

            boolean needsTest();
            boolean needsNothing();
            boolean needsX();
            boolean needsC();
            boolean needsD();
        }

        public interface Factory {
            StatefulFactory newFactory(int x, int y);

            boolean needsTest();
            boolean needsNothing();
            boolean needsX();
            boolean needsC();
            boolean needsD();
        }

        public static final ScriptContext<StatefulFactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", StatefulFactoryTestScript.Factory.class);
    }

    public void testStatefulFactory() {
        StatefulFactoryTestScript.Factory factory = scriptEngine.compile(
            "stateful_factory_test", "test + x + y + d", StatefulFactoryTestScript.CONTEXT, Collections.emptyMap());
        StatefulFactoryTestScript.StatefulFactory statefulFactory = factory.newFactory(1, 2);
        StatefulFactoryTestScript script = statefulFactory.newInstance(3, 4);
        assertEquals(24, script.execute(3));
        statefulFactory.newInstance(5, 6);
        assertEquals(28, script.execute(7));
        assertEquals(true, script.needsTest());
        assertEquals(false, script.needsNothing());
        assertEquals(true, script.needsX());
        assertEquals(false, script.needsC());
        assertEquals(true, script.needsD());
        assertEquals(true, statefulFactory.needsTest());
        assertEquals(false, statefulFactory.needsNothing());
        assertEquals(true, statefulFactory.needsX());
        assertEquals(false, statefulFactory.needsC());
        assertEquals(true, statefulFactory.needsD());
        assertEquals(true, factory.needsTest());
        assertEquals(false, factory.needsNothing());
        assertEquals(true, factory.needsX());
        assertEquals(false, factory.needsC());
        assertEquals(true, factory.needsD());
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

            boolean needsTest();
            boolean needsNothing();
        }

        public static final ScriptContext<FactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTestScript.Factory.class);
    }

    public abstract static class DeterministicFactoryTestScript {
        private final Map<String, Object> params;

        public DeterministicFactoryTestScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public interface Factory extends ScriptFactory{
            FactoryTestScript newInstance(Map<String, Object> params);

            boolean needsTest();
            boolean needsNothing();
        }

        public static final ScriptContext<DeterministicFactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", DeterministicFactoryTestScript.Factory.class);
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
        assertEquals(true, factory.needsTest());
        assertEquals(false, factory.needsNothing());
    }

    public void testDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            scriptEngine.compile("deterministic_test", "Integer.parseInt('123')",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertTrue(factory.isResultDeterministic());
        assertEquals(123, factory.newInstance(Collections.emptyMap()).execute(0));
    }

    public void testNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            scriptEngine.compile("not_deterministic_test", "Math.random()",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertFalse(factory.isResultDeterministic());
        Double d = (Double)factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 0.0 && d <= 1.0);
    }

    public void testMixedDeterministicIsNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            scriptEngine.compile("not_deterministic_test", "Integer.parseInt('123') + Math.random()",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertFalse(factory.isResultDeterministic());
        Double d = (Double)factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 123.0 && d <= 124.0);
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

    public void testGetterInLambda() {
        FactoryTestScript.Factory factory =
            scriptEngine.compile("template_test",
                "IntSupplier createLambda(IntSupplier s) { return s; } createLambda(() -> params['x'] + test).getAsInt()",
                FactoryTestScript.CONTEXT, Collections.emptyMap());
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("x", 1));
        assertEquals(2, script.execute(1));
    }
}
