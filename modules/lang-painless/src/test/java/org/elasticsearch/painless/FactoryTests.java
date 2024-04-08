/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.TemplateScript;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FactoryTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        contexts.put(StatefulFactoryTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(FactoryTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(DeterministicFactoryTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(EmptyTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(TemplateScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(VoidReturnTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(FactoryTestConverterScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        contexts.put(FactoryTestConverterScriptBadDef.CONTEXT, PAINLESS_BASE_WHITELIST);

        return contexts;
    }

    public abstract static class StatefulFactoryTestScript {
        private final int x;
        private final int y;

        public StatefulFactoryTestScript(int x, int y, int a, int b) {
            this.x = x * a;
            this.y = y * b;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y * 2;
        }

        public int getC() {
            return -1;
        }

        public int getD() {
            return 2;
        }

        public static final String[] PARAMETERS = new String[] { "test" };

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

        public static final ScriptContext<StatefulFactoryTestScript.Factory> CONTEXT = new ScriptContext<>(
            "test",
            StatefulFactoryTestScript.Factory.class
        );
    }

    public void testStatefulFactory() {
        StatefulFactoryTestScript.Factory factory = scriptEngine.compile(
            "stateful_factory_test",
            "test + x + y + d",
            StatefulFactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
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

        public static final String[] PARAMETERS = new String[] { "test" };

        public abstract Object execute(int test);

        public interface Factory {
            FactoryTestScript newInstance(Map<String, Object> params);

            boolean needsTest();

            boolean needsNothing();
        }

        public static final ScriptContext<FactoryTestScript.Factory> CONTEXT = new ScriptContext<>("test", FactoryTestScript.Factory.class);
    }

    public abstract static class DeterministicFactoryTestScript {
        private final Map<String, Object> params;

        public DeterministicFactoryTestScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] { "test" };

        public abstract Object execute(int test);

        public interface Factory extends ScriptFactory {
            FactoryTestScript newInstance(Map<String, Object> params);

            boolean needsTest();

            boolean needsNothing();
        }

        public static final ScriptContext<DeterministicFactoryTestScript.Factory> CONTEXT = new ScriptContext<>(
            "test",
            DeterministicFactoryTestScript.Factory.class
        );
    }

    public void testFactory() {
        FactoryTestScript.Factory factory = scriptEngine.compile(
            "factory_test",
            "test + params.get('test')",
            FactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
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
        DeterministicFactoryTestScript.Factory factory = scriptEngine.compile(
            "deterministic_test",
            "Integer.parseInt('123')",
            DeterministicFactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
        assertTrue(factory.isResultDeterministic());
        assertEquals(123, factory.newInstance(Collections.emptyMap()).execute(0));
    }

    public void testNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory = scriptEngine.compile(
            "not_deterministic_test",
            "Math.random()",
            DeterministicFactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
        assertFalse(factory.isResultDeterministic());
        Double d = (Double) factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 0.0 && d <= 1.0);
    }

    public void testMixedDeterministicIsNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory = scriptEngine.compile(
            "not_deterministic_test",
            "Integer.parseInt('123') + Math.random()",
            DeterministicFactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
        assertFalse(factory.isResultDeterministic());
        Double d = (Double) factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 123.0 && d <= 124.0);
    }

    public abstract static class EmptyTestScript {
        public static final String[] PARAMETERS = {};

        public abstract Object execute();

        public interface Factory {
            EmptyTestScript newInstance();
        }

        public static final ScriptContext<EmptyTestScript.Factory> CONTEXT = new ScriptContext<>("test", EmptyTestScript.Factory.class);
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
        TemplateScript.Factory factory = scriptEngine.compile(
            "template_test",
            "params['test']",
            TemplateScript.CONTEXT,
            Collections.emptyMap()
        );
        TemplateScript script = factory.newInstance(Collections.singletonMap("test", "abc"));
        assertEquals("abc", script.execute());
        assertEquals("abc", script.execute());
        script = factory.newInstance(Collections.singletonMap("test", "def"));
        assertEquals("def", script.execute());
        assertEquals("def", script.execute());
    }

    public void testGetterInLambda() {
        FactoryTestScript.Factory factory = scriptEngine.compile(
            "template_test",
            "IntSupplier createLambda(IntSupplier s) { return s; } createLambda(() -> params['x'] + test).getAsInt()",
            FactoryTestScript.CONTEXT,
            Collections.emptyMap()
        );
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("x", 1));
        assertEquals(2, script.execute(1));
    }

    public abstract static class VoidReturnTestScript {
        public static final String[] PARAMETERS = { "map" };

        public abstract void execute(Map<Object, Object> map);

        public interface Factory {
            VoidReturnTestScript newInstance();
        }

        public static final ScriptContext<VoidReturnTestScript.Factory> CONTEXT = new ScriptContext<>(
            "test",
            VoidReturnTestScript.Factory.class
        );
    }

    public void testVoidReturn() {
        scriptEngine.compile("void_return_test", "int x = 1 + 1; return;", VoidReturnTestScript.CONTEXT, Collections.emptyMap());
        IllegalArgumentException iae = expectScriptThrows(
            IllegalArgumentException.class,
            () -> scriptEngine.compile("void_return_test", "1 + 1", VoidReturnTestScript.CONTEXT, Collections.emptyMap())
        );
        assertEquals(iae.getMessage(), "not a statement: result not used from addition operation [+]");
        ClassCastException cce = expectScriptThrows(
            ClassCastException.class,
            () -> scriptEngine.compile("void_return_test", "def x = 1; return x;", VoidReturnTestScript.CONTEXT, Collections.emptyMap())
        );
        assertEquals(cce.getMessage(), "Cannot cast from [def] to [void].");
    }

    public abstract static class FactoryTestConverterScript {
        private final Map<String, Object> params;

        public FactoryTestConverterScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] { "test" };

        public abstract long[] execute(int test);

        public interface Factory {
            FactoryTestConverterScript newInstance(Map<String, Object> params);
        }

        public static final ScriptContext<FactoryTestConverterScript.Factory> CONTEXT = new ScriptContext<>(
            "test",
            FactoryTestConverterScript.Factory.class
        );

        public static long[] convertFromInt(int i) {
            return new long[] { i };
        }

        public static long[] convertFromString(String s) {
            return new long[] { Long.parseLong(s) };
        }

        public static long[] convertFromList(List<?> l) {
            long[] converted = new long[l.size()];
            for (int i = 0; i < l.size(); i++) {
                Object o = l.get(i);
                if (o instanceof Long) {
                    converted[i] = (Long) o;
                } else if (o instanceof Integer) {
                    converted[i] = (Integer) o;
                } else if (o instanceof String) {
                    converted[i] = Long.parseLong((String) o);
                }
            }
            return converted;
        }

        public static long[] convertFromDef(Object def) {
            if (def instanceof String) {
                return convertFromString((String) def);
            } else if (def instanceof Integer) {
                return convertFromInt(((Integer) def).intValue());
            } else if (def instanceof List) {
                return convertFromList((List) def);
            } else {
                return (long[]) def;
            }
            // throw new ClassCastException("Cannot convert [" + def + "] to long[]");
        }
    }

    public void testConverterFactory() {
        FactoryTestConverterScript.Factory factory = scriptEngine.compile(
            "converter_test",
            "return test;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        FactoryTestConverterScript script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 2 }, script.execute(2));
        script = factory.newInstance(Collections.singletonMap("test", 3));
        assertArrayEquals(new long[] { 3 }, script.execute(3));

        factory = scriptEngine.compile("converter_test", "return test + 1;", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1001 }, script.execute(1000));

        factory = scriptEngine.compile("converter_test", "return '100';", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 100 }, script.execute(1000));

        factory = scriptEngine.compile(
            "converter_test",
            "long[] a = new long[]{test, 123}; return a;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1000, 123 }, script.execute(1000));

        factory = scriptEngine.compile("converter_test", "return [test, 123];", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1000, 123 }, script.execute(1000));

        factory = scriptEngine.compile(
            "converter_test",
            "ArrayList a = new ArrayList(); a.add(test); a.add(456); a.add('789'); return a;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 123, 456, 789 }, script.execute(123));

        // autoreturn, no converter
        factory = scriptEngine.compile("converter_test", "new long[]{test}", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 123 }, script.execute(123));

        // autoreturn, converter
        factory = scriptEngine.compile("converter_test", "test", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 456 }, script.execute(456));

        factory = scriptEngine.compile("converter_test", "'1001'", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1001 }, script.execute(456));

        // def tests
        factory = scriptEngine.compile(
            "converter_test",
            "def a = new long[]{test, 123}; return a;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1000, 123 }, script.execute(1000));

        factory = scriptEngine.compile(
            "converter_test",
            "def l = [test, 123]; l;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1000, 123 }, script.execute(1000));

        factory = scriptEngine.compile(
            "converter_test",
            "def a = new ArrayList(); a.add(test); a.add(456); a.add('789'); return a;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 123, 456, 789 }, script.execute(123));

        // autoreturn, no converter
        factory = scriptEngine.compile(
            "converter_test",
            "def a = new long[]{test}; a;",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 123 }, script.execute(123));

        // autoreturn, converter
        factory = scriptEngine.compile("converter_test", "def a = '1001'; a", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1001 }, script.execute(456));

        factory = scriptEngine.compile("converter_test", "int x = 1", FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(null, script.execute(123));

        factory = scriptEngine.compile(
            "converter_test",
            "short x = 1; return x",
            FactoryTestConverterScript.CONTEXT,
            Collections.emptyMap()
        );
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[] { 1 }, script.execute(123));

        ClassCastException cce = expectScriptThrows(
            ClassCastException.class,
            () -> scriptEngine.compile("converter_test", "return true;", FactoryTestConverterScript.CONTEXT, Collections.emptyMap())
        );
        assertEquals(cce.getMessage(), "Cannot cast from [boolean] to [long[]].");
    }

    public abstract static class FactoryTestConverterScriptBadDef {
        private final Map<String, Object> params;

        public FactoryTestConverterScriptBadDef(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] { "test" };

        public abstract long[] execute(int test);

        public interface Factory {
            FactoryTestConverterScriptBadDef newInstance(Map<String, Object> params);
        }

        public static final ScriptContext<FactoryTestConverterScriptBadDef.Factory> CONTEXT = new ScriptContext<>(
            "test",
            FactoryTestConverterScriptBadDef.Factory.class
        );

        public static long[] convertFromDef(int def) {
            return new long[] { def };
        }
    }

    public void testConverterFactoryBadDef() {
        IllegalStateException ise = null;
        try {
            scriptEngine.compile("converter_def", "return test;", FactoryTestConverterScriptBadDef.CONTEXT, Collections.emptyMap());
        } catch (ScriptException e) {
            ise = (IllegalStateException) e.getCause();
        }
        assertNotNull(ise);
        assertEquals("convertFromDef must take a single Object as an argument, not [int]", ise.getMessage());
    }
}
