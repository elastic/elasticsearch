/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class BindingsTests extends ScriptTestCase {

    public static int classMul(int i, int j) {
        return i * j;
    }

    public static int compileTimeBlowUp(int i, int j) {
        throw new RuntimeException("Boom");
    }

    public static List<Object> fancyConstant(String thing1, String thing2) {
        return List.of(thing1, thing2);
    }

    public static class BindingTestClass {
        public int state;

        public BindingTestClass(int state0, int state1) {
            this.state = state0 + state1;
        }

        public int addWithState(int istateless, double dstateless) {
            return istateless + state + (int)dstateless;
        }
    }

    public static class ThisBindingTestClass {
        private BindingsTestScript bindingsTestScript;
        private int state;

        public ThisBindingTestClass(BindingsTestScript bindingsTestScript, int state0, int state1) {
            this.bindingsTestScript = bindingsTestScript;
            this.state = state0 + state1;
        }

        public int addThisWithState(int istateless, double dstateless) {
            return istateless + state + (int)dstateless + bindingsTestScript.getTestValue();
        }
    }

    public static class EmptyThisBindingTestClass {
        private BindingsTestScript bindingsTestScript;

        public EmptyThisBindingTestClass(BindingsTestScript bindingsTestScript) {
            this.bindingsTestScript = bindingsTestScript;
        }

        public int addEmptyThisWithState(int istateless) {
            return istateless + bindingsTestScript.getTestValue();
        }
    }

    public static class InstanceBindingTestClass {
        private int value;

        public InstanceBindingTestClass(int value) {
            this.value = value;
        }

        public void setInstanceBindingValue(int value) {
            this.value = value;
        }

        public int getInstanceBindingValue() {
            return value;
        }

        public int instanceMul(int i, int j) {
            return i * j;
        }
    }

    public abstract static class BindingsTestScript {
        public static final String[] PARAMETERS = { "test", "bound" };
        public int getTestValue() {return 7;}
        public abstract int execute(int test, int bound);
        public interface Factory {
            BindingsTestScript newInstance();
        }
        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("bindings_test", Factory.class);
    }

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));

        InstanceBindingTestClass instanceBindingTestClass = new InstanceBindingTestClass(1);
        WhitelistInstanceBinding getter = new WhitelistInstanceBinding("test", instanceBindingTestClass,
                "setInstanceBindingValue", "void", Collections.singletonList("int"), Collections.emptyList());
        WhitelistInstanceBinding setter = new WhitelistInstanceBinding("test", instanceBindingTestClass,
                "getInstanceBindingValue", "int", Collections.emptyList(), Collections.emptyList());
        WhitelistInstanceBinding mul = new WhitelistInstanceBinding("test", instanceBindingTestClass,
                "instanceMul", "int", List.of("int", "int"), List.of(CompileTimeOnlyAnnotation.INSTANCE));
        List<WhitelistInstanceBinding> instanceBindingsList = new ArrayList<>();
        instanceBindingsList.add(getter);
        instanceBindingsList.add(setter);
        instanceBindingsList.add(mul);
        Whitelist instanceBindingsWhitelist = new Whitelist(instanceBindingTestClass.getClass().getClassLoader(),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), instanceBindingsList);
        whitelists.add(instanceBindingsWhitelist);

        contexts.put(BindingsTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testBasicClassBinding() {
        String script = "addWithState(4, 5, 6, 0.0)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(15, executableScript.execute(0, 0));
    }

    public void testRepeatedClassBinding() {
        String script = "addWithState(4, 5, test, 0.0)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(14, executableScript.execute(5, 0));
        assertEquals(13, executableScript.execute(4, 0));
        assertEquals(16, executableScript.execute(7, 0));
    }

    public void testBoundClassBinding() {
        String script = "addWithState(4, bound, test, 0.0)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(10, executableScript.execute(5, 1));
        assertEquals(9, executableScript.execute(4, 2));
    }

    public void testThisClassBinding() {
        String script = "addThisWithState(4, bound, test, 0.0)";

        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(17, executableScript.execute(5, 1));
        assertEquals(16, executableScript.execute(4, 2));
    }

    public void testEmptyThisClassBinding() {
        String script = "addEmptyThisWithState(test)";

        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(8, executableScript.execute(1, 0));
        assertEquals(9, executableScript.execute(2, 0));
    }

    public void testInstanceBinding() {
        String script = "getInstanceBindingValue() + test + bound";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(3, executableScript.execute(1, 1));

        script = "setInstanceBindingValue(test + bound); getInstanceBindingValue()";
        factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        executableScript = factory.newInstance();
        assertEquals(4, executableScript.execute(-2, 6));

        script = "getInstanceBindingValue() + test + bound";
        factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        executableScript = factory.newInstance();
        assertEquals(8, executableScript.execute(-2, 6));
    }

    public void testClassMethodCompileTimeOnly() {
        String script = "classMul(2, 2)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(4, executableScript.execute(1, 1));

        assertThat(
            Debugger.toString(BindingsTestScript.class, script, new CompilerSettings(), scriptContexts().get(BindingsTestScript.CONTEXT)),
            containsString("ICONST_4")
        );
    }

    public void testClassMethodCompileTimeOnlyVariableParams() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> scriptEngine.compile(null, "def a = 2; classMul(2, a)", BindingsTestScript.CONTEXT, Collections.emptyMap())
        );
        assertThat(e.getMessage(), equalTo("all arguments to [classMul] must be constant but the [2] argument isn't"));
    }

    public void testClassMethodCompileTimeOnlyThrows() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> scriptEngine.compile(null, "compileTimeBlowUp(2, 2)", BindingsTestScript.CONTEXT, Collections.emptyMap())
        );
        assertThat(e.getMessage(), startsWith("error invoking"));
        assertThat(e.getCause().getMessage(), equalTo("Boom"));
    }

    public void testInstanceBindingCompileTimeOnly() {
        String script = "instanceMul(2, 2)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(4, executableScript.execute(1, 1));

        assertThat(
            Debugger.toString(BindingsTestScript.class, script, new CompilerSettings(), scriptContexts().get(BindingsTestScript.CONTEXT)),
            containsString("ICONST_4")
        );
    }

    public void testInstanceMethodCompileTimeOnlyVariableParams() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> scriptEngine.compile(null, "def a = 2; instanceMul(a, 2)", BindingsTestScript.CONTEXT, Collections.emptyMap())
        );
        assertThat(e.getMessage(), equalTo("all arguments to [instanceMul] must be constant but the [1] argument isn't"));
    }

    public void testCompileTimeOnlyParameterFoldedToConstant() {
        String script = "classMul(1, 1 + 1)";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(2, executableScript.execute(1, 1));

        assertThat(
            Debugger.toString(BindingsTestScript.class, script, new CompilerSettings(), scriptContexts().get(BindingsTestScript.CONTEXT)),
            containsString("ICONST_2")
        );
    }

    /**
     * Tests that {@code @compile_time_only} can return values that don't
     * fit into the constant pool and we'll create them as static members.
     */
    public void testCompileTimeOnlyResultOutsideConstantPool() {
        String script = "fancyConstant('foo', 'bar').stream().mapToInt(String::length).sum()";
        BindingsTestScript.Factory factory = scriptEngine.compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(6, executableScript.execute(1, 1));

        String code = Debugger.toString(
            BindingsTestScript.class,
            script,
            new CompilerSettings(),
            scriptContexts().get(BindingsTestScript.CONTEXT)
        );
        assertThat(
            "We make a static field to hold the constant",
            code,
            containsString("public static synthetic Ljava/util/List; constant$synthetic$0")
        );
        assertThat(
            "We load the constant from the static field",
            code,
            containsString("GETSTATIC org/elasticsearch/painless/PainlessScript$Script.constant$synthetic$0 : Ljava/util/List;")
        );
        /*
         * It's kind of important that we use java.util.List above and *not*
         * the specific subtype returned by java.util.List.of(). Doing that
         * means that we don't have to whitelist the actual returned type, just
         * the interface. The JVM ought to be able to optimize the invocation
         * because the constant is effectively final.
         */
    }
}
