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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for Painless implementing different interfaces.
 */
public class BaseClassTests extends ScriptTestCase {

    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(Gets.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(NoArgs.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(OneArg.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ArrayArg.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(PrimitiveArrayArg.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(DefArrayArg.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ManyArgs.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(VarArgs.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(DefaultMethods.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ReturnsVoid.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ReturnsPrimitiveBoolean.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ReturnsPrimitiveInt.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ReturnsPrimitiveFloat.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ReturnsPrimitiveDouble.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(NoArgsConstant.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(WrongArgsConstant.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(WrongLengthOfArgConstant.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(UnknownArgType.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(UnknownReturnType.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(UnknownArgTypeInArray.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(TwoExecuteMethods.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public abstract static class Gets {

        public interface Factory {
            Gets newInstance(String testString, int testInt, Map<String, Object> params);
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("gets", Factory.class);

        private final String testString;
        private final int testInt;
        private final Map<String, Object> testMap;

        public Gets(String testString, int testInt, Map<String, Object> testMap) {
            this.testString = testString;
            this.testInt = testInt;
            this.testMap = testMap;
        }

        public static final String[] PARAMETERS = new String[] {};
        public abstract Object execute();

        public String getTestString() {
            return testString;
        }
        public int getTestInt() {
            return Math.abs(testInt);
        }
        public Map<String, Object> getTestMap() {
            return testMap == null ? new HashMap<>() : testMap;
        }
    }
    public void testGets() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("s", 1);

        assertEquals(1, scriptEngine.compile("testGets0", "testInt", Gets.CONTEXT, emptyMap()).newInstance("s", -1, null).execute());
        assertEquals(Collections.emptyMap(),
                scriptEngine.compile("testGets1", "testMap", Gets.CONTEXT, emptyMap()).newInstance("s", -1, null).execute());
        assertEquals(Collections.singletonMap("1", "1"),
                scriptEngine.compile("testGets2", "testMap", Gets.CONTEXT, emptyMap())
                        .newInstance("s", -1, Collections.singletonMap("1", "1")).execute());
        assertEquals("s", scriptEngine.compile("testGets3", "testString", Gets.CONTEXT, emptyMap()).newInstance("s", -1, null).execute());
        assertEquals(map,
                scriptEngine.compile("testGets4", "testMap.put(testString, testInt); testMap", Gets.CONTEXT, emptyMap())
                        .newInstance("s", -1, null).execute());
    }

    public abstract static class NoArgs {
        public interface Factory {
            NoArgs newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("noargs", Factory.class);

        public static final String[] PARAMETERS = new String[] {};
        public abstract Object execute();
    }
    public void testNoArgs() throws Exception {
        assertEquals(1, scriptEngine.compile("testNoArgs0", "1", NoArgs.CONTEXT, emptyMap()).newInstance().execute());
        assertEquals("foo", scriptEngine.compile("testNoArgs1", "'foo'", NoArgs.CONTEXT, emptyMap()).newInstance().execute());

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile("testNoArgs2", "doc", NoArgs.CONTEXT, emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile("testNoArgs3", "_score", NoArgs.CONTEXT, emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());

        String debug = Debugger.toString(NoArgs.class, "int i = 0", new CompilerSettings());
        assertThat(debug, containsString("ACONST_NULL"));
        assertThat(debug, containsString("ARETURN"));
    }

    public abstract static class OneArg {
        public interface Factory {
            OneArg newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("onearg", Factory.class);

        public static final String[] PARAMETERS = new String[] {"arg"};
        public abstract Object execute(Object arg);
    }
    public void testOneArg() throws Exception {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile("testOneArg0", "arg", OneArg.CONTEXT, emptyMap()).newInstance().execute(rando));
        rando = randomAlphaOfLength(5);
        assertEquals(rando, scriptEngine.compile("testOneArg1", "arg", OneArg.CONTEXT, emptyMap()).newInstance().execute(rando));
    }

    public abstract static class ArrayArg {
        public interface Factory {
            ArrayArg newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("arrayarg", Factory.class);

        public static final String[] PARAMETERS = new String[] {"arg"};
        public abstract Object execute(String[] arg);
    }
    public void testArrayArg() throws Exception {
        String rando = randomAlphaOfLength(5);
        assertEquals(rando,
                scriptEngine.compile("testArrayArg0", "arg[0]", ArrayArg.CONTEXT, emptyMap())
                        .newInstance().execute(new String[] {rando, "foo"}));
    }

    public abstract static class PrimitiveArrayArg {
        public interface Factory {
            PrimitiveArrayArg newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("primitivearrayarg", Factory.class);

        public static final String[] PARAMETERS = new String[] {"arg"};
        public abstract Object execute(int[] arg);
    }
    public void testPrimitiveArrayArg() throws Exception {
        int rando = randomInt();
        assertEquals(rando,
                scriptEngine.compile("PrimitiveArrayArg0", "arg[0]", PrimitiveArrayArg.CONTEXT, emptyMap())
                        .newInstance().execute(new int[] {rando, 10}));
    }

    public abstract static class DefArrayArg {
        public interface Factory {
            DefArrayArg newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("defarrayarg", Factory.class);

        public static final String[] PARAMETERS = new String[] {"arg"};
        public abstract Object execute(Object[] arg);
    }
    public void testDefArrayArg()throws Exception {
        Object rando = randomInt();
        assertEquals(rando,
                scriptEngine.compile("testDefArray0", "arg[0]", DefArrayArg.CONTEXT, emptyMap())
                        .newInstance().execute(new Object[] {rando, 10}));
        rando = randomAlphaOfLength(5);
        assertEquals(rando,
                scriptEngine.compile("testDefArray1", "arg[0]", DefArrayArg.CONTEXT, emptyMap())
                        .newInstance().execute(new Object[] {rando, 10}));
        assertEquals(5, scriptEngine.compile(
                "testDefArray2", "arg[0].length()", DefArrayArg.CONTEXT, emptyMap())
                .newInstance().execute(new Object[] {rando, 10}));
    }

    public abstract static class ManyArgs {
        public interface Factory {
            ManyArgs newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("manyargs", Factory.class);

        public static final String[] PARAMETERS = new String[] {"a", "b", "c", "d"};
        public abstract Object execute(int a, int b, int c, int d);
        public abstract boolean needsA();
        public abstract boolean needsB();
        public abstract boolean needsC();
        public abstract boolean needsD();
    }
    public void testManyArgs() throws Exception {
        int rando = randomInt();
        assertEquals(rando,
                scriptEngine.compile("testManyArgs0", "a", ManyArgs.CONTEXT, emptyMap()).newInstance().execute(rando, 0, 0, 0));
        assertEquals(10,
                scriptEngine.compile("testManyArgs1", "a + b + c + d", ManyArgs.CONTEXT, emptyMap()).newInstance().execute(1, 2, 3, 4));

        // While we're here we can verify that painless correctly finds used variables
        ManyArgs script = scriptEngine.compile("testManyArgs2", "a", ManyArgs.CONTEXT, emptyMap()).newInstance();
        assertTrue(script.needsA());
        assertFalse(script.needsB());
        assertFalse(script.needsC());
        assertFalse(script.needsD());
        script = scriptEngine.compile("testManyArgs3", "a + b + c", ManyArgs.CONTEXT, emptyMap()).newInstance();
        assertTrue(script.needsA());
        assertTrue(script.needsB());
        assertTrue(script.needsC());
        assertFalse(script.needsD());
        script = scriptEngine.compile("testManyArgs4", "a + b + c + d", ManyArgs.CONTEXT, emptyMap()).newInstance();
        assertTrue(script.needsA());
        assertTrue(script.needsB());
        assertTrue(script.needsC());
        assertTrue(script.needsD());
    }

    public abstract static class VarArgs {
        public interface Factory {
            VarArgs newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("varargs", Factory.class);

        public static final String[] PARAMETERS = new String[] {"arg"};
        public abstract Object execute(String... arg);
    }
    public void testVarArgs() throws Exception {
        assertEquals("foo bar baz",
                scriptEngine.compile("testVarArgs0", "String.join(' ', Arrays.asList(arg))", VarArgs.CONTEXT, emptyMap())
                        .newInstance().execute("foo", "bar", "baz"));
    }

    public abstract static class DefaultMethods {
        public interface Factory {
            DefaultMethods newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("defaultmethods", Factory.class);

        public static final String[] PARAMETERS = new String[] {"a", "b", "c", "d"};
        public abstract Object execute(int a, int b, int c, int d);
        public Object executeWithOne() {
            return execute(1, 1, 1, 1);
        }
        public Object executeWithASingleOne(int a, int b, int c) {
            return execute(a, b, c, 1);
        }
    }
    public void testDefaultMethods() throws Exception {
        int rando = randomInt();
        assertEquals(rando,
                scriptEngine.compile("testDefaultMethods0", "a", DefaultMethods.CONTEXT, emptyMap()).newInstance().execute(rando, 0, 0, 0));
        assertEquals(rando,
                scriptEngine.compile("testDefaultMethods1", "a", DefaultMethods.CONTEXT, emptyMap())
                        .newInstance().executeWithASingleOne(rando, 0, 0));
        assertEquals(10,
                scriptEngine.compile("testDefaultMethods2", "a + b + c + d", DefaultMethods.CONTEXT, emptyMap())
                        .newInstance().execute(1, 2, 3, 4));
        assertEquals(4,
                scriptEngine.compile("testDefaultMethods3", "a + b + c + d", DefaultMethods.CONTEXT, emptyMap())
                        .newInstance().executeWithOne());
        assertEquals(7,
                scriptEngine.compile("testDefaultMethods4", "a + b + c + d", DefaultMethods.CONTEXT, emptyMap())
                        .newInstance().executeWithASingleOne(1, 2, 3));
    }

    public abstract static class ReturnsVoid {
        public interface Factory {
            ReturnsVoid newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("returnsvoid", Factory.class);

        public static final String[] PARAMETERS = new String[] {"map"};
        public abstract void execute(Map<String, Object> map);
    }
    public void testReturnsVoid() throws Exception {
        Map<String, Object> map = new HashMap<>();
        scriptEngine.compile("testReturnsVoid0", "map.a = 'foo'", ReturnsVoid.CONTEXT, emptyMap()).newInstance().execute(map);
        assertEquals(Collections.singletonMap("a", "foo"), map);
        scriptEngine.compile("testReturnsVoid1", "map.remove('a')", ReturnsVoid.CONTEXT, emptyMap()).newInstance().execute(map);
        assertEquals(emptyMap(), map);

        String debug = Debugger.toString(ReturnsVoid.class, "int i = 0", new CompilerSettings());
        // The important thing is that this contains the opcode for returning void
        assertThat(debug, containsString(" RETURN"));
        // We shouldn't contain any weird "default to null" logic
        assertThat(debug, not(containsString("ACONST_NULL")));
    }

    public abstract static class ReturnsPrimitiveBoolean {
        public interface Factory {
            ReturnsPrimitiveBoolean newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("returnsprimitiveboolean", Factory.class);

        public static final String[] PARAMETERS = new String[] {};
        public abstract boolean execute();
    }
    public void testReturnsPrimitiveBoolean() throws Exception {
        assertTrue(
                scriptEngine.compile("testReturnsPrimitiveBoolean0", "true", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertFalse(
                scriptEngine.compile("testReturnsPrimitiveBoolean1", "false", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertTrue(
                scriptEngine.compile("testReturnsPrimitiveBoolean2", "Boolean.TRUE", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertFalse(
                scriptEngine.compile("testReturnsPrimitiveBoolean3", "Boolean.FALSE", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());

        assertTrue(
                scriptEngine.compile("testReturnsPrimitiveBoolean4", "def i = true; i", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertTrue(
                scriptEngine.compile("testReturnsPrimitiveBoolean5", "def i = Boolean.TRUE; i", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertTrue(
                scriptEngine.compile("testReturnsPrimitiveBoolean6", "true || false", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());

        String debug = Debugger.toString(ReturnsPrimitiveBoolean.class, "false", new CompilerSettings());
        assertThat(debug, containsString("ICONST_0"));
        // The important thing here is that we have the bytecode for returning an integer instead of an object. booleans are integers.
        assertThat(debug, containsString("IRETURN"));

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean7", "1L",ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals("Cannot cast from [long] to [boolean].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean8", "1.1f", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals("Cannot cast from [float] to [boolean].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean9", "1.1d", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals("Cannot cast from [double] to [boolean].", e.getMessage());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean10", "def i = 1L; i", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean11", "def i = 1.1f; i", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveBoolean12", "def i = 1.1d; i", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());

        assertFalse(
                scriptEngine.compile("testReturnsPrimitiveBoolean13", "int i = 0", ReturnsPrimitiveBoolean.CONTEXT, emptyMap())
                        .newInstance().execute());
    }

    public abstract static class ReturnsPrimitiveInt {
        public interface Factory {
            ReturnsPrimitiveInt newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("returnsprimitiveint", Factory.class);

        public static final String[] PARAMETERS = new String[] {};
        public abstract int execute();
    }
    public void testReturnsPrimitiveInt() throws Exception {
        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt0", "1", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt1", "(int) 1L", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals(1, scriptEngine.compile("testReturnsPrimitiveInt2", "(int) 1.1d", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                .newInstance().execute());
        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt3", "(int) 1.1f", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt4", "Integer.valueOf(1)", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());

        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt5", "def i = 1; i", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals(1,
                scriptEngine.compile("testReturnsPrimitiveInt6", "def i = Integer.valueOf(1); i", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());

        assertEquals(2,
                scriptEngine.compile("testReturnsPrimitiveInt7", "1 + 1", ReturnsPrimitiveInt.CONTEXT, emptyMap()).newInstance().execute());

        String debug = Debugger.toString(ReturnsPrimitiveInt.class, "1", new CompilerSettings());
        assertThat(debug, containsString("ICONST_1"));
        // The important thing here is that we have the bytecode for returning an integer instead of an object
        assertThat(debug, containsString("IRETURN"));

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt8", "1L", ReturnsPrimitiveInt.CONTEXT, emptyMap()).newInstance().execute());
        assertEquals("Cannot cast from [long] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt9", "1.1f", ReturnsPrimitiveInt.CONTEXT, emptyMap()).newInstance().execute());
        assertEquals("Cannot cast from [float] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt10", "1.1d", ReturnsPrimitiveInt.CONTEXT, emptyMap()).newInstance().execute());
        assertEquals("Cannot cast from [double] to [int].", e.getMessage());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt11", "def i = 1L; i", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt12", "def i = 1.1f; i", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());
        expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveInt13", "def i = 1.1d; i", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                        .newInstance().execute());

        assertEquals(0, scriptEngine.compile("testReturnsPrimitiveInt14", "int i = 0", ReturnsPrimitiveInt.CONTEXT, emptyMap())
                .newInstance().execute());
    }

    public abstract static class ReturnsPrimitiveFloat {
        public interface Factory {
            ReturnsPrimitiveFloat newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("returnsprimitivefloat", Factory.class);

        public static final String[] PARAMETERS = new String[] {};
        public abstract float execute();
    }
    public void testReturnsPrimitiveFloat() throws Exception {
        assertEquals(1.1f,
                scriptEngine.compile("testReturnsPrimitiveFloat0", "1.1f", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1f,
                scriptEngine.compile("testReturnsPrimitiveFloat1", "(float) 1.1d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1f,
                scriptEngine.compile("testReturnsPrimitiveFloat2", "def d = 1.1f; d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1f, scriptEngine.compile(
                "testReturnsPrimitiveFloat3", "def d = Float.valueOf(1.1f); d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                .newInstance().execute(), 0);

        assertEquals(1.1f + 6.7f,
                scriptEngine.compile("testReturnsPrimitiveFloat4", "1.1f + 6.7f", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveFloat5", "1.1d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute());
        assertEquals("Cannot cast from [double] to [float].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                scriptEngine.compile("testReturnsPrimitiveFloat6", "def d = 1.1d; d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute());
        e = expectScriptThrows(ClassCastException.class, () -> scriptEngine.compile(
                "testReturnsPrimitiveFloat7", "def d = Double.valueOf(1.1); d", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                .newInstance().execute());

        String debug = Debugger.toString(ReturnsPrimitiveFloat.class, "1f", new CompilerSettings());
        assertThat(debug, containsString("FCONST_1"));
        // The important thing here is that we have the bytecode for returning a float instead of an object
        assertThat(debug, containsString("FRETURN"));

        assertEquals(0.0f,
                scriptEngine.compile("testReturnsPrimitiveFloat8", "int i = 0", ReturnsPrimitiveFloat.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
    }

   public abstract static class ReturnsPrimitiveDouble {
       public interface Factory {
           ReturnsPrimitiveDouble newInstance();
       }

       public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("returnsprimitivedouble", Factory.class);

        public static final String[] PARAMETERS = new String[] {};
        public abstract double execute();
    }
    public void testReturnsPrimitiveDouble() throws Exception {
        assertEquals(1.0,
                scriptEngine.compile("testReturnsPrimitiveDouble0", "1", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.0,
                scriptEngine.compile("testReturnsPrimitiveDouble1", "1L", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1,
                scriptEngine.compile("testReturnsPrimitiveDouble2", "1.1d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals((double) 1.1f,
                scriptEngine.compile("testReturnsPrimitiveDouble3", "1.1f", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1, scriptEngine.compile(
                "testReturnsPrimitiveDouble4", "Double.valueOf(1.1)", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                .newInstance().execute(), 0);
        assertEquals((double) 1.1f, scriptEngine.compile(
                "testReturnsPrimitiveDouble5", "Float.valueOf(1.1f)", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                .newInstance().execute(), 0);

        assertEquals(1.0,
                scriptEngine.compile("testReturnsPrimitiveDouble6", "def d = 1; d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.0,
                scriptEngine.compile("testReturnsPrimitiveDouble7", "def d = 1L; d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1,
                scriptEngine.compile("testReturnsPrimitiveDouble8", "def d = 1.1d; d", ReturnsPrimitiveDouble.CONTEXT, emptyMap()).
                        newInstance().execute(), 0);
        assertEquals((double) 1.1f,
                scriptEngine.compile("testReturnsPrimitiveDouble9", "def d = 1.1f; d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
        assertEquals(1.1, scriptEngine.compile(
                "testReturnsPrimitiveDouble10", "def d = Double.valueOf(1.1); d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                .newInstance().execute(), 0);
        assertEquals((double) 1.1f, scriptEngine.compile(
                "testReturnsPrimitiveDouble11", "def d = Float.valueOf(1.1f); d", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                .newInstance().execute(), 0);

        assertEquals(1.1 + 6.7,
                scriptEngine.compile("testReturnsPrimitiveDouble12", "1.1 + 6.7", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);

        String debug = Debugger.toString(ReturnsPrimitiveDouble.class, "1", new CompilerSettings());
        assertThat(debug, containsString("DCONST_1"));
        // The important thing here is that we have the bytecode for returning a double instead of an object
        assertThat(debug, containsString("DRETURN"));

        assertEquals(0.0,
                scriptEngine.compile("testReturnsPrimitiveDouble13", "int i = 0", ReturnsPrimitiveDouble.CONTEXT, emptyMap())
                        .newInstance().execute(), 0);
    }

    public abstract static class NoArgsConstant {
        public interface Factory {
            NoArgsConstant newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("noargsconstant", Factory.class);

        public abstract Object execute(String foo);
    }
    public void testNoArgsConstant() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testNoArgsConstant0", "1", NoArgsConstant.CONTEXT, emptyMap()).newInstance().execute("constant"));
        assertThat(e.getMessage(), startsWith(
                "Painless needs a constant [String[] PARAMETERS] on all interfaces it implements with the "
                + "names of the method arguments but [" + NoArgsConstant.class.getName() + "] doesn't have one."));
    }

    public abstract static class WrongArgsConstant {
        public interface Factory {
            WrongArgsConstant newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("wrongargscontext", Factory.class);

        boolean[] PARAMETERS = new boolean[] {false};
        public abstract Object execute(String foo);
    }
    public void testWrongArgsConstant() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testWrongArgsConstant0", "1", WrongArgsConstant.CONTEXT, emptyMap()));
        assertThat(e.getMessage(), startsWith(
                "Painless needs a constant [String[] PARAMETERS] on all interfaces it implements with the "
                + "names of the method arguments but [" + WrongArgsConstant.class.getName() + "] doesn't have one."));
    }

    public abstract static class WrongLengthOfArgConstant {
        public interface Factory {
            WrongLengthOfArgConstant newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("wronglengthofargcontext", Factory.class);

        public static final String[] PARAMETERS = new String[] {"foo", "bar"};
        public abstract Object execute(String foo);
    }
    public void testWrongLengthOfArgConstant() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testWrongLengthOfArgConstant", "1", WrongLengthOfArgConstant.CONTEXT, emptyMap()));
        assertThat(e.getMessage(), startsWith("[" + WrongLengthOfArgConstant.class.getName() + "#ARGUMENTS] has length [2] but ["
                + WrongLengthOfArgConstant.class.getName() + "#execute] takes [1] argument."));
    }

    public abstract static class UnknownArgType {
        public interface Factory {
            UnknownArgType newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("unknownargtype", Factory.class);

        public static final String[] PARAMETERS = new String[] {"foo"};
        public abstract Object execute(UnknownArgType foo);
    }
    public void testUnknownArgType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testUnknownArgType0", "1", UnknownArgType.CONTEXT, emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgType.class.getName() + ". Painless interfaces can only accept arguments "
                + "that are of whitelisted types.", e.getMessage());
    }

    public abstract static class UnknownReturnType {
        public interface Factory {
            UnknownReturnType newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("unknownreturntype", Factory.class);

        public static final String[] PARAMETERS = new String[] {"foo"};
        public abstract UnknownReturnType execute(String foo);
    }
    public void testUnknownReturnType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testUnknownReturnType0", "1", UnknownReturnType.CONTEXT, emptyMap()));
        assertEquals("Painless can only implement execute methods returning a whitelisted type but [" + UnknownReturnType.class.getName()
                + "#execute] returns [" + UnknownReturnType.class.getName() + "] which isn't whitelisted.", e.getMessage());
    }

    public abstract static class UnknownArgTypeInArray {
        public interface Factory {
            UnknownArgTypeInArray newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("unknownargtypeinarray", Factory.class);

        public static final String[] PARAMETERS = new String[] {"foo"};
        public abstract Object execute(UnknownArgTypeInArray[] foo);
    }
    public void testUnknownArgTypeInArray() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testUnknownAryTypeInArray0", "1", UnknownArgTypeInArray.CONTEXT, emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgTypeInArray.class.getName() + ". Painless interfaces can only accept "
                + "arguments that are of whitelisted types.", e.getMessage());
    }

    public abstract static class TwoExecuteMethods {
        public interface Factory {
            TwoExecuteMethods newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("twoexecutemethods", Factory.class);

        public abstract Object execute();
        public abstract Object execute(boolean foo);
    }
    public void testTwoExecuteMethods() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile("testTwoExecuteMethods0", "null", TwoExecuteMethods.CONTEXT, emptyMap()));
        assertEquals("Painless can only implement interfaces that have a single method named [execute] but ["
                + TwoExecuteMethods.class.getName() + "] has more than one.", e.getMessage());
    }
}
