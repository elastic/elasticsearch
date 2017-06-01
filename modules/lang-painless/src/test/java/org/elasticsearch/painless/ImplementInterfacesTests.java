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
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for Painless implementing different interfaces.
 */
public class ImplementInterfacesTests extends ScriptTestCase {
    public interface NoArgs {
        String[] ARGUMENTS = new String[] {};
        Object execute();
    }
    public void testNoArgs() {
        Compiler compiler = new Compiler(NoArgs.class, Definition.BUILTINS);
        assertEquals(1, ((NoArgs)scriptEngine.compile(compiler, null, "1", emptyMap())).execute());
        assertEquals("foo", ((NoArgs)scriptEngine.compile(compiler, null, "'foo'", emptyMap())).execute());

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile(compiler, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile(compiler, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());

        String debug = Debugger.toString(NoArgs.class, "int i = 0", new CompilerSettings());
        /* Elasticsearch requires that scripts that return nothing return null. We hack that together by returning null from scripts that
         * return Object if they don't return anything. */
        assertThat(debug, containsString("ACONST_NULL"));
        assertThat(debug, containsString("ARETURN"));
    }

    public interface OneArg {
        String[] ARGUMENTS = new String[] {"arg"};
        Object execute(Object arg);
    }
    public void testOneArg() {
        Compiler compiler = new Compiler(OneArg.class, Definition.BUILTINS);
        Object rando = randomInt();
        assertEquals(rando, ((OneArg)scriptEngine.compile(compiler, null, "arg", emptyMap())).execute(rando));
        rando = randomAlphaOfLength(5);
        assertEquals(rando, ((OneArg)scriptEngine.compile(compiler, null, "arg", emptyMap())).execute(rando));

        Compiler noargs = new Compiler(NoArgs.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile(noargs, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
                scriptEngine.compile(noargs, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());
    }

    public interface ArrayArg {
        String[] ARGUMENTS = new String[] {"arg"};
        Object execute(String[] arg);
    }
    public void testArrayArg() {
        Compiler compiler = new Compiler(ArrayArg.class, Definition.BUILTINS);
        String rando = randomAlphaOfLength(5);
        assertEquals(rando, ((ArrayArg)scriptEngine.compile(compiler, null, "arg[0]", emptyMap())).execute(new String[] {rando, "foo"}));
    }

    public interface PrimitiveArrayArg {
        String[] ARGUMENTS = new String[] {"arg"};
        Object execute(int[] arg);
    }
    public void testPrimitiveArrayArg() {
        Compiler compiler = new Compiler(PrimitiveArrayArg.class, Definition.BUILTINS);
        int rando = randomInt();
        assertEquals(rando, ((PrimitiveArrayArg)scriptEngine.compile(compiler, null, "arg[0]", emptyMap())).execute(new int[] {rando, 10}));
    }

    public interface DefArrayArg {
        String[] ARGUMENTS = new String[] {"arg"};
        Object execute(Object[] arg);
    }
    public void testDefArrayArg() {
        Compiler compiler = new Compiler(DefArrayArg.class, Definition.BUILTINS);
        Object rando = randomInt();
        assertEquals(rando, ((DefArrayArg)scriptEngine.compile(compiler, null, "arg[0]", emptyMap())).execute(new Object[] {rando, 10}));
        rando = randomAlphaOfLength(5);
        assertEquals(rando, ((DefArrayArg)scriptEngine.compile(compiler, null, "arg[0]", emptyMap())).execute(new Object[] {rando, 10}));
        assertEquals(5,
            ((DefArrayArg)scriptEngine.compile(compiler, null, "arg[0].length()", emptyMap())).execute(new Object[] {rando, 10}));
    }

    public interface ManyArgs {
        String[] ARGUMENTS = new String[] {"a", "b", "c", "d"};
        Object execute(int a, int b, int c, int d);
        boolean uses$a();
        boolean uses$b();
        boolean uses$c();
        boolean uses$d();
    }
    public void testManyArgs() {
        Compiler compiler = new Compiler(ManyArgs.class, Definition.BUILTINS);
        int rando = randomInt();
        assertEquals(rando, ((ManyArgs)scriptEngine.compile(compiler, null, "a", emptyMap())).execute(rando, 0, 0, 0));
        assertEquals(10, ((ManyArgs)scriptEngine.compile(compiler, null, "a + b + c + d", emptyMap())).execute(1, 2, 3, 4));

        // While we're here we can verify that painless correctly finds used variables
        ManyArgs script = (ManyArgs)scriptEngine.compile(compiler, null, "a", emptyMap());
        assertTrue(script.uses$a());
        assertFalse(script.uses$b());
        assertFalse(script.uses$c());
        assertFalse(script.uses$d());
        script = (ManyArgs)scriptEngine.compile(compiler, null, "a + b + c", emptyMap());
        assertTrue(script.uses$a());
        assertTrue(script.uses$b());
        assertTrue(script.uses$c());
        assertFalse(script.uses$d());
        script = (ManyArgs)scriptEngine.compile(compiler, null, "a + b + c + d", emptyMap());
        assertTrue(script.uses$a());
        assertTrue(script.uses$b());
        assertTrue(script.uses$c());
        assertTrue(script.uses$d());
    }

    public interface VarargTest {
        String[] ARGUMENTS = new String[] {"arg"};
        Object execute(String... arg);
    }
    public void testVararg() {
        Compiler compiler = new Compiler(VarargTest.class, Definition.BUILTINS);
        assertEquals("foo bar baz", ((VarargTest)scriptEngine.compile(compiler, null, "String.join(' ', Arrays.asList(arg))", emptyMap()))
                    .execute("foo", "bar", "baz"));
    }

    public interface DefaultMethods {
        String[] ARGUMENTS = new String[] {"a", "b", "c", "d"};
        Object execute(int a, int b, int c, int d);
        default Object executeWithOne() {
            return execute(1, 1, 1, 1);
        }
        default Object executeWithASingleOne(int a, int b, int c) {
            return execute(a, b, c, 1);
        }
    }
    public void testDefaultMethods() {
        Compiler compiler = new Compiler(DefaultMethods.class, Definition.BUILTINS);
        int rando = randomInt();
        assertEquals(rando, ((DefaultMethods)scriptEngine.compile(compiler, null, "a", emptyMap())).execute(rando, 0, 0, 0));
        assertEquals(rando, ((DefaultMethods)scriptEngine.compile(compiler, null, "a", emptyMap())).executeWithASingleOne(rando, 0, 0));
        assertEquals(10, ((DefaultMethods)scriptEngine.compile(compiler, null, "a + b + c + d", emptyMap())).execute(1, 2, 3, 4));
        assertEquals(4, ((DefaultMethods)scriptEngine.compile(compiler, null, "a + b + c + d", emptyMap())).executeWithOne());
        assertEquals(7, ((DefaultMethods)scriptEngine.compile(compiler, null, "a + b + c + d", emptyMap())).executeWithASingleOne(1, 2, 3));
    }

    public interface ReturnsVoid {
        String[] ARGUMENTS = new String[] {"map"};
        void execute(Map<String, Object> map);
    }
    public void testReturnsVoid() {
        Compiler compiler = new Compiler(ReturnsVoid.class, Definition.BUILTINS);
        Map<String, Object> map = new HashMap<>();
        ((ReturnsVoid)scriptEngine.compile(compiler, null, "map.a = 'foo'", emptyMap())).execute(map);
        assertEquals(singletonMap("a", "foo"), map);
        ((ReturnsVoid)scriptEngine.compile(compiler, null, "map.remove('a')", emptyMap())).execute(map);
        assertEquals(emptyMap(), map);

        String debug = Debugger.toString(ReturnsVoid.class, "int i = 0", new CompilerSettings());
        // The important thing is that this contains the opcode for returning void
        assertThat(debug, containsString(" RETURN"));
        // We shouldn't contain any weird "default to null" logic
        assertThat(debug, not(containsString("ACONST_NULL")));
    }

    public interface ReturnsPrimitiveBoolean {
        String[] ARGUMENTS = new String[] {};
        boolean execute();
    }
    public void testReturnsPrimitiveBoolean() {
        Compiler compiler = new Compiler(ReturnsPrimitiveBoolean.class, Definition.BUILTINS);

        assertEquals(true, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "true", emptyMap())).execute());
        assertEquals(false, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "false", emptyMap())).execute());
        assertEquals(true, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "Boolean.TRUE", emptyMap())).execute());
        assertEquals(false, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "Boolean.FALSE", emptyMap())).execute());

        assertEquals(true, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "def i = true; i", emptyMap())).execute());
        assertEquals(true,
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "def i = Boolean.TRUE; i", emptyMap())).execute());

        assertEquals(true, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "true || false", emptyMap())).execute());

        String debug = Debugger.toString(ReturnsPrimitiveBoolean.class, "false", new CompilerSettings());
        assertThat(debug, containsString("ICONST_0"));
        // The important thing here is that we have the bytecode for returning an integer instead of an object. booleans are integers.
        assertThat(debug, containsString("IRETURN"));

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "1L", emptyMap())).execute());
        assertEquals("Cannot cast from [long] to [boolean].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "1.1f", emptyMap())).execute());
        assertEquals("Cannot cast from [float] to [boolean].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "1.1d", emptyMap())).execute());
        assertEquals("Cannot cast from [double] to [boolean].", e.getMessage());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "def i = 1L; i", emptyMap())).execute());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "def i = 1.1f; i", emptyMap())).execute());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "def i = 1.1d; i", emptyMap())).execute());

        assertEquals(false, ((ReturnsPrimitiveBoolean)scriptEngine.compile(compiler, null, "int i = 0", emptyMap())).execute());
    }

    public interface ReturnsPrimitiveInt {
        String[] ARGUMENTS = new String[] {};
        int execute();
    }
    public void testReturnsPrimitiveInt() {
        Compiler compiler = new Compiler(ReturnsPrimitiveInt.class, Definition.BUILTINS);

        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "1", emptyMap())).execute());
        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "(int) 1L", emptyMap())).execute());
        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "(int) 1.1d", emptyMap())).execute());
        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "(int) 1.1f", emptyMap())).execute());
        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "Integer.valueOf(1)", emptyMap())).execute());

        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "def i = 1; i", emptyMap())).execute());
        assertEquals(1, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "def i = Integer.valueOf(1); i", emptyMap())).execute());

        assertEquals(2, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "1 + 1", emptyMap())).execute());

        String debug = Debugger.toString(ReturnsPrimitiveInt.class, "1", new CompilerSettings());
        assertThat(debug, containsString("ICONST_1"));
        // The important thing here is that we have the bytecode for returning an integer instead of an object
        assertThat(debug, containsString("IRETURN"));

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "1L", emptyMap())).execute());
        assertEquals("Cannot cast from [long] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "1.1f", emptyMap())).execute());
        assertEquals("Cannot cast from [float] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "1.1d", emptyMap())).execute());
        assertEquals("Cannot cast from [double] to [int].", e.getMessage());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "def i = 1L; i", emptyMap())).execute());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "def i = 1.1f; i", emptyMap())).execute());
        expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "def i = 1.1d; i", emptyMap())).execute());

        assertEquals(0, ((ReturnsPrimitiveInt)scriptEngine.compile(compiler, null, "int i = 0", emptyMap())).execute());
    }

    public interface ReturnsPrimitiveFloat {
        String[] ARGUMENTS = new String[] {};
        float execute();
    }
    public void testReturnsPrimitiveFloat() {
        Compiler compiler = new Compiler(ReturnsPrimitiveFloat.class, Definition.BUILTINS);

        assertEquals(1.1f, ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "1.1f", emptyMap())).execute(), 0);
        assertEquals(1.1f, ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "(float) 1.1d", emptyMap())).execute(), 0);
        assertEquals(1.1f, ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "def d = 1.1f; d", emptyMap())).execute(), 0);
        assertEquals(1.1f,
                ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "def d = Float.valueOf(1.1f); d", emptyMap())).execute(), 0);

        assertEquals(1.1f + 6.7f, ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "1.1f + 6.7f", emptyMap())).execute(), 0);

        Exception e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "1.1d", emptyMap())).execute());
        assertEquals("Cannot cast from [double] to [float].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "def d = 1.1d; d", emptyMap())).execute());
        e = expectScriptThrows(ClassCastException.class, () ->
                ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "def d = Double.valueOf(1.1); d", emptyMap())).execute());

        String debug = Debugger.toString(ReturnsPrimitiveFloat.class, "1f", new CompilerSettings());
        assertThat(debug, containsString("FCONST_1"));
        // The important thing here is that we have the bytecode for returning a float instead of an object
        assertThat(debug, containsString("FRETURN"));

        assertEquals(0.0f, ((ReturnsPrimitiveFloat)scriptEngine.compile(compiler, null, "int i = 0", emptyMap())).execute(), 0);
    }

    public interface ReturnsPrimitiveDouble {
        String[] ARGUMENTS = new String[] {};
        double execute();
    }
    public void testReturnsPrimitiveDouble() {
        Compiler compiler = new Compiler(ReturnsPrimitiveDouble.class, Definition.BUILTINS);

        assertEquals(1.0, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "1", emptyMap())).execute(), 0);
        assertEquals(1.0, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "1L", emptyMap())).execute(), 0);
        assertEquals(1.1, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "1.1d", emptyMap())).execute(), 0);
        assertEquals((double) 1.1f, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "1.1f", emptyMap())).execute(), 0);
        assertEquals(1.1, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "Double.valueOf(1.1)", emptyMap())).execute(), 0);
        assertEquals((double) 1.1f,
               ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "Float.valueOf(1.1f)", emptyMap())).execute(), 0);

        assertEquals(1.0, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = 1; d", emptyMap())).execute(), 0);
        assertEquals(1.0, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = 1L; d", emptyMap())).execute(), 0);
        assertEquals(1.1, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = 1.1d; d", emptyMap())).execute(), 0);
        assertEquals((double) 1.1f,
                ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = 1.1f; d", emptyMap())).execute(), 0);
        assertEquals(1.1,
                ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = Double.valueOf(1.1); d", emptyMap())).execute(), 0);
        assertEquals((double) 1.1f,
                ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "def d = Float.valueOf(1.1f); d", emptyMap())).execute(), 0);

        assertEquals(1.1 + 6.7, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "1.1 + 6.7", emptyMap())).execute(), 0);

        String debug = Debugger.toString(ReturnsPrimitiveDouble.class, "1", new CompilerSettings());
        assertThat(debug, containsString("DCONST_1"));
        // The important thing here is that we have the bytecode for returning a double instead of an object
        assertThat(debug, containsString("DRETURN"));

        assertEquals(0.0, ((ReturnsPrimitiveDouble)scriptEngine.compile(compiler, null, "int i = 0", emptyMap())).execute(), 0);
    }

    public interface NoArgumentsConstant {
        Object execute(String foo);
    }
    public void testNoArgumentsConstant() {
        Compiler compiler = new Compiler(NoArgumentsConstant.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertThat(e.getMessage(), startsWith("Painless needs a constant [String[] ARGUMENTS] on all interfaces it implements with the "
                + "names of the method arguments but [" + NoArgumentsConstant.class.getName() + "] doesn't have one."));
    }

    public interface WrongArgumentsConstant {
        boolean[] ARGUMENTS = new boolean[] {false};
        Object execute(String foo);
    }
    public void testWrongArgumentsConstant() {
        Compiler compiler = new Compiler(WrongArgumentsConstant.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertThat(e.getMessage(), startsWith("Painless needs a constant [String[] ARGUMENTS] on all interfaces it implements with the "
                + "names of the method arguments but [" + WrongArgumentsConstant.class.getName() + "] doesn't have one."));
    }

    public interface WrongLengthOfArgumentConstant {
        String[] ARGUMENTS = new String[] {"foo", "bar"};
        Object execute(String foo);
    }
    public void testWrongLengthOfArgumentConstant() {
        Compiler compiler = new Compiler(WrongLengthOfArgumentConstant.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertThat(e.getMessage(), startsWith("[" + WrongLengthOfArgumentConstant.class.getName() + "#ARGUMENTS] has length [2] but ["
                + WrongLengthOfArgumentConstant.class.getName() + "#execute] takes [1] argument."));
    }

    public interface UnknownArgType {
        String[] ARGUMENTS = new String[] {"foo"};
        Object execute(UnknownArgType foo);
    }
    public void testUnknownArgType() {
        Compiler compiler = new Compiler(UnknownArgType.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgType.class.getName() + ". Painless interfaces can only accept arguments "
                + "that are of whitelisted types.", e.getMessage());
    }

    public interface UnknownReturnType {
        String[] ARGUMENTS = new String[] {"foo"};
        UnknownReturnType execute(String foo);
    }
    public void testUnknownReturnType() {
        Compiler compiler = new Compiler(UnknownReturnType.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertEquals("Painless can only implement execute methods returning a whitelisted type but [" + UnknownReturnType.class.getName()
                + "#execute] returns [" + UnknownReturnType.class.getName() + "] which isn't whitelisted.", e.getMessage());
    }

    public interface UnknownArgTypeInArray {
        String[] ARGUMENTS = new String[] {"foo"};
        Object execute(UnknownArgTypeInArray[] foo);
    }
    public void testUnknownArgTypeInArray() {
        Compiler compiler = new Compiler(UnknownArgTypeInArray.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgTypeInArray.class.getName() + ". Painless interfaces can only accept "
                + "arguments that are of whitelisted types.", e.getMessage());
    }

    public interface TwoExecuteMethods {
        Object execute();
        Object execute(boolean foo);
    }
    public void testTwoExecuteMethods() {
        Compiler compiler = new Compiler(TwoExecuteMethods.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "null", emptyMap()));
        assertEquals("Painless can only implement interfaces that have a single method named [execute] but ["
                + TwoExecuteMethods.class.getName() + "] has more than one.", e.getMessage());
    }

    public interface BadMethod {
        Object something();
    }
    public void testBadMethod() {
        Compiler compiler = new Compiler(BadMethod.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "null", emptyMap()));
        assertEquals("Painless can only implement methods named [execute] and [uses$argName] but [" + BadMethod.class.getName()
                + "] contains a method named [something]", e.getMessage());
    }

    public interface BadUsesReturn {
        String[] ARGUMENTS = new String[] {"foo"};
        Object execute(String foo);
        Object uses$foo();
    }
    public void testBadUsesReturn() {
        Compiler compiler = new Compiler(BadUsesReturn.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that return boolean but [" + BadUsesReturn.class.getName()
                + "#uses$foo] returns [java.lang.Object].", e.getMessage());
    }

    public interface BadUsesParameter {
        String[] ARGUMENTS = new String[] {"foo", "bar"};
        Object execute(String foo, String bar);
        boolean uses$bar(boolean foo);
    }
    public void testBadUsesParameter() {
        Compiler compiler = new Compiler(BadUsesParameter.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that do not take parameters but [" + BadUsesParameter.class.getName()
                + "#uses$bar] does.", e.getMessage());
    }

    public interface BadUsesName {
        String[] ARGUMENTS = new String[] {"foo", "bar"};
        Object execute(String foo, String bar);
        boolean uses$baz();
    }
    public void testBadUsesName() {
        Compiler compiler = new Compiler(BadUsesName.class, Definition.BUILTINS);
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () ->
            scriptEngine.compile(compiler, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that match a parameter name but [" + BadUsesName.class.getName()
                + "#uses$baz] doesn't match any of [foo, bar].", e.getMessage());
    }
}
