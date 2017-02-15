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

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for Painless implementing different interfaces.
 */
public class ImplementInterfacesTests extends ScriptTestCase {
    public interface NoArgs {
        Object execute();
    }
    public void testNoArgs() {
        assertEquals(1, scriptEngine.compile(NoArgs.class, null, "1", emptyMap()).execute());
        assertEquals("foo", scriptEngine.compile(NoArgs.class, null, "'foo'", emptyMap()).execute());

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());
    }

    public interface OneArg {
        Object execute(@Arg(name = "arg") Object foo);
    }
    public void testOneArg() {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile(OneArg.class, null, "arg", emptyMap()).execute(rando));
        rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(OneArg.class, null, "arg", emptyMap()).execute(rando));

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());
    }

    public interface ArrayArg {
        Object execute(@Arg(name = "arg") String[] arg);
    }
    public void testArrayArg() {
        String rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(ArrayArg.class, null, "arg[0]", emptyMap()).execute(new String[] {rando, "foo"}));
    }

    public interface PrimitiveArrayArg {
        Object execute(@Arg(name = "arg") int[] arg);
    }
    public void testPrimitiveArrayArg() {
        int rando = randomInt();
        assertEquals(rando, scriptEngine.compile(PrimitiveArrayArg.class, null, "arg[0]", emptyMap()).execute(new int[] {rando, 10}));
    }

    public interface DefArrayArg {
        Object execute(@Arg(name = "arg") Object[] arg);
    }
    public void testDefArrayArg() {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile(DefArrayArg.class, null, "arg[0]", emptyMap()).execute(new Object[] {rando, 10}));
        rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(DefArrayArg.class, null, "arg[0]", emptyMap()).execute(new Object[] {rando, 10}));
        assertEquals(5, scriptEngine.compile(DefArrayArg.class, null, "arg[0].length()", emptyMap()).execute(new Object[] {rando, 10}));
    }

    public interface ManyArgs {
        Object execute(
                @Arg(name = "a") int a,
                @Arg(name = "b") int b,
                @Arg(name = "c") int c,
                @Arg(name = "d") int d);
        boolean uses$a();
        boolean uses$b();
        boolean uses$c();
        boolean uses$d();
    }
    public void testManyArgs() {
        int rando = randomInt();
        assertEquals(rando, scriptEngine.compile(ManyArgs.class, null, "a", emptyMap()).execute(rando, 0, 0, 0));
        assertEquals(10, scriptEngine.compile(ManyArgs.class, null, "a + b + c + d", emptyMap()).execute(1, 2, 3, 4));

        // While we're here we can verify that painless correctly finds used variables
        ManyArgs script = scriptEngine.compile(ManyArgs.class, null, "a", emptyMap());
        assertTrue(script.uses$a());
        assertFalse(script.uses$b());
        assertFalse(script.uses$c());
        assertFalse(script.uses$d());
        script = scriptEngine.compile(ManyArgs.class, null, "a + b + c", emptyMap());
        assertTrue(script.uses$a());
        assertTrue(script.uses$b());
        assertTrue(script.uses$c());
        assertFalse(script.uses$d());
        script = scriptEngine.compile(ManyArgs.class, null, "a + b + c + d", emptyMap());
        assertTrue(script.uses$a());
        assertTrue(script.uses$b());
        assertTrue(script.uses$c());
        assertTrue(script.uses$d());
    }

    public interface VarargTest {
        Object execute(@Arg(name="arg") String... arg);
    }
    public void testVararg() {
        assertEquals("foo bar baz", scriptEngine.compile(VarargTest.class, null, "String.join(' ', Arrays.asList(arg))", emptyMap())
                    .execute("foo", "bar", "baz"));
    }

    public interface NoAnnotationOnArg {
        Object execute(String foo);
    }
    public void testNoAnnotationOnArg() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoAnnotationOnArg.class, null, "1", emptyMap()));
        assertThat(e.getMessage(), startsWith("All arguments must be annotated with @Arg but the [1]th argument of"));
    }

    public interface UnknownArgType {
        Object execute(@Arg(name = "foo") UnknownArgType foo);
    }
    public void testUnknownArgType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(UnknownArgType.class, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgType.class.getName() + ". Painless interfaces can only accept arguments "
                + "that are of whitelisted types.", e.getMessage());
    }

    public interface UnknownArgTypeInArray {
        Object execute(@Arg(name = "foo") UnknownArgTypeInArray[] foo);
    }
    public void testUnknownArgTypeInArray() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(UnknownArgTypeInArray.class, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgTypeInArray.class.getName() + ". Painless interfaces can only accept "
                + "arguments that are of whitelisted types.", e.getMessage());
    }

    public interface TwoExecuteMethods {
        Object execute();
        Object execute(boolean foo);
    }
    public void testTwoExecuteMethods() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(TwoExecuteMethods.class, null, "null", emptyMap()));
        assertEquals("Painless can only implement interfaces that have a single method named [execute] but ["
                + TwoExecuteMethods.class.getName() + "] has more than one.", e.getMessage());
    }

    public interface BadMethod {
        Object something();
    }
    public void testBadMethod() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(BadMethod.class, null, "null", emptyMap()));
        assertEquals("Painless can only implement methods named [execute] and [uses$varName] but [" + BadMethod.class.getName()
                + "] contains a method named [something]", e.getMessage());
    }

    public interface BadUsesReturn {
        Object execute(String foo);
        Object uses$foo();
    }
    public void testBadUsesReturn() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(BadUsesReturn.class, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that return boolean but [" + BadUsesReturn.class.getName()
                + "#uses$foo] returns [java.lang.Object].", e.getMessage());
    }

    public interface BadUsesParameter {
        Object execute(@Arg(name="foo") String foo, @Arg(name="bar") String bar);
        boolean uses$bar(boolean foo);
    }
    public void testBadUsesParameter() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(BadUsesParameter.class, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that do not take parameters but [" + BadUsesParameter.class.getName()
                + "#uses$bar] does.", e.getMessage());
    }

    public interface BadUsesName {
        Object execute(@Arg(name="foo") String foo, @Arg(name="bar") String bar);
        boolean uses$baz();
    }
    public void testBadUsesName() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(BadUsesName.class, null, "null", emptyMap()));
        assertEquals("Painless can only implement uses$ methods that match a parameter name but [" + BadUsesName.class.getName()
                + "#uses$baz] doesn't match any of [foo, bar].", e.getMessage());
    }
}
