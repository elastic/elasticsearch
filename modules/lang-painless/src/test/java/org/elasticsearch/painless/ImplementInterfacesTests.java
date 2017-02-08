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

import java.util.Arrays;
import java.util.HashSet;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for Painless implementing different interfaces.
 */
public class ImplementInterfacesTests extends ScriptTestCase {
    @FunctionalInterface
    public interface NoArgs {
        Object test();
    }
    public void testNoArgs() {
        assertEquals(1, scriptEngine.compile(NoArgs.class, null, "1", emptyMap()).test());
        assertEquals("foo", scriptEngine.compile(NoArgs.class, null, "'foo'", emptyMap()).test());

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());
    }

    @FunctionalInterface
    public interface OneArg {
        Object test(@Arg(name = "arg") Object foo);
    }
    public void testOneArg() {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile(OneArg.class, null, "arg", emptyMap()).test(rando));
        rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(OneArg.class, null, "arg", emptyMap()).test(rando));

        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "doc", emptyMap()));
        assertEquals("Variable [doc] is not defined.", e.getMessage());
        // _score was once embedded into painless by deep magic
        e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoArgs.class, null, "_score", emptyMap()));
        assertEquals("Variable [_score] is not defined.", e.getMessage());
    }

    @FunctionalInterface
    public interface ArrayArg {
        Object test(@Arg(name = "arg") String[] arg);
    }
    public void testArrayArg() {
        String rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(ArrayArg.class, null, "arg[0]", emptyMap()).test(new String[] {rando, "foo"}));
    }

    @FunctionalInterface
    public interface PrimitiveArrayArg {
        Object test(@Arg(name = "arg") int[] arg);
    }
    public void testPrimitiveArrayArg() {
        int rando = randomInt();
        assertEquals(rando, scriptEngine.compile(PrimitiveArrayArg.class, null, "arg[0]", emptyMap()).test(new int[] {rando, 10}));
    }

    @FunctionalInterface
    public interface DefArrayArg {
        Object test(@Arg(name = "arg") Object[] arg);
    }
    public void testDefArrayArg() {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile(DefArrayArg.class, null, "arg[0]", emptyMap()).test(new Object[] {rando, 10}));
        rando = randomAsciiOfLength(5);
        assertEquals(rando, scriptEngine.compile(DefArrayArg.class, null, "arg[0]", emptyMap()).test(new Object[] {rando, 10}));
        assertEquals(5, scriptEngine.compile(DefArrayArg.class, null, "arg[0].length()", emptyMap()).test(new Object[] {rando, 10}));
    }

    @FunctionalInterface
    public interface ManyArgs {
        Object test(
                @Arg(name = "a") int a,
                @Arg(name = "b") int b,
                @Arg(name = "c") int c,
                @Arg(name = "d") int d);
    }
    public void testManyArgs() {
        int rando = randomInt();
        assertEquals(rando, scriptEngine.compile(ManyArgs.class, null, "a", emptyMap()).test(rando, 0, 0, 0));
        assertEquals(10, scriptEngine.compile(ManyArgs.class, null, "a + b + c + d", emptyMap()).test(1, 2, 3, 4));

        // While we're here we can verify that painless correctly finds used variables
        assertUsedVariables(ManyArgs.class, "a", "a");
        assertUsedVariables(ManyArgs.class, "a + b + c", "a", "b", "c");
        assertUsedVariables(ManyArgs.class, "a + b + c + d", "a", "b", "c", "d");
    }

    private void assertUsedVariables(Class<?> iface, String script, String... usedVariables) {
        assertEquals(new HashSet<>(Arrays.asList(usedVariables)),
                ((PainlessScript) scriptEngine.compile(iface, null, script, emptyMap())).getMetadata().getUsedVariables());
    }

    @FunctionalInterface
    public interface NoAnnotationOnArg {
        Object test(String foo);
    }
    public void testNoAnnotationOnArg() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(NoAnnotationOnArg.class, null, "1", emptyMap()));
        assertThat(e.getMessage(), startsWith("All arguments must be annotated with @Arg but the [1]th argument of"));
    }

    @FunctionalInterface
    public interface UnknownArgType {
        Object test(@Arg(name = "foo") UnknownArgType foo);
    }
    public void testUnknownArgType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(UnknownArgType.class, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgType.class.getName() + ". Painless interfaces can only accept arguments "
                + "that are of whitelisted types.", e.getMessage());
    }

    @FunctionalInterface
    public interface UnknownArgTypeInArray {
        Object test(@Arg(name = "foo") UnknownArgTypeInArray[] foo);
    }
    public void testUnknownArgTypeInArray() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(UnknownArgTypeInArray.class, null, "1", emptyMap()));
        assertEquals("[foo] is of unknown type [" + UnknownArgTypeInArray.class.getName() + ". Painless interfaces can only accept "
                + "arguments that are of whitelisted types.", e.getMessage());
    }

    @FunctionalInterface
    public interface MethodClash {
        PainlessScript.ScriptMetadata getMetadata();
    }
    public void testMethodClash() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(MethodClash.class, null, "null", emptyMap()));
        assertEquals("Painless cannot compile [" + MethodClash.class.getName() + "] because it contains a method named "
                + "[getMetadata] which can collide with PainlessScript#getMetadata", e.getMessage());
    }
}
