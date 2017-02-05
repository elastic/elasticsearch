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

import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.MainMethod.DerivedArgument;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.startsWith;

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
        Object test(@Arg(name = "arg", type = "def") Object foo);
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
    public interface DefPrimitive {
        Object test(@Arg(name = "arg", type = "def") int foo);
    }
    public void testDefPrimitive() {
        Object rando = randomInt();
        assertEquals(rando, scriptEngine.compile(OneArg.class, null, "arg", emptyMap()).test(rando));
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
    }

    public void testDerivedArgument() {
        ManyArgs script = scriptEngine.compile(ManyArgs.class, null, "a2", emptyMap(),
                new DerivedArgument(Definition.INT_TYPE, "a2", (writer, locals) -> {
                    // final int a2 = 2 * a;
                    Variable a = locals.getVariable(null, "a");
                    Variable a2 = locals.getVariable(null, "a2");

                    writer.push(2);
                    writer.visitVarInsn(Opcodes.ILOAD, a.getSlot());
                    writer.math(GeneratorAdapter.MUL, Definition.INT_TYPE.type);
                    writer.visitVarInsn(Opcodes.ISTORE, a2.getSlot());
                }));
        assertEquals(2, script.test(1, 0, 0, 0));
    }

    public void testManyDerivedArguments() {
        Function<String, DerivedArgument> build = varName -> new DerivedArgument(Definition.INT_TYPE, varName + "2", (writer, locals) -> {
            // final int a2 = 2 * a;
            Variable a = locals.getVariable(null, varName);
            Variable a2 = locals.getVariable(null, varName + "2");

            writer.push(2);
            writer.visitVarInsn(Opcodes.ILOAD, a.getSlot());
            writer.math(GeneratorAdapter.MUL, Definition.INT_TYPE.type);
            writer.visitVarInsn(Opcodes.ISTORE, a2.getSlot());
        });
        ManyArgs script = scriptEngine.compile(ManyArgs.class, null, "a2 + b2 + c2 + d2", emptyMap(),
                build.apply("a"), build.apply("b"), build.apply("c"), build.apply("d"));
        assertEquals(20, script.test(1, 2, 3, 4));
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
    public interface CannotInferArgType {
        Object test(@Arg(name = "foo") CannotInferArgType foo);
    }
    public void testCannotInferArgType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(CannotInferArgType.class, null, "1", emptyMap()));
        assertEquals("Can't infer Painless type for argument [foo]. Use the 'type' element of the "
                + "@Arg annotation to specify a whitelisted type.", e.getMessage());
    }

    @FunctionalInterface
    public interface UnknownArgType {
        Object test(@Arg(type = "UnknownArgType", name = "foo") UnknownArgType foo);
    }
    public void testUnknownArgType() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(UnknownArgType.class, null, "1", emptyMap()));
        assertEquals("Argument type [UnknownArgType] on [foo] isn't whitelisted.", e.getMessage());
    }

    @FunctionalInterface
    public interface ArgNotAssignable {
        Object test(@Arg(type = "String", name = "foo") Object foo);
    }
    public void testArgNotAssignable() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(ArgNotAssignable.class, null, "foo", emptyMap()));
        assertEquals("Painless argument type [String] not assignable from interface argument type [java.lang.Object] for [foo].",
                e.getMessage());
    }

    @FunctionalInterface
    public interface ArgNotAssignableDimsBig {
        Object test(@Arg(type = "int[]", name = "foo") int foo);
    }
    public void testArgNotAssignableDimsBig() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(ArgNotAssignableDimsBig.class, null, "foo", emptyMap()));
        assertEquals("Painless argument type [int[]] not assignable from interface argument type [int] for [foo].",
                e.getMessage());
    }

    @FunctionalInterface
    public interface ArgNotAssignableDimsSmall {
        Object test(@Arg(type = "int", name = "foo") int[] foo);
    }
    public void testArgNotAssignableDimsSmall() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () ->
            scriptEngine.compile(ArgNotAssignableDimsSmall.class, null, "foo", emptyMap()));
        assertEquals("Painless argument type [int] not assignable from interface argument type [int[]] for [foo].",
                e.getMessage());
    }

    public void testDerivedArgumentUncalledIfUnused() {
        ManyArgs script = scriptEngine.compile(ManyArgs.class, null, "a", emptyMap(),
                new DerivedArgument(Definition.INT_TYPE, "a2", (writer, locals) -> fail("shouldn't be called")));
        assertEquals(1, script.test(1, 0, 0, 0));
    }
}
