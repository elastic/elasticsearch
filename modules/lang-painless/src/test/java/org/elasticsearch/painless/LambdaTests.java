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

import org.apache.lucene.util.Constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class LambdaTests extends ScriptTestCase {

    public void testNoArgLambda() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("Optional.empty().orElseGet(() -> 1);"));
    }

    public void testNoArgLambdaDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("def x = Optional.empty(); x.orElseGet(() -> 1);"));
    }

    public void testLambdaWithArgs() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("short", exec("List l = new ArrayList(); l.add('looooong'); l.add('short'); "
                                 + "l.sort((a, b) -> a.length() - b.length()); return l.get(0)"));

    }

    public void testLambdaWithTypedArgs() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("short", exec("List l = new ArrayList(); l.add('looooong'); l.add('short'); "
                                 + "l.sort((String a, String b) -> a.length() - b.length()); return l.get(0)"));

    }

    public void testPrimitiveLambdas() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(4, exec("List l = new ArrayList(); l.add(1); l.add(1); "
                           + "return l.stream().mapToInt(x -> x + 1).sum();"));
    }

    public void testPrimitiveLambdasWithTypedArgs() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(4, exec("List l = new ArrayList(); l.add(1); l.add(1); "
                           + "return l.stream().mapToInt(int x -> x + 1).sum();"));
    }

    public void testPrimitiveLambdasDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(4, exec("def l = new ArrayList(); l.add(1); l.add(1); "
                           + "return l.stream().mapToInt(x -> x + 1).sum();"));
    }

    public void testPrimitiveLambdasWithTypedArgsDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(4, exec("def l = new ArrayList(); l.add(1); l.add(1); "
                           + "return l.stream().mapToInt(int x -> x + 1).sum();"));
    }

    public void testPrimitiveLambdasConvertible() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("List l = new ArrayList(); l.add(1.0); l.add(1); "
                           + "return l.stream().mapToInt(long x -> 1).sum();"));
    }

    public void testPrimitiveArgs() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int applyOne(IntFunction arg) { arg.apply(1) } applyOne(x -> x + 1)"));
    }

    public void testPrimitiveArgsTyped() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int applyOne(IntFunction arg) { arg.apply(1) } applyOne(int x -> x + 1)"));
    }

    public void testPrimitiveArgsTypedOddly() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2L, exec("long applyOne(IntFunction arg) { arg.apply(1) } applyOne(long x -> x + 1)"));
    }

    public void testMultipleStatements() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int applyOne(IntFunction arg) { arg.apply(1) } applyOne(x -> { def y = x + 1; return y })"));
    }

    public void testUnneededCurlyStatements() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int applyOne(IntFunction arg) { arg.apply(1) } applyOne(x -> { x + 1 })"));
    }

    /** interface ignores return value */
    public void testVoidReturn() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("List list = new ArrayList(); "
                           + "list.add(2); "
                           + "List list2 = new ArrayList(); "
                           + "list.forEach(x -> list2.add(x));"
                           + "return list[0]"));
    }

    /** interface ignores return value */
    public void testVoidReturnDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("def list = new ArrayList(); "
                           + "list.add(2); "
                           + "List list2 = new ArrayList(); "
                           + "list.forEach(x -> list2.add(x));"
                           + "return list[0]"));
    }

    public void testTwoLambdas() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("testingcdefg", exec(
                "org.elasticsearch.painless.FeatureTest test = new org.elasticsearch.painless.FeatureTest(2,3);" +
                "return test.twoFunctionsOfX(x -> 'testing'.concat(x), y -> 'abcdefg'.substring(y))"));
    }

    public void testNestedLambdas() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("Optional.empty().orElseGet(() -> Optional.empty().orElseGet(() -> 1));"));
    }

    public void testLambdaInLoop() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(100, exec("int sum = 0; " +
                               "for (int i = 0; i < 100; i++) {" +
                               "  sum += Optional.empty().orElseGet(() -> 1);" +
                               "}" +
                               "return sum;"));
    }

    public void testCapture() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(5, exec("int x = 5; return Optional.empty().orElseGet(() -> x);"));
    }

    public void testTwoCaptures() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("1test", exec("int x = 1; String y = 'test'; return Optional.empty().orElseGet(() -> x + y);"));
    }

    public void testCapturesAreReadOnly() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(1); l.add(1); "
                    + "return l.stream().mapToInt(x -> { l = null; return x + 1 }).sum();");
        });
        assertTrue(expected.getMessage().contains("is read-only"));
    }

    @AwaitsFix(bugUrl = "def type tracking")
    public void testOnlyCapturesAreReadOnly() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(4, exec("List l = new ArrayList(); l.add(1); l.add(1); "
                           + "return l.stream().mapToInt(x -> { x += 1; return x }).sum();"));
    }

    /** Lambda parameters shouldn't be able to mask a variable already in scope */
    public void testNoParamMasking() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int x = 0; List l = new ArrayList(); l.add(1); l.add(1); "
                    + "return l.stream().mapToInt(x -> { x += 1; return x }).sum();");
        });
        assertTrue(expected.getMessage().contains("already defined"));
    }

    public void testCaptureDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(5, exec("int x = 5; def y = Optional.empty(); y.orElseGet(() -> x);"));
    }

    public void testNestedCapture() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("boolean x = false; int y = 1;" +
                             "return Optional.empty().orElseGet(() -> x ? 5 : Optional.empty().orElseGet(() -> y));"));
    }

    public void testNestedCaptureParams() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int foo(Function f) { return f.apply(1) }" +
                             "return foo(x -> foo(y -> x + 1))"));
    }

    public void testWrongArity() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("Optional.empty().orElseGet(x -> x);");
        });
        assertTrue(expected.getMessage().contains("Incorrect number of parameters"));
    }

    public void testWrongArityDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def y = Optional.empty(); return y.orElseGet(x -> x);");
        });
        assertTrue(expected.getMessage(), expected.getMessage().contains("Incorrect number of parameters"));
    }

    public void testWrongArityNotEnough() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(1); l.add(1); "
               + "return l.stream().mapToInt(() -> 5).sum();");
        });
        assertTrue(expected.getMessage().contains("Incorrect number of parameters"));
    }

    public void testWrongArityNotEnoughDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def l = new ArrayList(); l.add(1); l.add(1); "
               + "return l.stream().mapToInt(() -> 5).sum();");
        });
        assertTrue(expected.getMessage().contains("Incorrect number of parameters"));
    }

    public void testLambdaInFunction() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(5, exec("def foo() { Optional.empty().orElseGet(() -> 5) } return foo();"));
    }

    public void testLambdaCaptureFunctionParam() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(5, exec("def foo(int x) { Optional.empty().orElseGet(() -> x) } return foo(5);"));
    }

    public void testReservedCapture() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        String compare = "boolean compare(Supplier s, def v) {s.get() == v}";
        assertEquals(true, exec(compare + "compare(() -> new ArrayList(), new ArrayList())"));
        assertEquals(true, exec(compare + "compare(() -> { new ArrayList() }, new ArrayList())"));

        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        params.put("number", 2);

        assertEquals(true, exec(compare + "compare(() -> { return params['key'] }, 'value')", params, true));
        assertEquals(false, exec(compare + "compare(() -> { return params['nokey'] }, 'value')", params, true));
        assertEquals(true, exec(compare + "compare(() -> { return params['nokey'] }, null)", params, true));
        assertEquals(true, exec(compare + "compare(() -> { return params['number'] }, 2)", params, true));
        assertEquals(false, exec(compare + "compare(() -> { return params['number'] }, 'value')", params, true));
        assertEquals(false, exec(compare + "compare(() -> { if (params['number'] == 2) { return params['number'] }" +
            "else { return params['key'] } }, 'value')", params, true));
        assertEquals(true, exec(compare + "compare(() -> { if (params['number'] == 2) { return params['number'] }" +
            "else { return params['key'] } }, 2)", params, true));
        assertEquals(true, exec(compare + "compare(() -> { if (params['number'] == 1) { return params['number'] }" +
            "else { return params['key'] } }, 'value')", params, true));
        assertEquals(false, exec(compare + "compare(() -> { if (params['number'] == 1) { return params['number'] }" +
            "else { return params['key'] } }, 2)", params, true));
    }

    public void testReturnVoid() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Throwable expected = expectScriptThrows(ClassCastException.class, () -> {
            exec("StringBuilder b = new StringBuilder(); List l = [1, 2]; l.stream().mapToLong(i -> b.setLength(i))");
        });
        assertThat(expected.getMessage(), containsString("Cannot cast from [void] to [long]."));
    }

    public void testReturnVoidDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        // If we can catch the error at compile time we do
        Exception expected = expectScriptThrows(ClassCastException.class, () -> {
            exec("StringBuilder b = new StringBuilder(); def l = [1, 2]; l.stream().mapToLong(i -> b.setLength(i))");
        });
        assertThat(expected.getMessage(), containsString("Cannot cast from [void] to [def]."));

        // Otherwise we convert the void into a null
        assertEquals(Arrays.asList(null, null),
                exec("def b = new StringBuilder(); def l = [1, 2]; l.stream().map(i -> b.setLength(i)).collect(Collectors.toList())"));
        assertEquals(Arrays.asList(null, null),
                exec("def b = new StringBuilder(); List l = [1, 2]; l.stream().map(i -> b.setLength(i)).collect(Collectors.toList())"));
    }
}
