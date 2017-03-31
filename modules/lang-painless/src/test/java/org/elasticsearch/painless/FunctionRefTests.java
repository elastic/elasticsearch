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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.lang.invoke.LambdaConversionException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public class FunctionRefTests extends ScriptTestCase {

    public void testStaticMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }

    public void testStaticMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("def l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }

    public void testVirtualMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("List l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }

    public void testVirtualMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("def l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }

    public void testQualifiedStaticMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(true,
                exec("List l = [true]; l.stream().map(org.elasticsearch.painless.FeatureTest::overloadedStatic).findFirst().get()"));
    }

    public void testQualifiedStaticMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(true,
                exec("def l = [true]; l.stream().map(org.elasticsearch.painless.FeatureTest::overloadedStatic).findFirst().get()"));
    }

    public void testQualifiedVirtualMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        long instant = randomLong();
        assertEquals(instant, exec(
                "List l = [params.d]; return l.stream().mapToLong(org.joda.time.ReadableDateTime::getMillis).sum()",
                singletonMap("d", new DateTime(instant, DateTimeZone.UTC)), true));
    }

    public void testQualifiedVirtualMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        long instant = randomLong();
        assertEquals(instant, exec(
                "def l = [params.d]; return l.stream().mapToLong(org.joda.time.ReadableDateTime::getMillis).sum()",
                singletonMap("d", new DateTime(instant, DateTimeZone.UTC)), true));
    }

    public void testCtorMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(3.0D,
                exec("List l = new ArrayList(); l.add(1.0); l.add(2.0); " +
                        "DoubleStream doubleStream = l.stream().mapToDouble(Double::doubleValue);" +
                        "DoubleSummaryStatistics stats = doubleStream.collect(DoubleSummaryStatistics::new, " +
                        "DoubleSummaryStatistics::accept, " +
                        "DoubleSummaryStatistics::combine); " +
                        "return stats.getSum()"));
    }

    public void testCtorMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(3.0D,
            exec("def l = new ArrayList(); l.add(1.0); l.add(2.0); " +
                 "def doubleStream = l.stream().mapToDouble(Double::doubleValue);" +
                 "def stats = doubleStream.collect(DoubleSummaryStatistics::new, " +
                                                  "DoubleSummaryStatistics::accept, " +
                                                  "DoubleSummaryStatistics::combine); " +
                 "return stats.getSum()"));
    }

    public void testArrayCtorMethodRef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1.0D,
                exec("List l = new ArrayList(); l.add(1.0); l.add(2.0); " +
                     "def[] array = l.stream().toArray(Double[]::new);" +
                     "return array[0];"));
    }

    public void testArrayCtorMethodRefDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1.0D,
                exec("def l = new ArrayList(); l.add(1.0); l.add(2.0); " +
                     "def[] array = l.stream().toArray(Double[]::new);" +
                     "return array[0];"));
    }

    public void testCapturingMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("5", exec("Integer x = Integer.valueOf(5); return Optional.empty().orElseGet(x::toString);"));
        assertEquals("[]", exec("List l = new ArrayList(); return Optional.empty().orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefImpl() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("5", exec("def x = Integer.valueOf(5); return Optional.empty().orElseGet(x::toString);"));
        assertEquals("[]", exec("def l = new ArrayList(); return Optional.empty().orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefInterface() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("5", exec("Integer x = Integer.valueOf(5); def opt = Optional.empty(); return opt.orElseGet(x::toString);"));
        assertEquals("[]", exec("List l = new ArrayList(); def opt = Optional.empty(); return opt.orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefEverywhere() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("5", exec("def x = Integer.valueOf(5); def opt = Optional.empty(); return opt.orElseGet(x::toString);"));
        assertEquals("[]", exec("def l = new ArrayList(); def opt = Optional.empty(); return opt.orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceMultipleLambdas() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("testingcdefg", exec(
                "String x = 'testing';" +
                "String y = 'abcdefg';" +
                "org.elasticsearch.painless.FeatureTest test = new org.elasticsearch.painless.FeatureTest(2,3);" +
                "return test.twoFunctionsOfX(x::concat, y::substring);"));
    }

    public void testCapturingMethodReferenceMultipleLambdasDefImpls() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("testingcdefg", exec(
                "def x = 'testing';" +
                "def y = 'abcdefg';" +
                "org.elasticsearch.painless.FeatureTest test = new org.elasticsearch.painless.FeatureTest(2,3);" +
                "return test.twoFunctionsOfX(x::concat, y::substring);"));
    }

    public void testCapturingMethodReferenceMultipleLambdasDefInterface() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("testingcdefg", exec(
                "String x = 'testing';" +
                "String y = 'abcdefg';" +
                "def test = new org.elasticsearch.painless.FeatureTest(2,3);" +
                "return test.twoFunctionsOfX(x::concat, y::substring);"));
    }

    public void testCapturingMethodReferenceMultipleLambdasDefEverywhere() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("testingcdefg", exec(
                "def x = 'testing';" +
                "def y = 'abcdefg';" +
                "def test = new org.elasticsearch.painless.FeatureTest(2,3);" +
                "return test.twoFunctionsOfX(x::concat, y::substring);"));
    }

    public void testOwnStaticMethodReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int mycompare(int i, int j) { j - i } " +
                             "List l = new ArrayList(); l.add(2); l.add(1); l.sort(this::mycompare); return l.get(0);"));
    }

    public void testOwnStaticMethodReferenceDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, exec("int mycompare(int i, int j) { j - i } " +
                             "def l = new ArrayList(); l.add(2); l.add(1); l.sort(this::mycompare); return l.get(0);"));
    }

    public void testInterfaceDefaultMethod() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("bar", exec("String f(BiFunction function) { function.apply('foo', 'bar') }" +
                                 "Map map = new HashMap(); f(map::getOrDefault)"));
    }

    public void testInterfaceDefaultMethodDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("bar", exec("String f(BiFunction function) { function.apply('foo', 'bar') }" +
                                 "def map = new HashMap(); f(map::getOrDefault)"));
    }

    public void testMethodMissing() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = [2, 1]; l.sort(Integer::bogus); return l.get(0);");
        });
        assertThat(e.getMessage(), startsWith("Unknown reference"));
    }

    public void testQualifiedMethodMissing() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = [2, 1]; l.sort(org.joda.time.ReadableDateTime::bogus); return l.get(0);", false);
        });
        assertThat(e.getMessage(), startsWith("Unknown reference"));
    }

    public void testClassMissing() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = [2, 1]; l.sort(Bogus::bogus); return l.get(0);", false);
        });
        assertThat(e.getMessage(), endsWith("Variable [Bogus] is not defined."));
    }

    public void testQualifiedClassMissing() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = [2, 1]; l.sort(org.joda.time.BogusDateTime::bogus); return l.get(0);", false);
        });
        /* Because the type isn't known and we use the lexer hack this fails to parse. I find this error message confusing but it is the one
         * we have... */
        assertEquals("invalid sequence of tokens near ['::'].", e.getMessage());
    }

    public void testNotFunctionalInterface() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.add(Integer::bogus); return l.get(0);");
        });
        assertThat(expected.getMessage(), containsString("Cannot convert function reference"));
    }

    public void testIncompatible() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        expectScriptThrows(BootstrapMethodError.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(String::startsWith); return l.get(0);");
        });
    }

    public void testWrongArity() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("Optional.empty().orElseGet(String::startsWith);");
        });
        assertThat(expected.getMessage(), containsString("Unknown reference"));
    }

    public void testWrongArityNotEnough() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(String::isEmpty);");
        });
        assertTrue(expected.getMessage().contains("Unknown reference"));
    }

    public void testWrongArityDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def y = Optional.empty(); return y.orElseGet(String::startsWith);");
        });
        assertThat(expected.getMessage(), containsString("Unknown reference"));
    }

    public void testWrongArityNotEnoughDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def l = new ArrayList(); l.add(2); l.add(1); l.sort(String::isEmpty);");
        });
        assertThat(expected.getMessage(), containsString("Unknown reference"));
    }

    public void testReturnVoid() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Throwable expected = expectScriptThrows(BootstrapMethodError.class, () -> {
            Object value = exec("StringBuilder b = new StringBuilder(); List l = [1, 2]; l.stream().mapToLong(b::setLength).sum();");
        });
        assertThat(expected.getCause().getMessage(),
                containsString("Type mismatch for lambda expected return: void is not convertible to long"));
    }

    public void testReturnVoidDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Exception expected = expectScriptThrows(LambdaConversionException.class, () -> {
            exec("StringBuilder b = new StringBuilder(); def l = [1, 2]; l.stream().mapToLong(b::setLength);");
        });
        assertThat(expected.getMessage(), containsString("Type mismatch for lambda expected return: void is not convertible to long"));

        expected = expectScriptThrows(LambdaConversionException.class, () -> {
            exec("def b = new StringBuilder(); def l = [1, 2]; l.stream().mapToLong(b::setLength);");
        });
        assertThat(expected.getMessage(), containsString("Type mismatch for lambda expected return: void is not convertible to long"));

        expected = expectScriptThrows(LambdaConversionException.class, () -> {
            exec("def b = new StringBuilder(); List l = [1, 2]; l.stream().mapToLong(b::setLength);");
        });
        assertThat(expected.getMessage(), containsString("Type mismatch for lambda expected return: void is not convertible to long"));
    }
}
