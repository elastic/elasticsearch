/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.lang.invoke.LambdaConversionException;
import java.time.Instant;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;

public class FunctionRefTests extends ScriptTestCase {

    public void testStaticMethodReference() {
        assertEquals(1, exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }

    public void testStaticMethodReferenceDef() {
        assertEquals(1, exec("def l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }

    public void testVirtualMethodReference() {
        assertEquals(2, exec("List l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }

    public void testVirtualMethodReferenceDef() {
        assertEquals(2, exec("def l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }

    public void testQualifiedStaticMethodReference() {
        assertEquals(
            true,
            exec("List l = [true]; l.stream().map(org.elasticsearch.painless.FeatureTestObject::overloadedStatic).findFirst().get()")
        );
    }

    public void testQualifiedStaticMethodReferenceDef() {
        assertEquals(
            true,
            exec("def l = [true]; l.stream().map(org.elasticsearch.painless.FeatureTestObject::overloadedStatic).findFirst().get()")
        );
    }

    public void testQualifiedVirtualMethodReference() {
        long instant = randomLong();
        assertEquals(
            instant,
            exec(
                "List l = [params.d]; return l.stream().mapToLong(Instant::toEpochMilli).sum()",
                singletonMap("d", Instant.ofEpochMilli(instant)),
                true
            )
        );
    }

    public void testQualifiedVirtualMethodReferenceDef() {
        long instant = randomLong();
        assertEquals(
            instant,
            exec(
                "def l = [params.d]; return l.stream().mapToLong(Instant::toEpochMilli).sum()",
                singletonMap("d", Instant.ofEpochMilli(instant)),
                true
            )
        );
    }

    public void testCtorMethodReference() {
        assertEquals(
            3.0D,
            exec(
                "List l = new ArrayList(); l.add(1.0); l.add(2.0); "
                    + "DoubleStream doubleStream = l.stream().mapToDouble(Double::doubleValue);"
                    + "DoubleSummaryStatistics stats = doubleStream.collect(DoubleSummaryStatistics::new, "
                    + "DoubleSummaryStatistics::accept, "
                    + "DoubleSummaryStatistics::combine); "
                    + "return stats.getSum()"
            )
        );
    }

    public void testCtorMethodReferenceDef() {
        assertEquals(
            3.0D,
            exec(
                "def l = new ArrayList(); l.add(1.0); l.add(2.0); "
                    + "def doubleStream = l.stream().mapToDouble(Double::doubleValue);"
                    + "def stats = doubleStream.collect(DoubleSummaryStatistics::new, "
                    + "DoubleSummaryStatistics::accept, "
                    + "DoubleSummaryStatistics::combine); "
                    + "return stats.getSum()"
            )
        );
    }

    public void testCtorWithParams() {
        assertArrayEquals(
            new Object[] { "foo", "bar" },
            (Object[]) exec(
                "List l = new ArrayList(); l.add('foo'); l.add('bar'); "
                    + "Stream stream = l.stream().map(StringBuilder::new);"
                    + "return stream.map(Object::toString).toArray()"
            )
        );
    }

    public void testArrayCtorMethodRef() {
        assertEquals(
            1.0D,
            exec(
                "List l = new ArrayList(); l.add(1.0); l.add(2.0); "
                    + "def[] array = l.stream().toArray(Double[]::new);"
                    + "return array[0];"
            )
        );
    }

    public void testArrayCtorMethodRefDef() {
        assertEquals(
            1.0D,
            exec(
                "def l = new ArrayList(); l.add(1.0); l.add(2.0); "
                    + "def[] array = l.stream().toArray(Double[]::new);"
                    + "return array[0];"
            )
        );
    }

    public void testCapturingMethodReference() {
        assertEquals("5", exec("Integer x = Integer.valueOf(5); return Optional.empty().orElseGet(x::toString);"));
        assertEquals("[]", exec("List l = new ArrayList(); return Optional.empty().orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefImpl() {
        assertEquals("5", exec("def x = Integer.valueOf(5); return Optional.empty().orElseGet(x::toString);"));
        assertEquals("[]", exec("def l = new ArrayList(); return Optional.empty().orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefInterface() {
        assertEquals("5", exec("Integer x = Integer.valueOf(5); def opt = Optional.empty(); return opt.orElseGet(x::toString);"));
        assertEquals("[]", exec("List l = new ArrayList(); def opt = Optional.empty(); return opt.orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceDefEverywhere() {
        assertEquals("5", exec("def x = Integer.valueOf(5); def opt = Optional.empty(); return opt.orElseGet(x::toString);"));
        assertEquals("[]", exec("def l = new ArrayList(); def opt = Optional.empty(); return opt.orElseGet(l::toString);"));
    }

    public void testCapturingMethodReferenceMultipleLambdas() {
        assertEquals(
            "testingcdefg",
            exec(
                "String x = 'testing';"
                    + "String y = 'abcdefg';"
                    + "org.elasticsearch.painless.FeatureTestObject test = new org.elasticsearch.painless.FeatureTestObject(2,3);"
                    + "return test.twoFunctionsOfX(x::concat, y::substring);"
            )
        );
    }

    public void testCapturingMethodReferenceMultipleLambdasDefImpls() {
        assertEquals(
            "testingcdefg",
            exec(
                "def x = 'testing';"
                    + "def y = 'abcdefg';"
                    + "org.elasticsearch.painless.FeatureTestObject test = new org.elasticsearch.painless.FeatureTestObject(2,3);"
                    + "return test.twoFunctionsOfX(x::concat, y::substring);"
            )
        );
    }

    public void testCapturingMethodReferenceMultipleLambdasDefInterface() {
        assertEquals(
            "testingcdefg",
            exec(
                "String x = 'testing';"
                    + "String y = 'abcdefg';"
                    + "def test = new org.elasticsearch.painless.FeatureTestObject(2,3);"
                    + "return test.twoFunctionsOfX(x::concat, y::substring);"
            )
        );
    }

    public void testCapturingMethodReferenceMultipleLambdasDefEverywhere() {
        assertEquals(
            "testingcdefg",
            exec(
                "def x = 'testing';"
                    + "def y = 'abcdefg';"
                    + "def test = new org.elasticsearch.painless.FeatureTestObject(2,3);"
                    + "return test.twoFunctionsOfX(x::concat, y::substring);"
            )
        );
    }

    public void testOwnMethodReference() {
        assertEquals(
            2,
            exec(
                "int mycompare(int i, int j) { j - i } "
                    + "List l = new ArrayList(); l.add(2); l.add(1); l.sort(this::mycompare); return l.get(0);"
            )
        );
    }

    public void testOwnMethodReferenceDef() {
        assertEquals(
            2,
            exec(
                "int mycompare(int i, int j) { j - i } "
                    + "def l = new ArrayList(); l.add(2); l.add(1); l.sort(this::mycompare); return l.get(0);"
            )
        );
    }

    public void testInterfaceDefaultMethod() {
        assertEquals(
            "bar",
            exec("String f(BiFunction function) { function.apply('foo', 'bar') }" + "Map map = new HashMap(); f(map::getOrDefault)")
        );
    }

    public void testInterfaceDefaultMethodDef() {
        assertEquals(
            "bar",
            exec("String f(BiFunction function) { function.apply('foo', 'bar') }" + "def map = new HashMap(); f(map::getOrDefault)")
        );
    }

    public void testInterfaceStaticMethod() {
        assertEquals(
            -1,
            exec(
                "Supplier get(Supplier supplier) { return supplier }" + "Supplier s = get(Comparator::naturalOrder); s.get().compare(1, 2)"
            )
        );
    }

    public void testMethodMissing() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = [2, 1]; l.sort(Integer::bogus); return l.get(0);"); }
        );
        assertThat(e.getMessage(), containsString("function reference [Integer::bogus/2] matching [java.util.Comparator"));
    }

    public void testQualifiedMethodMissing() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = [2, 1]; l.sort(java.time.Instant::bogus); return l.get(0);", false); }
        );
        assertThat(
            e.getMessage(),
            containsString("function reference [java.time.Instant::bogus/2] matching [java.util.Comparator, compare/2")
        );
    }

    public void testClassMissing() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = [2, 1]; l.sort(Bogus::bogus); return l.get(0);", false); }
        );
        assertThat(e.getMessage(), endsWith("variable [Bogus] is not defined"));
    }

    public void testQualifiedClassMissing() {
        Exception e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = [2, 1]; l.sort(org.package.BogusClass::bogus); return l.get(0);", false); }
        );
        assertEquals("variable [org.package.BogusClass] is not defined", e.getMessage());
    }

    public void testNotFunctionalInterface() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = new ArrayList(); l.add(2); l.add(1); l.add(Integer::bogus); return l.get(0);"); }
        );
        assertThat(
            expected.getMessage(),
            containsString("cannot convert function reference [Integer::bogus] to a non-functional interface [def]")
        );
    }

    public void testIncompatible() {
        expectScriptThrows(
            ClassCastException.class,
            () -> { exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(String::startsWith); return l.get(0);"); }
        );
    }

    public void testWrongArity() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("Optional.empty().orElseGet(String::startsWith);"); }
        );
        assertThat(
            expected.getMessage(),
            containsString("function reference [String::startsWith/0] matching [java.util.function.Supplier")
        );
    }

    public void testWrongArityNotEnough() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(String::isEmpty);"); }
        );
        assertThat(expected.getMessage(), containsString("function reference [String::isEmpty/2] matching [java.util.Comparator"));
    }

    public void testWrongArityDef() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("def y = Optional.empty(); return y.orElseGet(String::startsWith);"); }
        );
        assertThat(
            expected.getMessage(),
            containsString("function reference [String::startsWith/0] matching [java.util.function.Supplier")
        );
    }

    public void testWrongArityNotEnoughDef() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> { exec("def l = new ArrayList(); l.add(2); l.add(1); l.sort(String::isEmpty);"); }
        );
        assertThat(expected.getMessage(), containsString("function reference [String::isEmpty/2] matching [java.util.Comparator"));
    }

    public void testReturnVoid() {
        Throwable expected = expectScriptThrows(
            ClassCastException.class,
            () -> { exec("StringBuilder b = new StringBuilder(); List l = [1, 2]; l.stream().mapToLong(b::setLength).sum();"); }
        );
        assertThat(expected.getMessage(), containsString("Cannot cast from [void] to [long]."));
    }

    public void testReturnVoidDef() {
        Exception expected = expectScriptThrows(
            LambdaConversionException.class,
            () -> { exec("StringBuilder b = new StringBuilder(); def l = [1, 2]; l.stream().mapToLong(b::setLength);"); }
        );
        assertThat(expected.getMessage(), containsString("lambda expects return type [long], but found return type [void]"));

        expected = expectScriptThrows(
            LambdaConversionException.class,
            () -> { exec("def b = new StringBuilder(); def l = [1, 2]; l.stream().mapToLong(b::setLength);"); }
        );
        assertThat(expected.getMessage(), containsString("lambda expects return type [long], but found return type [void]"));

        expected = expectScriptThrows(
            LambdaConversionException.class,
            () -> { exec("def b = new StringBuilder(); List l = [1, 2]; l.stream().mapToLong(b::setLength);"); }
        );
        assertThat(expected.getMessage(), containsString("lambda expects return type [long], but found return type [void]"));
    }

    public void testPrimitiveMethodReferences() {
        assertEquals(true, exec("boolean test(Function s) {return s.apply(Boolean.valueOf(true));} return test(boolean::booleanValue);"));
        assertEquals(true, exec("boolean test(Supplier s) {return s.get();} boolean b = true; return test(b::booleanValue);"));
        assertEquals((byte) 1, exec("byte test(Function s) {return s.apply(Byte.valueOf(1));} return test(byte::byteValue);"));
        assertEquals((byte) 1, exec("byte test(Supplier s) {return s.get();} byte b = 1; return test(b::byteValue);"));
        assertEquals((short) 1, exec("short test(Function s) {return s.apply(Short.valueOf(1));} return test(short::shortValue);"));
        assertEquals((short) 1, exec("short test(Supplier s) {return s.get();} short s = 1; return test(s::shortValue);"));
        assertEquals((char) 1, exec("char test(Function s) {return s.apply(Character.valueOf(1));} return test(char::charValue);"));
        assertEquals((char) 1, exec("char test(Supplier s) {return s.get();} char c = 1; return test(c::charValue);"));
        assertEquals(1, exec("int test(Function s) {return s.apply(Integer.valueOf(1));} return test(int::intValue);"));
        assertEquals(1, exec("int test(Supplier s) {return s.get();} int i = 1; return test(i::intValue);"));
        assertEquals((long) 1, exec("long test(Function s) {return s.apply(Long.valueOf(1));} return test(long::longValue);"));
        assertEquals((long) 1, exec("long test(Supplier s) {return s.get();} long l = 1; return test(l::longValue);"));
        assertEquals((float) 1, exec("float test(Function s) {return s.apply(Short.valueOf(1));} return test(float::floatValue);"));
        assertEquals((float) 1, exec("float test(Supplier s) {return s.get();} float f = 1; return test(f::floatValue);"));
        assertEquals((double) 1, exec("double test(Function s) {return s.apply(Double.valueOf(1));} return test(double::doubleValue);"));
        assertEquals((double) 1, exec("double test(Supplier s) {return s.get();} double d = 1; return test(d::doubleValue);"));
    }

    public void testObjectMethodOverride() {
        assertEquals("s", exec("CharSequence test(Supplier s) {return s.get();} CharSequence s = 's'; return test(s::toString);"));
        assertEquals("s", exec("CharSequence test(Supplier s) {return s.get();} def s = 's'; return test(s::toString);"));
        assertEquals("s", exec("CharSequence test(Function s) {return s.apply('s');} return test(CharSequence::toString);"));
    }

    public void testInvalidStaticCaptureMethodReference() {
        IllegalArgumentException expected = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("int test(Function f, String s) {return f.apply(s);} Integer i = Integer.valueOf(1); test(i::parseInt, '1')")
        );
        assertThat(expected.getMessage(), containsString("cannot use a static method as a function reference"));
    }
}
