/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.common.Strings;
import org.elasticsearch.entitlement.bridge.InstrumentationRegistry;
import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.function.Call0;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationRegistryImpl;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;

/**
 * This tests {@link InstrumenterImpl} can instrument various method signatures
 * (e.g. overloaded methods, overloaded targets, multiple instrumentation, etc.)
 */
public class InstrumenterTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(InstrumenterTests.class);

    private static InternalInstrumentationRegistry registry;

    /**
     * Called by instrumentation added by {@link InstrumenterImpl} to retrieve the instrumentation registry.
     */
    public static InstrumentationRegistry instance() {
        return registry;
    }

    /**
     * Contains all the virtual methods from {@link TestClassToInstrument},
     * allowing this test to call them on the dynamically loaded instrumented class.
     */
    public interface Testable {
        // This method is here to demonstrate Instrumenter does not get confused by overloads
        void someMethod(int arg);

        void someMethod(int arg, String anotherArg);

        boolean someMethodReturningFalse();

        void someMethodWithSideEffects(AtomicInteger counter);

        String someMethodReturningString(String arg);

        int someMethodReturningInt(int arg);

        boolean someMethodReturningBoolean(boolean arg);

        byte someMethodReturningByte(byte arg);

        short someMethodReturningShort(short arg);

        char someMethodReturningChar(char arg);

        long someMethodReturningLong(long arg);

        float someMethodReturningFloat(float arg);

        double someMethodReturningDouble(double arg);
    }

    /**
     * This is a placeholder for real class library methods.
     * Without the java agent, we can't instrument the real methods, so we instrument this instead.
     * <p>
     * The instrumented copy of this class will not extend this class, but it will implement {@link Testable}.
     */
    public static class TestClassToInstrument implements Testable {

        public TestClassToInstrument() {}

        public TestClassToInstrument(int arg) {}

        public void someMethod(int arg) {}

        public void someMethod(int arg, String anotherArg) {}

        @Override
        public void someMethodWithSideEffects(AtomicInteger counter) {
            counter.incrementAndGet();
        }

        @Override
        public boolean someMethodReturningFalse() {
            return false;
        }

        @Override
        public String someMethodReturningString(String arg) {
            return "original";
        }

        @Override
        public int someMethodReturningInt(int arg) {
            return -1;
        }

        @Override
        public boolean someMethodReturningBoolean(boolean arg) {
            return false;
        }

        @Override
        public byte someMethodReturningByte(byte arg) {
            return -1;
        }

        @Override
        public short someMethodReturningShort(short arg) {
            return -1;
        }

        @Override
        public char someMethodReturningChar(char arg) {
            return 'X';
        }

        @Override
        public long someMethodReturningLong(long arg) {
            return -1L;
        }

        @Override
        public float someMethodReturningFloat(float arg) {
            return -1.0f;
        }

        @Override
        public double someMethodReturningDouble(double arg) {
            return -1.0;
        }

        public static void someStaticMethod(int arg) {}

        public static void someStaticMethod(int arg, String anotherArg) {}

        public static void anotherStaticMethod(int arg) {}

        public static boolean someStaticMethodReturningFalse() {
            return false;
        }

        public static void someStaticMethodWithSideEffects(AtomicInteger counter) {
            counter.incrementAndGet();
        }

        public static String someStaticMethodReturningString(String arg) {
            return "original";
        }

        public static int someStaticMethodReturningInt(int arg) {
            return -1;
        }

        public static boolean someStaticMethodReturningBoolean(boolean arg) {
            return false;
        }

        public static byte someStaticMethodReturningByte(byte arg) {
            return -1;
        }

        public static short someStaticMethodReturningShort(short arg) {
            return -1;
        }

        public static char someStaticMethodReturningChar(char arg) {
            return 'X';
        }

        public static long someStaticMethodReturningLong(long arg) {
            return -1L;
        }

        public static float someStaticMethodReturningFloat(float arg) {
            return -1.0f;
        }

        public static double someStaticMethodReturningDouble(double arg) {
            return -1.0;
        }
    }

    @Before
    public void resetRegistry() {
        registry = null;
    }

    public void testStaticMethod() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethod", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(verifier)
                .elseThrowNotEntitled()
        );

        // Before checking is active, nothing should throw
        verifier.setActive(false);
        verifier.assertStaticMethod(loader, 123);

        // After checking is activated, everything should throw
        verifier.setActive(true);
        verifier.assertStaticMethodThrows(loader, 123);
        verifier.assertCalled(2);
    }

    public void testNotInstrumentedTwice() throws Exception {
        var targetMethod = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        var verifier = new TestVerifier(targetMethod);
        var loader1 = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(verifier)
                .elseThrowNotEntitled()
        );

        var instrumenter = loader1.getInstrumenter();
        var instrumentedTwiceBytes = instrumenter.instrumentClass(TestClassToInstrument.class.getName(), loader1.testClassBytes, true);
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytes)));
        var loader2 = new TestLoader(TestClassToInstrument.class.getName(), instrumentedTwiceBytes, instrumenter);

        verifier.assertStaticMethodThrows(loader2, 123);
        verifier.assertCalled(1);

    }

    public void testMultipleMethods() throws Exception {
        var verifier1 = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethod", int.class));
        var verifier2 = new TestVerifier(TestClassToInstrument.class.getMethod("anotherStaticMethod", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(verifier1)
                .elseThrowNotEntitled()
                .callingVoidStatic(TestClassToInstrument::anotherStaticMethod, Integer.class)
                .enforce(verifier2)
                .elseThrowNotEntitled()
        );

        verifier1.assertStaticMethodThrows(loader, 123);
        verifier2.assertStaticMethodThrows(loader, 123);
        verifier1.assertCalled(1);
        verifier2.assertCalled(1);

    }

    public void testStaticMethodOverload() throws Exception {
        var verifier1 = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethod", int.class));
        var verifier2 = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethod", int.class, String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(verifier1)
                .elseThrowNotEntitled()
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class, String.class)
                .enforce(verifier2)
                .elseThrowNotEntitled()
        );

        verifier1.assertStaticMethodThrows(loader, 123);
        verifier2.assertStaticMethodThrows(loader, 123, "abc");
        verifier1.assertCalled(1);
        verifier2.assertCalled(1);
    }

    public void testInstanceMethodOverload() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethod", int.class, String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoid(TestClassToInstrument::someMethod, Integer.class, String.class)
                .enforce(verifier)
                .elseThrowNotEntitled()
        );

        var instance = loader.newInstance();

        // This overload is not instrumented, so it will not throw
        instance.someMethod(123);
        expectThrows(NotEntitledException.class, () -> instance.someMethod(123, "def"));
        verifier.assertCalled(1);
    }

    public void testStaticMethodReturnBoolean() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningFalse"));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningFalse)
                .enforce(verifier)
                .elseReturn(true)
        );

        // Before checking is active, should return normal value
        verifier.setActive(false);
        assertEquals(false, verifier.assertStaticMethod(loader));

        // After checking is activated, should return overridden value
        verifier.setActive(true);
        assertEquals(true, verifier.assertStaticMethod(loader));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnBoolean() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningFalse"));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningFalse)
                .enforce(verifier)
                .elseReturn(true)
        );

        var instance = loader.newInstance();

        // Before checking is active, should return normal value
        verifier.setActive(false);
        assertEquals(false, instance.someMethodReturningFalse());

        // After checking is activated, should return overridden value
        verifier.setActive(true);
        assertEquals(true, instance.someMethodReturningFalse());

        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnString() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningString", String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningString, String.class)
                .enforce(verifier)
                .elseReturn("overridden")
        );

        verifier.setActive(false);
        assertEquals("original", verifier.assertStaticMethod(loader, "test"));

        verifier.setActive(true);
        assertEquals("overridden", verifier.assertStaticMethod(loader, "test"));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnString() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningString", String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningString, String.class)
                .enforce(verifier)
                .elseReturn("overridden")
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals("original", instance.someMethodReturningString("test"));

        verifier.setActive(true);
        assertEquals("overridden", instance.someMethodReturningString("test"));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnInt() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningInt", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningInt, Integer.class)
                .enforce(verifier)
                .elseReturn(99)
        );

        verifier.setActive(false);
        assertEquals(-1, verifier.assertStaticMethod(loader, 42));

        verifier.setActive(true);
        assertEquals(99, verifier.assertStaticMethod(loader, 42));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnInt() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningInt", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningInt, Integer.class)
                .enforce(verifier)
                .elseReturn(99)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1, instance.someMethodReturningInt(42));

        verifier.setActive(true);
        assertEquals(99, instance.someMethodReturningInt(42));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnLong() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningLong", long.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningLong, Long.class)
                .enforce(verifier)
                .elseReturn(99L)
        );

        verifier.setActive(false);
        assertEquals(-1L, verifier.assertStaticMethod(loader, 42L));

        verifier.setActive(true);
        assertEquals(99L, verifier.assertStaticMethod(loader, 42L));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnLong() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningLong", long.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningLong, Long.class)
                .enforce(verifier)
                .elseReturn(99L)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1L, instance.someMethodReturningLong(42L));

        verifier.setActive(true);
        assertEquals(99L, instance.someMethodReturningLong(42L));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnFloat() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningFloat", float.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningFloat, Float.class)
                .enforce(verifier)
                .elseReturn(99.5f)
        );

        verifier.setActive(false);
        assertEquals(-1.0f, (float) verifier.assertStaticMethod(loader, 42.5f), 0.0f);

        verifier.setActive(true);
        assertEquals(99.5f, (float) verifier.assertStaticMethod(loader, 42.5f), 0.0f);
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnFloat() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningFloat", float.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningFloat, Float.class)
                .enforce(verifier)
                .elseReturn(99.5f)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1.0f, instance.someMethodReturningFloat(42.5f), 0.0f);

        verifier.setActive(true);
        assertEquals(99.5f, instance.someMethodReturningFloat(42.5f), 0.0f);
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnDouble() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningDouble", double.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningDouble, Double.class)
                .enforce(verifier)
                .elseReturn(99.5)
        );

        verifier.setActive(false);
        assertEquals(-1.0, (double) verifier.assertStaticMethod(loader, 42.5), 0.0);

        verifier.setActive(true);
        assertEquals(99.5, (double) verifier.assertStaticMethod(loader, 42.5), 0.0);
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnDouble() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningDouble", double.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningDouble, Double.class)
                .enforce(verifier)
                .elseReturn(99.5)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1.0, instance.someMethodReturningDouble(42.5), 0.0);

        verifier.setActive(true);
        assertEquals(99.5, instance.someMethodReturningDouble(42.5), 0.0);
        verifier.assertCalled(2);
    }

    public void testConstructors() throws Exception {
        var verifier1 = new TestVerifier(TestClassToInstrument.class.getConstructor());
        var verifier2 = new TestVerifier(TestClassToInstrument.class.getConstructor(int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::new)
                .enforce(verifier1)
                .elseThrowNotEntitled()
                .callingStatic(TestClassToInstrument::new, Integer.class)
                .enforce(verifier2)
                .elseThrowNotEntitled()
        );

        verifier1.assertCtorThrows(loader);
        verifier2.assertCtorThrows(loader, 123);
        verifier1.assertCalled(1);
        verifier2.assertCalled(1);
    }

    public void testStaticMethodReturnEarly() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodWithSideEffects", AtomicInteger.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethodWithSideEffects, AtomicInteger.class)
                .enforce(verifier)
                .elseReturnEarly()
        );

        var counter = new AtomicInteger();
        // Before checking is active, method should run as normal
        verifier.setActive(false);
        verifier.assertStaticMethod(loader, counter);
        assertEquals(1, counter.get());

        // After checking is active, method should be a noop
        verifier.setActive(true);
        verifier.assertStaticMethod(loader, counter);
        assertEquals(1, counter.get());
    }

    public void testInstanceMethodReturnEarly() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodWithSideEffects", AtomicInteger.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoid(TestClassToInstrument::someMethodWithSideEffects, AtomicInteger.class)
                .enforce(verifier)
                .elseReturnEarly()
        );

        var instance = loader.newInstance();
        var counter = new AtomicInteger();
        // Before checking is active, method should run as normal
        verifier.setActive(false);
        instance.someMethodWithSideEffects(counter);
        assertEquals(1, counter.get());

        // After checking is active, method should be a noop
        verifier.setActive(true);
        instance.someMethodWithSideEffects(counter);
        assertEquals(1, counter.get());
    }

    public void testStaticMethodReturnArgString() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningString", String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningString, String.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        // Before checking is active, should return original value
        verifier.setActive(false);
        assertEquals("original", verifier.assertStaticMethod(loader, "test"));

        // After checking is activated, should return the argument
        verifier.setActive(true);
        assertEquals("test", verifier.assertStaticMethod(loader, "test"));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgInt() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningInt", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningInt, Integer.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        // Before checking is active, should return original value
        verifier.setActive(false);
        assertEquals(-1, verifier.assertStaticMethod(loader, 42));

        // After checking is activated, should return the argument
        verifier.setActive(true);
        assertEquals(42, verifier.assertStaticMethod(loader, 42));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgString() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningString", String.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningString, String.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        // Before checking is active, should return original value
        verifier.setActive(false);
        assertEquals("original", instance.someMethodReturningString("test"));

        // After checking is activated, should return the argument
        verifier.setActive(true);
        assertEquals("test", instance.someMethodReturningString("test"));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgInt() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningInt", int.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningInt, Integer.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        // Before checking is active, should return original value
        verifier.setActive(false);
        assertEquals(-1, instance.someMethodReturningInt(42));

        // After checking is activated, should return the argument
        verifier.setActive(true);
        assertEquals(42, instance.someMethodReturningInt(42));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgBoolean() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningBoolean", boolean.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningBoolean, Boolean.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals(false, verifier.assertStaticMethod(loader, true));

        verifier.setActive(true);
        assertEquals(true, verifier.assertStaticMethod(loader, true));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgBoolean() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningBoolean", boolean.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningBoolean, Boolean.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(false, instance.someMethodReturningBoolean(true));

        verifier.setActive(true);
        assertEquals(true, instance.someMethodReturningBoolean(true));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgByte() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningByte", byte.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningByte, Byte.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals((byte) -1, verifier.assertStaticMethod(loader, (byte) 42));

        verifier.setActive(true);
        assertEquals((byte) 42, verifier.assertStaticMethod(loader, (byte) 42));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgByte() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningByte", byte.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningByte, Byte.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals((byte) -1, instance.someMethodReturningByte((byte) 42));

        verifier.setActive(true);
        assertEquals((byte) 42, instance.someMethodReturningByte((byte) 42));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgShort() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningShort", short.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningShort, Short.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals((short) -1, verifier.assertStaticMethod(loader, (short) 42));

        verifier.setActive(true);
        assertEquals((short) 42, verifier.assertStaticMethod(loader, (short) 42));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgShort() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningShort", short.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningShort, Short.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals((short) -1, instance.someMethodReturningShort((short) 42));

        verifier.setActive(true);
        assertEquals((short) 42, instance.someMethodReturningShort((short) 42));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgChar() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningChar", char.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningChar, Character.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals('X', verifier.assertStaticMethod(loader, 'A'));

        verifier.setActive(true);
        assertEquals('A', verifier.assertStaticMethod(loader, 'A'));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgChar() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningChar", char.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningChar, Character.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals('X', instance.someMethodReturningChar('A'));

        verifier.setActive(true);
        assertEquals('A', instance.someMethodReturningChar('A'));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgLong() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningLong", long.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningLong, Long.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals(-1L, verifier.assertStaticMethod(loader, 42L));

        verifier.setActive(true);
        assertEquals(42L, verifier.assertStaticMethod(loader, 42L));
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgLong() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningLong", long.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningLong, Long.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1L, instance.someMethodReturningLong(42L));

        verifier.setActive(true);
        assertEquals(42L, instance.someMethodReturningLong(42L));
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgFloat() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningFloat", float.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningFloat, Float.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals(-1.0f, (float) verifier.assertStaticMethod(loader, 42.5f), 0.0f);

        verifier.setActive(true);
        assertEquals(42.5f, (float) verifier.assertStaticMethod(loader, 42.5f), 0.0f);
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgFloat() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningFloat", float.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningFloat, Float.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1.0f, instance.someMethodReturningFloat(42.5f), 0.0f);

        verifier.setActive(true);
        assertEquals(42.5f, instance.someMethodReturningFloat(42.5f), 0.0f);
        verifier.assertCalled(2);
    }

    public void testStaticMethodReturnArgDouble() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someStaticMethodReturningDouble", double.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::someStaticMethodReturningDouble, Double.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        verifier.setActive(false);
        assertEquals(-1.0, (double) verifier.assertStaticMethod(loader, 42.5), 0.0);

        verifier.setActive(true);
        assertEquals(42.5, (double) verifier.assertStaticMethod(loader, 42.5), 0.0);
        verifier.assertCalled(2);
    }

    public void testInstanceMethodReturnArgDouble() throws Exception {
        var verifier = new TestVerifier(TestClassToInstrument.class.getMethod("someMethodReturningDouble", double.class));
        var loader = buildInstrumentation(
            builder -> builder.on(TestClassToInstrument.class)
                .calling(TestClassToInstrument::someMethodReturningDouble, Double.class)
                .enforce(verifier)
                .elseReturnArg(0)
        );

        var instance = loader.newInstance();

        verifier.setActive(false);
        assertEquals(-1.0, instance.someMethodReturningDouble(42.5), 0.0);

        verifier.setActive(true);
        assertEquals(42.5, instance.someMethodReturningDouble(42.5), 0.0);
        verifier.assertCalled(2);
    }

    private static TestLoader buildInstrumentation(Consumer<EntitlementRulesBuilder> builderConsumer) throws Exception {
        InstrumentationRegistryImpl registry = new InstrumentationRegistryImpl(null);
        EntitlementRulesBuilder rulesBuilder = new EntitlementRulesBuilder(registry);
        builderConsumer.accept(rulesBuilder);
        InstrumenterTests.registry = registry;
        InstrumenterImpl instrumenter = new InstrumenterImpl(
            Type.getType(InstrumenterTests.class).getInternalName(),
            Type.getMethodDescriptor(Type.getType(InstrumentationRegistry.class)),
            registry.getInstrumentedMethods()
        );

        return instrumentTestClass(instrumenter);
    }

    private static TestLoader instrumentTestClass(InstrumenterImpl instrumenter) throws IOException {
        var clazz = TestClassToInstrument.class;
        byte[] newBytecode = instrumenter.instrumentClass(Type.getInternalName(clazz), getClassBytecode(clazz), true);
        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }
        return new TestLoader(clazz.getName(), newBytecode, instrumenter);
    }

    private static byte[] getClassBytecode(Class<?> clazz) throws IOException {
        String internalName = Type.getInternalName(clazz);
        String fileName = "/" + internalName + ".class";
        byte[] originalBytecodes;
        try (InputStream classStream = clazz.getResourceAsStream(fileName)) {
            if (classStream == null) {
                throw new IllegalStateException("Classfile not found in jar: " + fileName);
            }
            originalBytecodes = classStream.readAllBytes();
        }
        return originalBytecodes;
    }

    private static class TestLoader extends ClassLoader {
        final byte[] testClassBytes;
        final Class<?> testClass;
        final Instrumenter instrumenter;

        TestLoader(String testClassName, byte[] testClassBytes, Instrumenter instrumenter) {
            super(InstrumenterTests.class.getClassLoader());
            this.testClassBytes = testClassBytes;
            this.instrumenter = instrumenter;
            this.testClass = defineClass(testClassName, testClassBytes, 0, testClassBytes.length);
        }

        Method getSameMethod(Method method) {
            try {
                return testClass.getMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        Constructor<?> getSameConstructor(Constructor<?> ctor) {
            try {
                return testClass.getConstructor(ctor.getParameterTypes());
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        Testable newInstance() throws Exception {
            return (Testable) (testClass.getConstructor().newInstance());
        }

        Instrumenter getInstrumenter() {
            return instrumenter;
        }
    }

    private static class TestVerifier implements Call0<CheckMethod> {
        private boolean active = true;
        private Object[] calledArgs = null;
        private final AtomicInteger callCount = new AtomicInteger(0);
        private final Executable executable;

        private TestVerifier(Executable executable) {
            this.executable = executable;
        }

        @Override
        public CheckMethod call() throws Exception {
            return asVarargCall().call();
        }

        @Override
        public VarargCall<CheckMethod> asVarargCall() {
            return args -> (callerClass, policyChecker) -> {
                this.callCount.incrementAndGet();
                this.calledArgs = args;
                throwIfActive();
            };
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public void assertCalled(int count) {
            assertEquals(count, callCount.get());
        }

        public void assertCalledWithArgs(Object... args) {
            assertArrayEquals(args, calledArgs);
        }

        public Object assertStaticMethod(TestLoader loader, Object... args) {
            if (executable instanceof Method method) {
                Method testMethod = loader.getSameMethod(method);
                Object result = callStaticMethod(testMethod, args);
                assertCalledWithArgs(args);
                return result;
            } else {
                throw new IllegalStateException(
                    "Cannot call 'assertStaticMethod' as instrumented function is of type " + executable.getClass().getSimpleName()
                );
            }
        }

        public void assertStaticMethodThrows(TestLoader loader, Object... args) {
            if (executable instanceof Method method) {
                Method testMethod = loader.getSameMethod(method);
                assertThrows(NotEntitledException.class, () -> callStaticMethod(testMethod, args));
                assertCalledWithArgs(args);
            } else {
                throw new IllegalStateException(
                    "Cannot call 'assertStaticMethodThrows' as instrumented function is of type " + executable.getClass().getSimpleName()
                );
            }
        }

        public void assertCtorThrows(TestLoader loader, Object... args) {
            if (executable instanceof Constructor<?> ctor) {
                Constructor<?> testConstructor = loader.getSameConstructor(ctor);
                assertThrows(NotEntitledException.class, () -> {
                    try {
                        testConstructor.newInstance(args);
                    } catch (InvocationTargetException e) {
                        unwrapInvocationException(e);
                    } catch (IllegalAccessException | InstantiationException e) {
                        throw new AssertionError(e);
                    }
                });
                assertCalledWithArgs(args);
            } else {
                throw new IllegalStateException(
                    "Cannot call 'assertCtorThrows' as instrumented function is of type " + executable.getClass().getSimpleName()
                );
            }

        }

        static Object callStaticMethod(Method method, Object... args) {
            Object result = null;
            try {
                result = method.invoke(null, args);
            } catch (InvocationTargetException e) {
                unwrapInvocationException(e);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            }

            return result;
        }

        private static void unwrapInvocationException(InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NotEntitledException n) {
                // Sometimes we're expecting this one!
                throw n;
            } else {
                throw new AssertionError(cause);
            }
        }

        private void throwIfActive() {
            if (active) {
                throw new NotEntitledException("not entitled");
            }
        }
    }
}
