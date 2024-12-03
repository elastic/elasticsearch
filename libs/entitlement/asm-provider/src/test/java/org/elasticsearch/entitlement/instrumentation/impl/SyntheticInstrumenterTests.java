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
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;
import static org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.getClassFileInfo;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.callStaticMethod;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.getCheckMethod;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.methodKeyForTarget;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * This tests {@link InstrumenterImpl} with some ad-hoc instrumented method and checker methods, to allow us to check
 * some ad-hoc test cases (e.g. overloaded methods, overloaded targets, multiple instrumentation, etc.)
 */
@ESTestCase.WithoutSecurityManager
public class SyntheticInstrumenterTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(SyntheticInstrumenterTests.class);

    /**
     * Contains all the virtual methods from {@link TestClassToInstrument},
     * allowing this test to call them on the dynamically loaded instrumented class.
     */
    public interface Testable {
        // This method is here to demonstrate Instrumenter does not get confused by overloads
        void someMethod(int arg);

        void someMethod(int arg, String anotherArg);
    }

    /**
     * This is a placeholder for real class library methods.
     * Without the java agent, we can't instrument the real methods, so we instrument this instead.
     * <p>
     * Methods of this class must have the same signature and the same static/virtual condition as the corresponding real method.
     * They should assert that the arguments came through correctly.
     * They must not throw {@link TestException}.
     */
    public static class TestClassToInstrument implements Testable {

        public TestClassToInstrument() {}

        public TestClassToInstrument(int arg) {}

        public void someMethod(int arg) {}

        public void someMethod(int arg, String anotherArg) {}

        public static void someStaticMethod(int arg) {}

        public static void someStaticMethod(int arg, String anotherArg) {}

        public static void anotherStaticMethod(int arg) {}
    }

    /**
     * Interface to test specific, "synthetic" cases (e.g. overloaded methods, overloaded constructors, etc.) that
     * may be not present/may be difficult to find or not clear in the production EntitlementChecker interface
     */
    public interface MockEntitlementChecker {
        void checkSomeStaticMethod(Class<?> clazz, int arg);

        void checkSomeStaticMethod(Class<?> clazz, int arg, String anotherArg);

        void checkSomeInstanceMethod(Class<?> clazz, Testable that, int arg, String anotherArg);

        void checkCtor(Class<?> clazz);

        void checkCtor(Class<?> clazz, int arg);
    }

    public static class TestEntitlementCheckerHolder {
        static TestEntitlementChecker checkerInstance = new TestEntitlementChecker();

        public static MockEntitlementChecker instance() {
            return checkerInstance;
        }
    }

    public static class TestEntitlementChecker implements MockEntitlementChecker {
        /**
         * This allows us to test that the instrumentation is correct in both cases:
         * if the check throws, and if it doesn't.
         */
        volatile boolean isActive;

        int checkSomeStaticMethodIntCallCount = 0;
        int checkSomeStaticMethodIntStringCallCount = 0;
        int checkSomeInstanceMethodCallCount = 0;

        int checkCtorCallCount = 0;
        int checkCtorIntCallCount = 0;

        private void throwIfActive() {
            if (isActive) {
                throw new TestException();
            }
        }

        @Override
        public void checkSomeStaticMethod(Class<?> callerClass, int arg) {
            checkSomeStaticMethodIntCallCount++;
            assertSame(TestMethodUtils.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }

        @Override
        public void checkSomeStaticMethod(Class<?> callerClass, int arg, String anotherArg) {
            checkSomeStaticMethodIntStringCallCount++;
            assertSame(TestMethodUtils.class, callerClass);
            assertEquals(123, arg);
            assertEquals("abc", anotherArg);
            throwIfActive();
        }

        @Override
        public void checkSomeInstanceMethod(Class<?> callerClass, Testable that, int arg, String anotherArg) {
            checkSomeInstanceMethodCallCount++;
            assertSame(SyntheticInstrumenterTests.class, callerClass);
            assertThat(
                that.getClass().getName(),
                startsWith("org.elasticsearch.entitlement.instrumentation.impl.SyntheticInstrumenterTests$TestClassToInstrument")
            );
            assertEquals(123, arg);
            assertEquals("def", anotherArg);
            throwIfActive();
        }

        @Override
        public void checkCtor(Class<?> callerClass) {
            checkCtorCallCount++;
            assertSame(SyntheticInstrumenterTests.class, callerClass);
            throwIfActive();
        }

        @Override
        public void checkCtor(Class<?> callerClass, int arg) {
            checkCtorIntCallCount++;
            assertSame(SyntheticInstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }
    }

    public void testClassIsInstrumented() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        CheckMethod checkMethod = getCheckMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class);
        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class)),
            checkMethod
        );

        var instrumenter = createInstrumenter(checkMethods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = false;

        // Before checking is active, nothing should throw
        callStaticMethod(newClass, "someStaticMethod", 123);

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123));
    }

    public void testClassIsNotInstrumentedTwice() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        CheckMethod checkMethod = getCheckMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class);
        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class)),
            checkMethod
        );

        var instrumenter = createInstrumenter(checkMethods);

        InstrumenterImpl.ClassFileInfo initial = getClassFileInfo(classToInstrument);
        var internalClassName = Type.getInternalName(classToInstrument);

        byte[] instrumentedBytecode = instrumenter.instrumentClass(internalClassName, initial.bytecodes());
        byte[] instrumentedTwiceBytecode = instrumenter.instrumentClass(internalClassName, instrumentedBytecode);

        logger.trace(() -> Strings.format("Bytecode after 1st instrumentation:\n%s", bytecode2text(instrumentedBytecode)));
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytecode)));

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW_NEW",
            instrumentedTwiceBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123));
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
    }

    public void testClassAllMethodsAreInstrumentedFirstPass() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        CheckMethod checkMethod = getCheckMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class);
        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class)),
            checkMethod,
            methodKeyForTarget(classToInstrument.getMethod("anotherStaticMethod", int.class)),
            checkMethod
        );

        var instrumenter = createInstrumenter(checkMethods);

        InstrumenterImpl.ClassFileInfo initial = getClassFileInfo(classToInstrument);
        var internalClassName = Type.getInternalName(classToInstrument);

        byte[] instrumentedBytecode = instrumenter.instrumentClass(internalClassName, initial.bytecodes());
        byte[] instrumentedTwiceBytecode = instrumenter.instrumentClass(internalClassName, instrumentedBytecode);

        logger.trace(() -> Strings.format("Bytecode after 1st instrumentation:\n%s", bytecode2text(instrumentedBytecode)));
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytecode)));

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW_NEW",
            instrumentedTwiceBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123));
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "anotherStaticMethod", 123));
        assertEquals(2, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
    }

    public void testInstrumenterWorksWithOverloads() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class)),
            getCheckMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class),
            methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class, String.class)),
            getCheckMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class, String.class)
        );

        var instrumenter = createInstrumenter(checkMethods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount = 0;
        TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntStringCallCount = 0;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123));
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123, "abc"));

        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntStringCallCount);
    }

    public void testInstrumenterWorksWithInstanceMethodsAndOverloads() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("someMethod", int.class, String.class)),
            getCheckMethod(MockEntitlementChecker.class, "checkSomeInstanceMethod", Class.class, Testable.class, int.class, String.class)
        );

        var instrumenter = createInstrumenter(checkMethods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        TestEntitlementCheckerHolder.checkerInstance.checkSomeInstanceMethodCallCount = 0;

        Testable testTargetClass = (Testable) (newClass.getConstructor().newInstance());

        // This overload is not instrumented, so it will not throw
        testTargetClass.someMethod(123);
        assertThrows(TestException.class, () -> testTargetClass.someMethod(123, "def"));

        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeInstanceMethodCallCount);
    }

    public void testInstrumenterWorksWithConstructors() throws Exception {
        var classToInstrument = TestClassToInstrument.class;

        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            new MethodKey(classToInstrument.getName().replace('.', '/'), "<init>", List.of()),
            getCheckMethod(MockEntitlementChecker.class, "checkCtor", Class.class),
            new MethodKey(classToInstrument.getName().replace('.', '/'), "<init>", List.of("I")),
            getCheckMethod(MockEntitlementChecker.class, "checkCtor", Class.class, int.class)
        );

        var instrumenter = createInstrumenter(checkMethods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;

        var ex = assertThrows(InvocationTargetException.class, () -> newClass.getConstructor().newInstance());
        assertThat(ex.getCause(), instanceOf(TestException.class));
        var ex2 = assertThrows(InvocationTargetException.class, () -> newClass.getConstructor(int.class).newInstance(123));
        assertThat(ex2.getCause(), instanceOf(TestException.class));

        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkCtorCallCount);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkCtorIntCallCount);
    }

    /** This test doesn't replace classToInstrument in-place but instead loads a separate
     * class with the same class name plus a "_NEW" suffix (classToInstrument.class.getName() + "_NEW")
     * that contains the instrumentation. Because of this, we need to configure the Transformer to use a
     * MethodKey and instrumentationMethod with slightly different signatures (using the common interface
     * Testable) which is not what would happen when it's run by the agent.
     */
    private InstrumenterImpl createInstrumenter(Map<MethodKey, CheckMethod> checkMethods) {
        String checkerClass = Type.getInternalName(SyntheticInstrumenterTests.MockEntitlementChecker.class);
        String handleClass = Type.getInternalName(SyntheticInstrumenterTests.TestEntitlementCheckerHolder.class);
        String getCheckerClassMethodDescriptor = Type.getMethodDescriptor(Type.getObjectType(checkerClass));

        return new InstrumenterImpl(handleClass, getCheckerClassMethodDescriptor, "_NEW", checkMethods);
    }
}
