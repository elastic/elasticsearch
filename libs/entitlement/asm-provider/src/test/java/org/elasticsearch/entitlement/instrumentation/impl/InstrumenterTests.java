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
import org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.ClassFileInfo;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.lang.reflect.AccessFlag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;
import static org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.getClassFileInfo;
import static org.hamcrest.Matchers.equalTo;

/**
 * This tests {@link InstrumenterImpl} can instrument various method signatures
 * (e.g. overloaded methods, overloaded targets, multiple instrumentation, etc.)
 */
public class InstrumenterTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(InstrumenterTests.class);

    static class TestLoader extends ClassLoader {
        final byte[] testClassBytes;
        final Class<?> testClass;

        TestLoader(String testClassName, byte[] testClassBytes) {
            super(InstrumenterTests.class.getClassLoader());
            this.testClassBytes = testClassBytes;
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
    }

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
     * The instrumented copy of this class will not extend this class, but it will implement {@link Testable}.
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
     * may be not present/may be difficult to find or not clear in the production EntitlementChecker interface.
     * <p>
     * This interface isn't subject to the {@code check$} method naming conventions because it doesn't
     * participate in the automated scan that configures the instrumenter based on the method names;
     * instead, we configure the instrumenter minimally as needed for each test.
     */
    public interface MockEntitlementChecker {
        void checkSomeStaticMethod(Class<?> callerClass, int arg);

        void checkSomeStaticMethodOverload(Class<?> callerClass, int arg, String anotherArg);

        void checkAnotherStaticMethod(Class<?> callerClass, int arg);

        void checkSomeInstanceMethod(Class<?> callerClass, Testable that, int arg, String anotherArg);

        void checkCtor(Class<?> callerClass);

        void checkCtorOverload(Class<?> callerClass, int arg);

    }

    public static class TestEntitlementCheckerHolder {
        static MockEntitlementCheckerImpl checkerInstance = new MockEntitlementCheckerImpl();

        public static MockEntitlementChecker instance() {
            return checkerInstance;
        }
    }

    public static class MockEntitlementCheckerImpl implements MockEntitlementChecker {
        /**
         * This allows us to test that the instrumentation is correct in both cases:
         * if the check throws, and if it doesn't.
         */
        volatile boolean isActive;

        int checkSomeStaticMethodIntCallCount = 0;
        int checkAnotherStaticMethodIntCallCount = 0;
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
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }

        @Override
        public void checkSomeStaticMethodOverload(Class<?> callerClass, int arg, String anotherArg) {
            checkSomeStaticMethodIntStringCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            assertEquals("abc", anotherArg);
            throwIfActive();
        }

        @Override
        public void checkAnotherStaticMethod(Class<?> callerClass, int arg) {
            checkAnotherStaticMethodIntCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }

        @Override
        public void checkSomeInstanceMethod(Class<?> callerClass, Testable that, int arg, String anotherArg) {
            checkSomeInstanceMethodCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(that.getClass().getName(), equalTo(TestClassToInstrument.class.getName()));
            assertEquals(123, arg);
            assertEquals("def", anotherArg);
            throwIfActive();
        }

        @Override
        public void checkCtor(Class<?> callerClass) {
            checkCtorCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            throwIfActive();
        }

        @Override
        public void checkCtorOverload(Class<?> callerClass, int arg) {
            checkCtorIntCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }
    }

    @Before
    public void resetInstance() {
        TestEntitlementCheckerHolder.checkerInstance = new MockEntitlementCheckerImpl();
    }

    public void testStaticMethod() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        TestLoader loader = instrumentTestClass(createInstrumenter(Map.of("checkSomeStaticMethod", targetMethod)));

        // Before checking is active, nothing should throw
        assertStaticMethod(loader, targetMethod, 123);
        // After checking is activated, everything should throw
        assertStaticMethodThrows(loader, targetMethod, 123);
    }

    public void testNotInstrumentedTwice() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        var instrumenter = createInstrumenter(Map.of("checkSomeStaticMethod", targetMethod));

        var loader1 = instrumentTestClass(instrumenter);
        byte[] instrumentedTwiceBytes = instrumenter.instrumentClass(TestClassToInstrument.class.getName(), loader1.testClassBytes, true);
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytes)));
        var loader2 = new TestLoader(TestClassToInstrument.class.getName(), instrumentedTwiceBytes);

        assertStaticMethodThrows(loader2, targetMethod, 123);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
    }

    public void testMultipleMethods() throws Exception {
        Method targetMethod1 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        Method targetMethod2 = TestClassToInstrument.class.getMethod("anotherStaticMethod", int.class);

        var instrumenter = createInstrumenter(Map.of("checkSomeStaticMethod", targetMethod1, "checkAnotherStaticMethod", targetMethod2));
        var loader = instrumentTestClass(instrumenter);

        assertStaticMethodThrows(loader, targetMethod1, 123);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
        assertStaticMethodThrows(loader, targetMethod2, 123);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkAnotherStaticMethodIntCallCount);
    }

    public void testStaticMethodOverload() throws Exception {
        Method targetMethod1 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        Method targetMethod2 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class, String.class);
        var instrumenter = createInstrumenter(
            Map.of("checkSomeStaticMethod", targetMethod1, "checkSomeStaticMethodOverload", targetMethod2)
        );
        var loader = instrumentTestClass(instrumenter);

        assertStaticMethodThrows(loader, targetMethod1, 123);
        assertStaticMethodThrows(loader, targetMethod2, 123, "abc");
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntCallCount);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeStaticMethodIntStringCallCount);
    }

    public void testInstanceMethodOverload() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someMethod", int.class, String.class);
        var instrumenter = createInstrumenter(Map.of("checkSomeInstanceMethod", targetMethod));
        var loader = instrumentTestClass(instrumenter);

        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        Testable testTargetClass = (Testable) (loader.testClass.getConstructor().newInstance());

        // This overload is not instrumented, so it will not throw
        testTargetClass.someMethod(123);
        expectThrows(TestException.class, () -> testTargetClass.someMethod(123, "def"));

        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkSomeInstanceMethodCallCount);
    }

    public void testConstructors() throws Exception {
        Constructor<?> ctor1 = TestClassToInstrument.class.getConstructor();
        Constructor<?> ctor2 = TestClassToInstrument.class.getConstructor(int.class);
        var loader = instrumentTestClass(createInstrumenter(Map.of("checkCtor", ctor1, "checkCtorOverload", ctor2)));

        assertCtorThrows(loader, ctor1);
        assertCtorThrows(loader, ctor2, 123);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkCtorCallCount);
        assertEquals(1, TestEntitlementCheckerHolder.checkerInstance.checkCtorIntCallCount);
    }

    /**
     * These tests don't replace classToInstrument in-place but instead load a separate class with the same class name.
     * This requires a configuration slightly different from what we'd use in production.
     */
    private static InstrumenterImpl createInstrumenter(Map<String, Executable> methods) throws NoSuchMethodException {
        Map<MethodKey, CheckMethod> checkMethods = new HashMap<>();
        for (var entry : methods.entrySet()) {
            checkMethods.put(getMethodKey(entry.getValue()), getCheckMethod(entry.getKey(), entry.getValue()));
        }
        String checkerClass = Type.getInternalName(MockEntitlementChecker.class);
        String handleClass = Type.getInternalName(InstrumenterTests.TestEntitlementCheckerHolder.class);
        String getCheckerClassMethodDescriptor = Type.getMethodDescriptor(Type.getObjectType(checkerClass));

        return new InstrumenterImpl(handleClass, getCheckerClassMethodDescriptor, "", checkMethods);
    }

    private static TestLoader instrumentTestClass(InstrumenterImpl instrumenter) throws IOException {
        var clazz = TestClassToInstrument.class;
        ClassFileInfo initial = getClassFileInfo(clazz);
        byte[] newBytecode = instrumenter.instrumentClass(Type.getInternalName(clazz), initial.bytecodes(), true);
        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }
        return new TestLoader(clazz.getName(), newBytecode);
    }

    private static MethodKey getMethodKey(Executable method) {
        logger.info("method key: {}", method.getName());
        String methodName = method instanceof Constructor<?> ? "<init>" : method.getName();
        return new MethodKey(
            Type.getInternalName(method.getDeclaringClass()),
            methodName,
            Stream.of(method.getParameterTypes()).map(Type::getType).map(Type::getInternalName).toList()
        );
    }

    private static CheckMethod getCheckMethod(String methodName, Executable targetMethod) throws NoSuchMethodException {
        Set<AccessFlag> flags = targetMethod.accessFlags();
        boolean isInstance = flags.contains(AccessFlag.STATIC) == false && targetMethod instanceof Method;
        int extraArgs = 1; // caller class
        if (isInstance) {
            ++extraArgs;
        }
        Class<?>[] targetParameterTypes = targetMethod.getParameterTypes();
        Class<?>[] checkParameterTypes = new Class<?>[targetParameterTypes.length + extraArgs];
        checkParameterTypes[0] = Class.class;
        if (isInstance) {
            checkParameterTypes[1] = Testable.class;
        }
        System.arraycopy(targetParameterTypes, 0, checkParameterTypes, extraArgs, targetParameterTypes.length);
        var checkMethod = MockEntitlementChecker.class.getMethod(methodName, checkParameterTypes);
        return new CheckMethod(
            Type.getInternalName(MockEntitlementChecker.class),
            checkMethod.getName(),
            Arrays.stream(Type.getArgumentTypes(checkMethod)).map(Type::getDescriptor).toList()
        );
    }

    private static void unwrapInvocationException(InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof TestException n) {
            // Sometimes we're expecting this one!
            throw n;
        } else {
            throw new AssertionError(cause);
        }
    }

    /**
     * Calling a static method of a dynamically loaded class is significantly more cumbersome
     * than calling a virtual method.
     */
    static void callStaticMethod(Method method, Object... args) {
        try {
            method.invoke(null, args);
        } catch (InvocationTargetException e) {
            unwrapInvocationException(e);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private void assertStaticMethodThrows(TestLoader loader, Method method, Object... args) {
        Method testMethod = loader.getSameMethod(method);
        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        expectThrows(TestException.class, () -> callStaticMethod(testMethod, args));
    }

    private void assertStaticMethod(TestLoader loader, Method method, Object... args) {
        Method testMethod = loader.getSameMethod(method);
        TestEntitlementCheckerHolder.checkerInstance.isActive = false;
        callStaticMethod(testMethod, args);
    }

    private void assertCtorThrows(TestLoader loader, Constructor<?> ctor, Object... args) {
        Constructor<?> testCtor = loader.getSameConstructor(ctor);
        TestEntitlementCheckerHolder.checkerInstance.isActive = true;
        expectThrows(TestException.class, () -> {
            try {
                testCtor.newInstance(args);
            } catch (InvocationTargetException e) {
                unwrapInvocationException(e);
            } catch (IllegalAccessException | InstantiationException e) {
                throw new AssertionError(e);
            }
        });
    }
}
