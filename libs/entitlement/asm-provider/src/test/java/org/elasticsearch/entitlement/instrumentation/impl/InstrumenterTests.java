/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.entitlement.bridge.InstrumentationRegistry;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.ClassFileInfo;
import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.function.Call1;
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Consumer;

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

        void checkSomeInstanceMethod(Class<?> callerClass, Object that, int arg, String anotherArg);

        void checkCtor(Class<?> callerClass);

        void checkCtorOverload(Class<?> callerClass, int arg);

    }

    private static MockEntitlementCheckerImpl checker = new MockEntitlementCheckerImpl();

    public static class TestInstrumentationRegistryHandle {
        static InternalInstrumentationRegistry registry;

        public static InstrumentationRegistry instance() {
            return registry;
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
        public void checkSomeInstanceMethod(Class<?> callerClass, Object that, int arg, String anotherArg) {
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
        checker = new MockEntitlementCheckerImpl();
    }

    public void testStaticMethod() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkSomeStaticMethod(clazz, 123))
                .elseThrowNotEntitled(),
            (loader, instrumenter) -> {
                // Before checking is active, nothing should throw
                assertStaticMethod(loader, targetMethod, 123);
                // After checking is activated, everything should throw
                assertStaticMethodThrows(loader, targetMethod, 123);
            }
        );
    }

    public void testNotInstrumentedTwice() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkSomeStaticMethod(clazz, 123))
                .elseThrowNotEntitled(),
            (loader1, instrumenter) -> {
                byte[] instrumentedTwiceBytes = instrumenter.instrumentClass(
                    TestClassToInstrument.class.getName(),
                    loader1.testClassBytes,
                    true
                );
                logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytes)));
                var loader2 = new TestLoader(TestClassToInstrument.class.getName(), instrumentedTwiceBytes);

                assertStaticMethodThrows(loader2, targetMethod, 123);
                assertEquals(1, checker.checkSomeStaticMethodIntCallCount);
            }
        );
    }

    public void testMultipleMethods() throws Exception {
        Method targetMethod1 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        Method targetMethod2 = TestClassToInstrument.class.getMethod("anotherStaticMethod", int.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkSomeStaticMethod(clazz, 123))
                .elseThrowNotEntitled()
                .callingVoidStatic(TestClassToInstrument::anotherStaticMethod, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkAnotherStaticMethod(clazz, 123))
                .elseThrowNotEntitled(),
            (loader, instrumenter) -> {
                assertStaticMethodThrows(loader, targetMethod1, 123);
                assertEquals(1, checker.checkSomeStaticMethodIntCallCount);
                assertStaticMethodThrows(loader, targetMethod2, 123);
                assertEquals(1, checker.checkAnotherStaticMethodIntCallCount);
            }
        );
    }

    public void testStaticMethodOverload() throws Exception {
        Method targetMethod1 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class);
        Method targetMethod2 = TestClassToInstrument.class.getMethod("someStaticMethod", int.class, String.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkSomeStaticMethod(clazz, 123))
                .elseThrowNotEntitled()
                .callingVoidStatic(TestClassToInstrument::someStaticMethod, Integer.class, String.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkSomeStaticMethodOverload(clazz, 123, "abc"))
                .elseThrowNotEntitled(),
            (loader, instrumenter) -> {
                assertStaticMethodThrows(loader, targetMethod1, 123);
                assertStaticMethodThrows(loader, targetMethod2, 123, "abc");
                assertEquals(1, checker.checkSomeStaticMethodIntCallCount);
                assertEquals(1, checker.checkSomeStaticMethodIntStringCallCount);
            }
        );
    }

    public void testInstanceMethodOverload() throws Exception {
        Method targetMethod = TestClassToInstrument.class.getMethod("someMethod", int.class, String.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingVoid(TestClassToInstrument::someMethod, Integer.class, String.class)
                // We use an anonymous class here instead of a lambda so we can override `asVarargCall` to remove the explicit cast.
                // Unlike in the real agent, we aren't actually replacing these classes, so we end up having a ClassCastException
                // due to the type in the lambda, and the instrumented type mismatch.
                .enforce(new Call1<>() {
                    @Override
                    public CheckMethod call(TestClassToInstrument self) throws Exception {
                        return asVarargCall().call(self);
                    }

                    @Override
                    public VarargCall<CheckMethod> asVarargCall() {
                        return args -> (clazz, policyChecker) -> checker.checkSomeInstanceMethod(clazz, args[0], 123, "def");
                    }
                })
                .elseThrowNotEntitled(),
            (loader, instrumenter) -> {
                checker.isActive = true;
                Testable testTargetClass = (Testable) (loader.testClass.getConstructor().newInstance());

                // This overload is not instrumented, so it will not throw
                testTargetClass.someMethod(123);
                expectThrows(TestException.class, () -> testTargetClass.someMethod(123, "def"));

                assertEquals(1, checker.checkSomeInstanceMethodCallCount);
            }
        );
    }

    public void testConstructors() throws Exception {
        Constructor<?> ctor1 = TestClassToInstrument.class.getConstructor();
        Constructor<?> ctor2 = TestClassToInstrument.class.getConstructor(int.class);
        withInstrumentedClass(
            builder -> builder.on(TestClassToInstrument.class)
                .callingStatic(TestClassToInstrument::new)
                .enforce(() -> (clazz, policyChecker) -> checker.checkCtor(clazz))
                .elseThrowNotEntitled()
                .callingStatic(TestClassToInstrument::new, Integer.class)
                .enforce(() -> (clazz, policyChecker) -> checker.checkCtorOverload(clazz, 123))
                .elseThrowNotEntitled(),
            (loader, instrumenter) -> {
                assertCtorThrows(loader, ctor1);
                assertCtorThrows(loader, ctor2, 123);
                assertEquals(1, checker.checkCtorCallCount);
                assertEquals(1, checker.checkCtorIntCallCount);
            }
        );
    }

    public static void withInstrumentedClass(
        Consumer<EntitlementRulesBuilder> builderConsumer,
        CheckedBiConsumer<TestLoader, Instrumenter, Exception> assertions
    ) throws Exception {
        InstrumentationRegistryImpl registry = new InstrumentationRegistryImpl(null);
        EntitlementRulesBuilder rulesBuilder = new EntitlementRulesBuilder(registry);
        builderConsumer.accept(rulesBuilder);
        TestInstrumentationRegistryHandle.registry = registry;
        InstrumenterImpl instrumenter = new InstrumenterImpl(
            Type.getType(TestInstrumentationRegistryHandle.class).getInternalName(),
            Type.getMethodDescriptor(Type.getType(InstrumentationRegistry.class)),
            "",
            registry.getInstrumentedMethods()
        );
        TestLoader testLoader = instrumentTestClass(instrumenter);
        assertions.accept(testLoader, instrumenter);
        TestInstrumentationRegistryHandle.registry = null;
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
        checker.isActive = true;
        expectThrows(TestException.class, () -> callStaticMethod(testMethod, args));
    }

    private void assertStaticMethod(TestLoader loader, Method method, Object... args) {
        Method testMethod = loader.getSameMethod(method);
        checker.isActive = false;
        callStaticMethod(testMethod, args);
    }

    private void assertCtorThrows(TestLoader loader, Constructor<?> ctor, Object... args) {
        Constructor<?> testCtor = loader.getSameConstructor(ctor);
        checker.isActive = true;
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
