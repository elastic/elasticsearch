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
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckerMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;
import static org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.getClassFileInfo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

/**
 * This tests {@link InstrumenterImpl} in isolation, without a java agent.
 * It causes the methods to be instrumented, and verifies that the instrumentation is called as expected.
 * Problems with bytecode generation are easier to debug this way than in the context of an agent.
 */
@ESTestCase.WithoutSecurityManager
public class InstrumenterTests extends ESTestCase {
    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    static volatile TestEntitlementChecker testChecker;

    public static TestEntitlementChecker getTestEntitlementChecker() {
        return testChecker;
    }

    @Before
    public void initialize() {
        testChecker = new TestEntitlementChecker();
    }

    /**
     * Contains all the virtual methods from {@link ClassToInstrument},
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
    public static class ClassToInstrument implements Testable {

        public ClassToInstrument() {}

        public ClassToInstrument(int arg) {}

        public static void systemExit(int status) {
            assertEquals(123, status);
        }

        public static void anotherSystemExit(int status) {
            assertEquals(123, status);
        }

        public void someMethod(int arg) {}

        public void someMethod(int arg, String anotherArg) {}

        public static void someStaticMethod(int arg) {}

        public static void someStaticMethod(int arg, String anotherArg) {}
    }

    static final class TestException extends RuntimeException {}

    /**
     * Interface to test specific, "synthetic" cases (e.g. overloaded methods, overloaded constructors, etc.) that
     * may be not present/may be difficult to find or not clear in the production EntitlementChecker interface
     */
    public interface MockEntitlementChecker extends EntitlementChecker {
        void checkSomeStaticMethod(Class<?> clazz, int arg);

        void checkSomeStaticMethod(Class<?> clazz, int arg, String anotherArg);

        void checkSomeInstanceMethod(Class<?> clazz, Testable that, int arg, String anotherArg);

        void checkCtor(Class<?> clazz);

        void checkCtor(Class<?> clazz, int arg);
    }

    /**
     * We're not testing the permission checking logic here;
     * only that the instrumented methods are calling the correct check methods with the correct arguments.
     * This is a trivial implementation of {@link EntitlementChecker} that just always throws,
     * just to demonstrate that the injected bytecodes succeed in calling these methods.
     * It also asserts that the arguments are correct.
     */
    public static class TestEntitlementChecker implements MockEntitlementChecker {
        /**
         * This allows us to test that the instrumentation is correct in both cases:
         * if the check throws, and if it doesn't.
         */
        volatile boolean isActive;

        int checkSystemExitCallCount = 0;
        int checkSomeStaticMethodIntCallCount = 0;
        int checkSomeStaticMethodIntStringCallCount = 0;
        int checkSomeInstanceMethodCallCount = 0;

        int checkCtorCallCount = 0;
        int checkCtorIntCallCount = 0;

        @Override
        public void check$java_lang_System$exit(Class<?> callerClass, int status) {
            checkSystemExitCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, status);
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls) {}

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent) {}

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {}

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent) {}

        @Override
        public void check$java_net_URLClassLoader$(
            Class<?> callerClass,
            String name,
            URL[] urls,
            ClassLoader parent,
            URLStreamHandlerFactory factory
        ) {}

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
        public void checkSomeStaticMethod(Class<?> callerClass, int arg, String anotherArg) {
            checkSomeStaticMethodIntStringCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            assertEquals("abc", anotherArg);
            throwIfActive();
        }

        @Override
        public void checkSomeInstanceMethod(Class<?> callerClass, Testable that, int arg, String anotherArg) {
            checkSomeInstanceMethodCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(
                that.getClass().getName(),
                startsWith("org.elasticsearch.entitlement.instrumentation.impl.InstrumenterTests$ClassToInstrument")
            );
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
        public void checkCtor(Class<?> callerClass, int arg) {
            checkCtorIntCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, arg);
            throwIfActive();
        }
    }

    public void testClassIsInstrumented() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        CheckerMethod checkerMethod = getCheckerMethod(EntitlementChecker.class, "check$java_lang_System$exit", Class.class, int.class);
        Map<MethodKey, CheckerMethod> methods = Map.of(
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("systemExit", int.class)),
            checkerMethod
        );

        var instrumenter = createInstrumenter(methods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        getTestEntitlementChecker().isActive = false;

        // Before checking is active, nothing should throw
        callStaticMethod(newClass, "systemExit", 123);

        getTestEntitlementChecker().isActive = true;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
    }

    public void testClassIsNotInstrumentedTwice() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        CheckerMethod checkerMethod = getCheckerMethod(EntitlementChecker.class, "check$java_lang_System$exit", Class.class, int.class);
        Map<MethodKey, CheckerMethod> methods = Map.of(
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("systemExit", int.class)),
            checkerMethod
        );

        var instrumenter = createInstrumenter(methods);

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

        getTestEntitlementChecker().isActive = true;
        getTestEntitlementChecker().checkSystemExitCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
        assertEquals(1, getTestEntitlementChecker().checkSystemExitCallCount);
    }

    public void testClassAllMethodsAreInstrumentedFirstPass() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        CheckerMethod checkerMethod = getCheckerMethod(EntitlementChecker.class, "check$java_lang_System$exit", Class.class, int.class);
        Map<MethodKey, CheckerMethod> methods = Map.of(
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("systemExit", int.class)),
            checkerMethod,
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("anotherSystemExit", int.class)),
            checkerMethod
        );

        var instrumenter = createInstrumenter(methods);

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

        getTestEntitlementChecker().isActive = true;
        getTestEntitlementChecker().checkSystemExitCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
        assertEquals(1, getTestEntitlementChecker().checkSystemExitCallCount);

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "anotherSystemExit", 123));
        assertEquals(2, getTestEntitlementChecker().checkSystemExitCallCount);
    }

    public void testInstrumenterWorksWithOverloads() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        Map<MethodKey, CheckerMethod> methods = Map.of(
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class)),
            getCheckerMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class),
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("someStaticMethod", int.class, String.class)),
            getCheckerMethod(MockEntitlementChecker.class, "checkSomeStaticMethod", Class.class, int.class, String.class)
        );

        var instrumenter = createInstrumenter(methods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        getTestEntitlementChecker().isActive = true;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123));
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "someStaticMethod", 123, "abc"));

        assertEquals(1, getTestEntitlementChecker().checkSomeStaticMethodIntCallCount);
        assertEquals(1, getTestEntitlementChecker().checkSomeStaticMethodIntStringCallCount);
    }

    public void testInstrumenterWorksWithInstanceMethodsAndOverloads() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        Map<MethodKey, CheckerMethod> methods = Map.of(
            instrumentationService.methodKeyForTarget(classToInstrument.getMethod("someMethod", int.class, String.class)),
            getCheckerMethod(MockEntitlementChecker.class, "checkSomeInstanceMethod", Class.class, Testable.class, int.class, String.class)
        );

        var instrumenter = createInstrumenter(methods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        getTestEntitlementChecker().isActive = true;

        Testable testTargetClass = (Testable) (newClass.getConstructor().newInstance());

        // This overload is not instrumented, so it will not throw
        testTargetClass.someMethod(123);
        assertThrows(TestException.class, () -> testTargetClass.someMethod(123, "def"));

        assertEquals(1, getTestEntitlementChecker().checkSomeInstanceMethodCallCount);
    }

    public void testInstrumenterWorksWithConstructors() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        Map<MethodKey, CheckerMethod> methods = Map.of(
            new MethodKey(classToInstrument.getName().replace('.', '/'), "<init>", List.of()),
            getCheckerMethod(MockEntitlementChecker.class, "checkCtor", Class.class),
            new MethodKey(classToInstrument.getName().replace('.', '/'), "<init>", List.of("I")),
            getCheckerMethod(MockEntitlementChecker.class, "checkCtor", Class.class, int.class)
        );

        var instrumenter = createInstrumenter(methods);

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            classToInstrument.getName() + "_NEW",
            newBytecode
        );

        getTestEntitlementChecker().isActive = true;

        var ex = assertThrows(InvocationTargetException.class, () -> newClass.getConstructor().newInstance());
        assertThat(ex.getCause(), instanceOf(TestException.class));
        var ex2 = assertThrows(InvocationTargetException.class, () -> newClass.getConstructor(int.class).newInstance(123));
        assertThat(ex2.getCause(), instanceOf(TestException.class));

        assertEquals(1, getTestEntitlementChecker().checkCtorCallCount);
        assertEquals(1, getTestEntitlementChecker().checkCtorIntCallCount);
    }

    /** This test doesn't replace classToInstrument in-place but instead loads a separate
     * class with the same class name plus a "_NEW" suffix (classToInstrument.class.getName() + "_NEW")
     * that contains the instrumentation. Because of this, we need to configure the Transformer to use a
     * MethodKey and instrumentationMethod with slightly different signatures (using the common interface
     * Testable) which is not what would happen when it's run by the agent.
     */
    private InstrumenterImpl createInstrumenter(Map<MethodKey, CheckerMethod> methods) throws NoSuchMethodException {
        Method getter = InstrumenterTests.class.getMethod("getTestEntitlementChecker");
        return new InstrumenterImpl("_NEW", methods) {
            /**
             * We're not testing the bridge library here.
             * Just call our own getter instead.
             */
            @Override
            protected void pushEntitlementChecker(MethodVisitor mv) {
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    Type.getInternalName(getter.getDeclaringClass()),
                    getter.getName(),
                    Type.getMethodDescriptor(getter),
                    false
                );
            }
        };
    }

    private static CheckerMethod getCheckerMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes)
        throws NoSuchMethodException {
        var method = clazz.getMethod(methodName, parameterTypes);
        return new CheckerMethod(
            Type.getInternalName(clazz),
            method.getName(),
            Arrays.stream(Type.getArgumentTypes(method)).map(Type::getDescriptor).toList()
        );
    }

    /**
     * Calling a static method of a dynamically loaded class is significantly more cumbersome
     * than calling a virtual method.
     */
    private static void callStaticMethod(Class<?> c, String methodName, int arg) throws NoSuchMethodException, IllegalAccessException {
        try {
            c.getMethod(methodName, int.class).invoke(null, arg);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TestException n) {
                // Sometimes we're expecting this one!
                throw n;
            } else {
                throw new AssertionError(cause);
            }
        }
    }

    private static void callStaticMethod(Class<?> c, String methodName, int arg1, String arg2) throws NoSuchMethodException,
        IllegalAccessException {
        try {
            c.getMethod(methodName, int.class, String.class).invoke(null, arg1, arg2);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TestException n) {
                // Sometimes we're expecting this one!
                throw n;
            } else {
                throw new AssertionError(cause);
            }
        }
    }

    static class TestLoader extends ClassLoader {
        TestLoader(ClassLoader parent) {
            super(parent);
        }

        public Class<?> defineClassFromBytes(String name, byte[] bytes) {
            return defineClass(name, bytes, 0, bytes.length);
        }
    }

    private static final Logger logger = LogManager.getLogger(InstrumenterTests.class);
}
