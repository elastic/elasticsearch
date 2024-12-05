/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.callStaticMethod;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.getCheckMethod;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.methodKeyForConstructor;
import static org.elasticsearch.entitlement.instrumentation.impl.TestMethodUtils.methodKeyForTarget;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

/**
 * This tests {@link InstrumenterImpl} in isolation, without a java agent.
 * It causes the methods to be instrumented, and verifies that the instrumentation is called as expected.
 * Problems with bytecode generation are easier to debug this way than in the context of an agent.
 */
@ESTestCase.WithoutSecurityManager
public class InstrumenterTests extends ESTestCase {

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
    public interface Testable {}

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

        // URLClassLoader ctor
        public ClassToInstrument(URL[] urls) {}

        public static void systemExit(int status) {
            assertEquals(123, status);
        }
    }

    private static final String SAMPLE_NAME = "TEST";

    private static final URL SAMPLE_URL = createSampleUrl();

    private static URL createSampleUrl() {
        try {
            return URI.create("file:/test/example").toURL();
        } catch (MalformedURLException e) {
            return null;
        }
    }

    /**
     * We're not testing the permission checking logic here;
     * only that the instrumented methods are calling the correct check methods with the correct arguments.
     * This is a trivial implementation of {@link EntitlementChecker} that just always throws,
     * just to demonstrate that the injected bytecodes succeed in calling these methods.
     * It also asserts that the arguments are correct.
     */
    public static class TestEntitlementChecker implements EntitlementChecker {
        /**
         * This allows us to test that the instrumentation is correct in both cases:
         * if the check throws, and if it doesn't.
         */
        volatile boolean isActive;

        int checkSystemExitCallCount = 0;
        int checkURLClassLoaderCallCount = 0;

        @Override
        public void check$java_lang_System$exit(Class<?> callerClass, int status) {
            checkSystemExitCallCount++;
            assertSame(TestMethodUtils.class, callerClass);
            assertEquals(123, status);
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls) {
            checkURLClassLoaderCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(urls, arrayContaining(SAMPLE_URL));
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent) {
            checkURLClassLoaderCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(urls, arrayContaining(SAMPLE_URL));
            assertThat(parent, equalTo(ClassLoader.getSystemClassLoader()));
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
            checkURLClassLoaderCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(urls, arrayContaining(SAMPLE_URL));
            assertThat(parent, equalTo(ClassLoader.getSystemClassLoader()));
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent) {
            checkURLClassLoaderCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(name, equalTo(SAMPLE_NAME));
            assertThat(urls, arrayContaining(SAMPLE_URL));
            assertThat(parent, equalTo(ClassLoader.getSystemClassLoader()));
            throwIfActive();
        }

        @Override
        public void check$java_net_URLClassLoader$(
            Class<?> callerClass,
            String name,
            URL[] urls,
            ClassLoader parent,
            URLStreamHandlerFactory factory
        ) {
            checkURLClassLoaderCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertThat(name, equalTo(SAMPLE_NAME));
            assertThat(urls, arrayContaining(SAMPLE_URL));
            assertThat(parent, equalTo(ClassLoader.getSystemClassLoader()));
            throwIfActive();
        }

        private void throwIfActive() {
            if (isActive) {
                throw new TestException();
            }
        }
    }

    public void testSystemExitIsInstrumented() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForTarget(classToInstrument.getMethod("systemExit", int.class)),
            getCheckMethod(EntitlementChecker.class, "check$java_lang_System$exit", Class.class, int.class)
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

        getTestEntitlementChecker().isActive = false;

        // Before checking is active, nothing should throw
        callStaticMethod(newClass, "systemExit", 123);

        getTestEntitlementChecker().isActive = true;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
    }

    public void testURLClassLoaderIsInstrumented() throws Exception {
        var classToInstrument = ClassToInstrument.class;

        Map<MethodKey, CheckMethod> checkMethods = Map.of(
            methodKeyForConstructor(classToInstrument, List.of(Type.getInternalName(URL[].class))),
            getCheckMethod(EntitlementChecker.class, "check$java_net_URLClassLoader$", Class.class, URL[].class)
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

        getTestEntitlementChecker().isActive = false;

        // Before checking is active, nothing should throw
        newClass.getConstructor(URL[].class).newInstance((Object) new URL[] { SAMPLE_URL });

        getTestEntitlementChecker().isActive = true;

        // After checking is activated, everything should throw
        var exception = assertThrows(
            InvocationTargetException.class,
            () -> newClass.getConstructor(URL[].class).newInstance((Object) new URL[] { SAMPLE_URL })
        );
        assertThat(exception.getCause(), instanceOf(TestException.class));
    }

    /** This test doesn't replace classToInstrument in-place but instead loads a separate
     * class with the same class name plus a "_NEW" suffix (classToInstrument.class.getName() + "_NEW")
     * that contains the instrumentation. Because of this, we need to configure the Transformer to use a
     * MethodKey and instrumentationMethod with slightly different signatures (using the common interface
     * Testable) which is not what would happen when it's run by the agent.
     */
    private InstrumenterImpl createInstrumenter(Map<MethodKey, CheckMethod> checkMethods) throws NoSuchMethodException {
        Method getter = InstrumenterTests.class.getMethod("getTestEntitlementChecker");

        return new InstrumenterImpl(null, null, "_NEW", checkMethods) {
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

    private static final Logger logger = LogManager.getLogger(InstrumenterTests.class);
}
