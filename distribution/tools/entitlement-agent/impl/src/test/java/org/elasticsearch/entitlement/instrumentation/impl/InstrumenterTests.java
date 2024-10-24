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
import org.elasticsearch.entitlement.api.EntitlementChecks;
import org.elasticsearch.entitlement.api.EntitlementProvider;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.entitlement.instrumentation.impl.ASMUtils.bytecode2text;
import static org.elasticsearch.entitlement.instrumentation.impl.InstrumenterImpl.getClassFileInfo;
import static org.hamcrest.Matchers.is;

/**
 * This tests {@link InstrumenterImpl} in isolation, without a java agent.
 * It causes the methods to be instrumented, and verifies that the instrumentation is called as expected.
 * Problems with bytecode generation are easier to debug this way than in the context of an agent.
 */
@ESTestCase.WithoutSecurityManager
public class InstrumenterTests extends ESTestCase {
    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    private static TestEntitlementManager getTestChecks() {
        return (TestEntitlementManager) EntitlementProvider.checks();
    }

    @Before
    public void initialize() {
        getTestChecks().isActive = false;
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
        public static void systemExit(int status) {
            assertEquals(123, status);
        }

        public static void anotherSystemExit(int status) {
            assertEquals(123, status);
        }
    }

    static final class TestException extends RuntimeException {}

    /**
     * We're not testing the permission checking logic here.
     * This is a trivial implementation of {@link EntitlementChecks} that just always throws,
     * just to demonstrate that the injected bytecodes succeed in calling these methods.
     */
    public static class TestEntitlementManager implements EntitlementChecks {
        /**
         * This allows us to test that the instrumentation is correct in both cases:
         * if the check throws, and if it doesn't.
         */
        volatile boolean isActive;

        int checkSystemExitCallCount = 0;

        @Override
        public void checkSystemExit(Class<?> callerClass, int status) {
            checkSystemExitCallCount++;
            assertSame(InstrumenterTests.class, callerClass);
            assertEquals(123, status);
            throwIfActive();
        }

        private void throwIfActive() {
            if (isActive) {
                throw new TestException();
            }
        }
    }

    public void testClassIsInstrumented() throws Exception {
        var classToInstrument = ClassToInstrument.class;
        var instrumenter = createInstrumenter(classToInstrument, "systemExit");

        byte[] newBytecode = instrumenter.instrumentClassFile(classToInstrument).bytecodes();

        if (logger.isTraceEnabled()) {
            logger.trace("Bytecode after instrumentation:\n{}", bytecode2text(newBytecode));
        }

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            ClassToInstrument.class.getName() + "_NEW",
            newBytecode
        );

        getTestChecks().isActive = false;

        // Before checking is active, nothing should throw
        callStaticMethod(newClass, "systemExit", 123);

        getTestChecks().isActive = true;

        // After checking is activated, everything should throw
        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
    }

    public void testClassIsNotInstrumentedTwice() throws Exception {
        var classToInstrument = ClassToInstrument.class;
        var instrumenter = createInstrumenter(classToInstrument, "systemExit");

        InstrumenterImpl.ClassFileInfo initial = getClassFileInfo(classToInstrument);
        var internalClassName = Type.getInternalName(classToInstrument);

        byte[] instrumentedBytecode = instrumenter.instrumentClass(internalClassName, initial.bytecodes());
        byte[] instrumentedTwiceBytecode = instrumenter.instrumentClass(internalClassName, instrumentedBytecode);

        logger.trace(() -> Strings.format("Bytecode after 1st instrumentation:\n%s", bytecode2text(instrumentedBytecode)));
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytecode)));

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            ClassToInstrument.class.getName() + "_NEW_NEW",
            instrumentedTwiceBytecode
        );

        getTestChecks().isActive = true;
        getTestChecks().checkSystemExitCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
        assertThat(getTestChecks().checkSystemExitCallCount, is(1));
    }

    public void testClassAllMethodsAreInstrumentedFirstPass() throws Exception {
        var classToInstrument = ClassToInstrument.class;
        var instrumenter = createInstrumenter(classToInstrument, "systemExit", "anotherSystemExit");

        InstrumenterImpl.ClassFileInfo initial = getClassFileInfo(classToInstrument);
        var internalClassName = Type.getInternalName(classToInstrument);

        byte[] instrumentedBytecode = instrumenter.instrumentClass(internalClassName, initial.bytecodes());
        byte[] instrumentedTwiceBytecode = instrumenter.instrumentClass(internalClassName, instrumentedBytecode);

        logger.trace(() -> Strings.format("Bytecode after 1st instrumentation:\n%s", bytecode2text(instrumentedBytecode)));
        logger.trace(() -> Strings.format("Bytecode after 2nd instrumentation:\n%s", bytecode2text(instrumentedTwiceBytecode)));

        Class<?> newClass = new TestLoader(Testable.class.getClassLoader()).defineClassFromBytes(
            ClassToInstrument.class.getName() + "_NEW_NEW",
            instrumentedTwiceBytecode
        );

        getTestChecks().isActive = true;
        getTestChecks().checkSystemExitCallCount = 0;

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "systemExit", 123));
        assertThat(getTestChecks().checkSystemExitCallCount, is(1));

        assertThrows(TestException.class, () -> callStaticMethod(newClass, "anotherSystemExit", 123));
        assertThat(getTestChecks().checkSystemExitCallCount, is(2));
    }

    /** This test doesn't replace ClassToInstrument in-place but instead loads a separate
     * class ClassToInstrument_NEW that contains the instrumentation. Because of this,
     * we need to configure the Transformer to use a MethodKey and instrumentationMethod
     * with slightly different signatures (using the common interface Testable) which
     * is not what would happen when it's run by the agent.
     */
    private InstrumenterImpl createInstrumenter(Class<?> classToInstrument, String... methodNames) throws NoSuchMethodException {
        Method v1 = EntitlementChecks.class.getMethod("checkSystemExit", Class.class, int.class);
        var methods = Arrays.stream(methodNames).map(name -> {
            try {
                return instrumentationService.methodKeyForTarget(classToInstrument.getMethod(name, int.class));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toUnmodifiableMap(name -> name, name -> v1));

        return new InstrumenterImpl("_NEW", methods);
    }

    /**
     * Calling a static method of a dynamically loaded class is significantly more cumbersome
     * than calling a virtual method.
     */
    private static void callStaticMethod(Class<?> c, String methodName, int status) throws NoSuchMethodException, IllegalAccessException {
        try {
            c.getMethod(methodName, int.class).invoke(null, status);
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
