/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.WrongLoggerUsage;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

public class ESLoggerUsageTests extends ESTestCase {

    public void testLoggerUsageChecks() throws IOException {
        for (Method method : getClass().getMethods()) {
            if (method.getDeclaringClass().equals(getClass())) {
                if (method.getName().startsWith("check")) {
                    logger.info("Checking logger usage for method {}", method.getName());
                    InputStream classInputStream = getClass().getResourceAsStream(getClass().getSimpleName() + ".class");
                    List<WrongLoggerUsage> errors = new ArrayList<>();
                    ESLoggerUsageChecker.check(
                        errors::add,
                        classInputStream,
                        m -> m.equals(method.getName()) || m.startsWith("lambda$" + method.getName())
                    );
                    if (method.getName().startsWith("checkFail")) {
                        assertFalse("Expected " + method.getName() + " to have wrong Logger usage", errors.isEmpty());
                    } else {
                        assertTrue("Method " + method.getName() + " has unexpected Logger usage errors: " + errors, errors.isEmpty());
                    }
                } else {
                    assertTrue("only allow methods starting with test or check in this class", method.getName().startsWith("test"));
                }
            }
        }
    }

    public void testLoggerUsageCheckerCompatibilityWithLog4j2Logger() throws NoSuchMethodException {
        for (Method method : Logger.class.getMethods()) {
            if (ESLoggerUsageChecker.LOGGER_METHODS.contains(method.getName())) {
                assertThat(method.getParameterTypes().length, greaterThanOrEqualTo(1));
                int markerOffset = method.getParameterTypes()[0].equals(Marker.class) ? 1 : 0;
                int paramLength = method.getParameterTypes().length - markerOffset;
                if (method.isVarArgs()) {
                    assertEquals(2, paramLength);
                    assertEquals(String.class, method.getParameterTypes()[markerOffset]);
                    assertThat(method.getParameterTypes()[markerOffset + 1], is(oneOf(Object[].class, Supplier[].class)));
                } else {
                    assertThat(
                        method.getParameterTypes()[markerOffset],
                        is(oneOf(Message.class, MessageSupplier.class, CharSequence.class, Object.class, String.class, Supplier.class))
                    );

                    if (paramLength == 2) {
                        assertThat(method.getParameterTypes()[markerOffset + 1], is(oneOf(Throwable.class, Object.class)));
                        if (method.getParameterTypes()[markerOffset + 1].equals(Object.class)) {
                            assertEquals(String.class, method.getParameterTypes()[markerOffset]);
                        }
                    }
                    if (paramLength > 2) {
                        assertEquals(String.class, method.getParameterTypes()[markerOffset]);
                        assertThat(paramLength, lessThanOrEqualTo(11));
                        for (int i = 1; i < paramLength; i++) {
                            assertEquals(Object.class, method.getParameterTypes()[markerOffset + i]);
                        }
                    }
                }
            }
        }

        for (String methodName : ESLoggerUsageChecker.LOGGER_METHODS) {
            assertEquals(48, Stream.of(Logger.class.getMethods()).filter(m -> methodName.equals(m.getName())).count());
        }

        for (Constructor<?> constructor : ParameterizedMessage.class.getConstructors()) {
            assertThat(constructor.getParameterTypes().length, greaterThanOrEqualTo(2));
            assertEquals(String.class, constructor.getParameterTypes()[0]);
            assertThat(constructor.getParameterTypes()[1], is(oneOf(String[].class, Object[].class, Object.class)));

            if (constructor.getParameterTypes().length > 2) {
                assertEquals(3, constructor.getParameterTypes().length);
                if (constructor.getParameterTypes()[1].equals(Object.class)) {
                    assertEquals(Object.class, constructor.getParameterTypes()[2]);
                } else {
                    assertEquals(Throwable.class, constructor.getParameterTypes()[2]);
                }
            }
        }

        assertEquals(5, ParameterizedMessage.class.getConstructors().length);
    }

    public void checkArgumentsProvidedInConstructor() {
        logger.debug(new ESLogMessage("message {}", "some-arg").field("x-opaque-id", "some-value"));
    }

    public void checkWithUsage() {
        logger.debug(
            new ESLogMessage("message {}").argAndField("x-opaque-id", "some-value").field("field", "value").with("field2", "value2")
        );
    }

    public void checkFailArraySizeForSubclasses(Object... arr) {
        logger.debug(new ESLogMessage("message {}", arr));
    }

    public void checkFailForTooManyArgumentsInConstr() {
        logger.debug(new ESLogMessage("message {}", "arg1", "arg2"));
    }

    public void checkFailForTooManyArgumentsWithChain() {
        logger.debug(new ESLogMessage("message {}").argAndField("x-opaque-id", "some-value").argAndField("too-many-arg", "xxx"));
    }

    public void checkNumberOfArguments1() {
        logger.info("Hello {}", "world");
    }

    public void checkFailNumberOfArguments1() {
        logger.info("Hello {}");
    }

    @SuppressLoggerChecks(reason = "test ignore functionality")
    public void checkIgnoreWhenAnnotationPresent() {
        logger.info("Hello {}");
    }

    public void checkNumberOfArguments2() {
        logger.info("Hello {}, {}, {}", "world", 2, "third argument");
    }

    public void checkFailNumberOfArguments2() {
        logger.info("Hello {}, {}", "world", 2, "third argument");
    }

    public void checkNumberOfArguments3() {
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, new String("last arg"));
    }

    public void checkFailNumberOfArguments3() {
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, 7, new String("last arg"));
    }

    public void checkOrderOfExceptionArgument() {
        logger.info("Hello", new Exception());
    }

    public void checkOrderOfExceptionArgument1() {
        logger.info(() -> format("Hello %s", "world"), new Exception());
    }

    public void checkFailOrderOfExceptionArgument1() {
        logger.info("Hello {}", "world", new Exception());
    }

    public void checkOrderOfExceptionArgument2() {
        logger.info(() -> format("Hello %s, %s", "world", 42), new Exception());
    }

    public void checkFailOrderOfExceptionArgument2() {
        logger.info("Hello {}, {}", "world", 42, new Exception());
    }

    public void checkNonConstantMessageWithZeroArguments(boolean b) {
        logger.info(Boolean.toString(b), new Exception());
    }

    public void checkComplexUsage(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "also two args {}{}";
            args = new Object[] { "world", 43 };
        }
        logger.info(message, args);
    }

    public void checkFailComplexUsage1(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "just one arg {}";
            args = new Object[] { "world", 43 };
        }
        logger.info(message, args);
    }

    public void checkFailComplexUsage2(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "also two args {}{}";
            args = new Object[] { "world", 43, "another argument" };
        }
        logger.info(message, args);
    }

    public void checkDeprecationLogger() {
        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ESLoggerUsageTests.class);
        deprecationLogger.warn(DeprecationCategory.OTHER, "key", "message {}", 123);
    }

}
