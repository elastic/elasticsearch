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

package org.elasticsearch.test.loggerusage;

import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.WrongLoggerUsage;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class ESLoggerUsageTests extends ESTestCase {

    public void testLoggerUsageChecks() throws IOException {
        for (Method method : getClass().getMethods()) {
            if (method.getDeclaringClass().equals(getClass())) {
                if (method.getName().startsWith("check")) {
                    logger.info("Checking logger usage for method {}", method.getName());
                    InputStream classInputStream = getClass().getResourceAsStream(getClass().getSimpleName() + ".class");
                    List<WrongLoggerUsage> errors = new ArrayList<>();
                    ESLoggerUsageChecker.check(errors::add, classInputStream, Predicate.isEqual(method.getName()));
                    if (method.getName().startsWith("checkFail")) {
                        assertFalse("Expected " + method.getName() + " to have wrong ESLogger usage", errors.isEmpty());
                    } else {
                        assertTrue("Method " + method.getName() + " has unexpected ESLogger usage errors: " + errors, errors.isEmpty());
                    }
                } else {
                    assertTrue("only allow methods starting with test or check in this class", method.getName().startsWith("test"));
                }
            }
        }
    }

    public void testLoggerUsageCheckerCompatibilityWithESLogger() throws NoSuchMethodException {
        assertThat(ESLoggerUsageChecker.LOGGER_CLASS, equalTo(ESLogger.class.getName()));
        assertThat(ESLoggerUsageChecker.THROWABLE_CLASS, equalTo(Throwable.class.getName()));
        int varargsMethodCount = 0;
        for (Method method : ESLogger.class.getMethods()) {
            if (method.isVarArgs()) {
                // check that logger usage checks all varargs methods
                assertThat(ESLoggerUsageChecker.LOGGER_METHODS, hasItem(method.getName()));
                varargsMethodCount++;
            }
        }
        // currently we have two overloaded methods for each of debug, info, ...
        // if that changes, we might want to have another look at the usage checker
        assertThat(varargsMethodCount, equalTo(ESLoggerUsageChecker.LOGGER_METHODS.size() * 2));

        // check that signature is same as we expect in the usage checker
        for (String methodName : ESLoggerUsageChecker.LOGGER_METHODS) {
            assertThat(ESLogger.class.getMethod(methodName, String.class, Object[].class), notNullValue());
            assertThat(ESLogger.class.getMethod(methodName, String.class, Throwable.class, Object[].class), notNullValue());
        }
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
        // long argument list (> 5), emits different bytecode
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, new String("last arg"));
    }

    public void checkFailNumberOfArguments3() {
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, 7, new String("last arg"));
    }

    public void checkOrderOfExceptionArgument() {
        logger.info("Hello", new Exception());
    }

    public void checkOrderOfExceptionArgument1() {
        logger.info("Hello {}", new Exception(), "world");
    }

    public void checkFailOrderOfExceptionArgument1() {
        logger.info("Hello {}", "world", new Exception());
    }

    public void checkOrderOfExceptionArgument2() {
        logger.info("Hello {}, {}", new Exception(), "world", 42);
    }

    public void checkFailOrderOfExceptionArgument2() {
        logger.info("Hello {}, {}", "world", 42, new Exception());
    }

    public void checkFailNonConstantMessage(boolean b) {
        logger.info(Boolean.toString(b));
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
}
