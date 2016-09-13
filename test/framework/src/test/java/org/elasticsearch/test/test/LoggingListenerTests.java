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

package org.elasticsearch.test.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.runner.Description;
import org.junit.runner.Result;

import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.equalTo;

public class LoggingListenerTests extends ESTestCase {

    public void testTestRunStartedSupportsClassInDefaultPackage() throws Exception {
        LoggingListener loggingListener = new LoggingListener();
        Description description = Description.createTestDescription(Class.forName("Dummy"), "dummy");

        // Will throw an exception without the check for testClassPackage != null in testRunStarted
        loggingListener.testRunStarted(description);
    }

    public void testCustomLevelPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(TestClass.class);

        Logger xyzLogger = Loggers.getLogger("xyz");
        Logger abcLogger = Loggers.getLogger("abc");

        assertEquals(Level.INFO, abcLogger.getLevel());
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));

        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
    }

    public void testCustomLevelPerClass() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);

        Logger abcLogger = Loggers.getLogger("abc");
        Logger xyzLogger = Loggers.getLogger("xyz");

        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "test");
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
    }

    public void testCustomLevelPerClassAndPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);

        Logger abcLogger = Loggers.getLogger("abc");
        Logger xyzLogger = Loggers.getLogger("xyz");

        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        Method method2 = TestClass.class.getMethod("annotatedTestMethod2");
        TestLogging annotation2 = method2.getAnnotation(TestLogging.class);
        Description testDescription2 = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod2", annotation2);
        loggingListener.testStarted(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(Level.DEBUG));
        assertThat(abcLogger.getLevel(), equalTo(Level.TRACE));

        loggingListener.testFinished(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.INFO));
        assertThat(abcLogger.getLevel(), equalTo(Level.INFO));
    }

    /**
     * dummy class used to create a junit suite description that has the @TestLogging annotation
     */
    @TestLogging("abc:WARN")
    public static class AnnotatedTestClass {

    }

    /**
     * dummy class used to create a junit suite description that doesn't have the @TestLogging annotation, but its test methods have it
     */
    public static class TestClass {

        @SuppressWarnings("unused")
        @TestLogging("xyz:TRACE")
        public void annotatedTestMethod() {}

        @SuppressWarnings("unused")
        @TestLogging("abc:TRACE,xyz:DEBUG")
        public void annotatedTestMethod2() {}

    }

}
