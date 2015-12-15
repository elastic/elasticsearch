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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.runner.Description;
import org.junit.runner.Result;

import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class LoggingListenerTests extends ESTestCase {
    public void testCustomLevelPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(TestClass.class);

        ESLogger abcLogger = Loggers.getLogger("abc");
        ESLogger xyzLogger = Loggers.getLogger("xyz");

        assertThat(abcLogger.getLevel(), nullValue());
        assertThat(xyzLogger.getLevel(), nullValue());
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), nullValue());
        assertThat(abcLogger.getLevel(), nullValue());

        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo("TRACE"));
        assertThat(abcLogger.getLevel(), nullValue());

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), nullValue());
        assertThat(abcLogger.getLevel(), nullValue());

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), nullValue());
        assertThat(abcLogger.getLevel(), nullValue());
    }

    public void testCustomLevelPerClass() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);

        ESLogger abcLogger = Loggers.getLogger("abc");
        ESLogger xyzLogger = Loggers.getLogger("xyz");

        assertThat(xyzLogger.getLevel(), nullValue());
        assertThat(abcLogger.getLevel(), nullValue());
        loggingListener.testRunStarted(suiteDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "test");
        loggingListener.testStarted(testDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        loggingListener.testFinished(testDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        loggingListener.testRunFinished(new Result());
        assertThat(abcLogger.getLevel(), nullValue());
        assertThat(xyzLogger.getLevel(), nullValue());
    }

    public void testCustomLevelPerClassAndPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);

        ESLogger abcLogger = Loggers.getLogger("abc");
        ESLogger xyzLogger = Loggers.getLogger("xyz");

        assertThat(xyzLogger.getLevel(), nullValue());
        assertThat(abcLogger.getLevel(), nullValue());
        loggingListener.testRunStarted(suiteDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), equalTo("TRACE"));

        loggingListener.testFinished(testDescription);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        Method method2 = TestClass.class.getMethod("annotatedTestMethod2");
        TestLogging annotation2 = method2.getAnnotation(TestLogging.class);
        Description testDescription2 = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod2", annotation2);
        loggingListener.testStarted(testDescription2);
        assertThat(abcLogger.getLevel(), equalTo("TRACE"));
        assertThat(xyzLogger.getLevel(), equalTo("DEBUG"));

        loggingListener.testFinished(testDescription2);
        assertThat(abcLogger.getLevel(), equalTo("ERROR"));
        assertThat(xyzLogger.getLevel(), nullValue());

        loggingListener.testRunFinished(new Result());
        assertThat(abcLogger.getLevel(), nullValue());
        assertThat(xyzLogger.getLevel(), nullValue());
    }

    @TestLogging("abc:ERROR")
    public static class AnnotatedTestClass {
        //dummy class used to create a junit suite description that has the @TestLogging annotation
    }

    public static class TestClass {
        //dummy class used to create a junit suite description that doesn't have the @TestLogging annotation, but its test methods have it

        @SuppressWarnings("unused")
        @TestLogging("xyz:TRACE")
        public void annotatedTestMethod() {}

        @SuppressWarnings("unused")
        @TestLogging("abc:TRACE,xyz:DEBUG")
        public void annotatedTestMethod2() {}
    }
}
