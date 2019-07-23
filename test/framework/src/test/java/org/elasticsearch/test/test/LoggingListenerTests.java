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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.runner.Description;
import org.junit.runner.Result;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class LoggingListenerTests extends ESTestCase {

    public void testTestRunStartedSupportsClassInDefaultPackage() throws Exception {
        LoggingListener loggingListener = new LoggingListener();
        Description description = Description.createTestDescription(Class.forName("Dummy"), "dummy");

        // Will throw an exception without the check for testClassPackage != null in testRunStarted
        loggingListener.testRunStarted(description);
    }

    public void testCustomLevelPerMethod() throws Exception {
        runTestCustomLevelPerMethod(TestClass.class);
    }

    public void testIssueCustomLevelPerMethod() throws Exception {
        runTestCustomLevelPerMethod(TestIssueClass.class);
    }

    public void testMixedCustomLevelPerMethod() throws Exception {
        runTestCustomLevelPerMethod(TestMixedClass.class);
    }

    private void runTestCustomLevelPerMethod(final Class<?> clazz) throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(clazz);

        Logger xyzLogger = LogManager.getLogger("xyz");
        Logger abcLogger = LogManager.getLogger("abc");

        final Level level = LogManager.getRootLogger().getLevel();

        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));

        Method method = clazz.getMethod("annotatedTestMethod");
        TestLogging testLogging = method.getAnnotation(TestLogging.class);
        TestIssueLogging testIssueLogging = method.getAnnotation(TestIssueLogging.class);
        Annotation[] annotations = Stream.of(testLogging, testIssueLogging).filter(Objects::nonNull).toArray(Annotation[]::new);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotations);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(level));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
    }

    public void testCustomLevelPerClass() throws Exception {
        runTestCustomLevelPerClass(AnnotatedTestClass.class);
    }

    public void testIssueCustomLevelPerClass() throws Exception {
        runTestCustomLevelPerClass(AnnotatedTestIssueClass.class);
    }

    public void testCustomLevelPerClassMixed() throws Exception {
        runTestCustomLevelPerClass(AnnotatedTestMixedClass.class);
    }

    private void runTestCustomLevelPerClass(final Class<?> clazz) throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(clazz);

        Logger abcLogger = LogManager.getLogger("abc");
        Logger xyzLogger = LogManager.getLogger("xyz");
        /*
         * We include foo and foo.bar to maintain that logging levels are applied from the top of the hierarchy down. This ensures that
         * setting the logging level for a parent logger and a child logger applies the parent level first and then the child as otherwise
         * setting the parent level would overwrite the child level.
         */
        Logger fooLogger = LogManager.getLogger("foo");
        Logger fooBarLogger = LogManager.getLogger("foo.bar");

        final Level level = LogManager.getRootLogger().getLevel();

        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
        assertThat(fooLogger.getLevel(), equalTo(level));
        assertThat(fooBarLogger.getLevel(), equalTo(level));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooBarLogger.getLevel(), equalTo(Level.ERROR));

        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "test");
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooBarLogger.getLevel(), equalTo(Level.ERROR));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooLogger.getLevel(), equalTo(Level.WARN));
        assertThat(fooBarLogger.getLevel(), equalTo(Level.ERROR));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
        assertThat(fooLogger.getLevel(), equalTo(level));
        assertThat(fooBarLogger.getLevel(), equalTo(level));
    }

    public void testCustomLevelPerClassAndPerMethod() throws Exception {
        runTestCustomLevelPerClassAndPerMethod(AnnotatedTestClass.class);
    }

    public void testIssueCustomLevelPerClassAndPerMethod() throws Exception {
        runTestCustomLevelPerClassAndPerMethod(AnnotatedTestIssueClass.class);
    }

    public void testCustomLevelPerClassAndPerMethodMixed() throws Exception {
        runTestCustomLevelPerClassAndPerMethod(AnnotatedTestMixedClass.class);
    }

    private void runTestCustomLevelPerClassAndPerMethod(final Class<?> clazz) throws Exception {
        LoggingListener loggingListener = new LoggingListener();

        Description suiteDescription = Description.createSuiteDescription(clazz);

        Logger abcLogger = LogManager.getLogger("abc");
        Logger xyzLogger = LogManager.getLogger("xyz");

        final Level level = LogManager.getRootLogger().getLevel();

        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging testLogging = method.getAnnotation(TestLogging.class);
        TestIssueLogging testIssueLogging = method.getAnnotation(TestIssueLogging.class);
        Annotation[] annotations = Stream.of(testLogging, testIssueLogging).filter(Objects::nonNull).toArray(Annotation[]::new);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotations);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        Method method2 = TestClass.class.getMethod("annotatedTestMethod2");
        TestLogging testLogging2 = method2.getAnnotation(TestLogging.class);
        TestIssueLogging testIssueLogging2 = method2.getAnnotation(TestIssueLogging.class);
        Annotation[] annotations2 = Stream.of(testLogging2, testIssueLogging2).filter(Objects::nonNull).toArray(Annotation[]::new);
        Description testDescription2 = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod2", annotations2);
        loggingListener.testStarted(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(Level.DEBUG));
        assertThat(abcLogger.getLevel(), equalTo(Level.TRACE));

        loggingListener.testFinished(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));

        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(level));
        assertThat(abcLogger.getLevel(), equalTo(level));
    }

    public void testInvalidClassTestLoggingAnnotation() throws Exception {
        runTestInvalidClassTestLoggingAnnotation(InvalidClass.class);
    }

    public void testInvalidClassTestIssueLoggingAnnotation() throws Exception {
        runTestInvalidClassTestLoggingAnnotation(InvalidIssueClass.class);
    }

    private void runTestInvalidClassTestLoggingAnnotation(final Class<?> clazz) {
        final LoggingListener loggingListener = new LoggingListener();

        final Description suiteDescription = Description.createSuiteDescription(clazz);

        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> loggingListener.testRunStarted(suiteDescription));
        assertThat(e.getMessage(), equalTo("invalid test logging annotation [abc]"));
    }

    public void testInvalidMethodTestLoggingAnnotation() throws Exception {
        runTestInvalidMethodTestLoggingAnnotation(InvalidTestLoggingMethod.class);
    }

    public void testInvalidMethodTestIssueLoggingAnnotation() throws Exception {
        runTestInvalidMethodTestLoggingAnnotation(InvalidTestIssueLoggingMethod.class);
    }

    private void runTestInvalidMethodTestLoggingAnnotation(final Class<?> clazz) throws Exception {
        final LoggingListener loggingListener = new LoggingListener();

        final Description suiteDescription = Description.createSuiteDescription(clazz);

        loggingListener.testRunStarted(suiteDescription);

        final Method method = clazz.getMethod("invalidMethod");
        final TestLogging testLogging = method.getAnnotation(TestLogging.class);
        final TestIssueLogging testIssueLogging = method.getAnnotation(TestIssueLogging.class);
        final Annotation[] annotations = Stream.of(testLogging, testIssueLogging).filter(Objects::nonNull).toArray(Annotation[]::new);
        Description testDescription = Description.createTestDescription(clazz, "invalidMethod", annotations);
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> loggingListener.testStarted(testDescription));
        assertThat(e.getMessage(), equalTo("invalid test logging annotation [abc:INFO:WARN]"));
    }

    public void testDuplicateLoggerBetweenTestLoggingAndTestIssueLogging() throws Exception {
        final LoggingListener loggingListener = new LoggingListener();

        final Description suiteDescription = Description.createSuiteDescription(DuplicateLoggerBetweenTestLoggingAndTestIssueLogging.class);

        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> loggingListener.testRunStarted(suiteDescription));
        assertThat(e, hasToString(containsString("found intersection [abc] between TestLogging and TestIssueLogging")));
    }

    /**
     * Dummy class used to create a JUnit suite description that has the {@link TestLogging} annotation.
     */
    @TestLogging(value = "abc:WARN,foo:WARN,foo.bar:ERROR", reason = "testing TestLogging class annotations")
    public static class AnnotatedTestClass {

    }

    /**
     * Dummy class used to create a JUnit suite description that has the {@link TestIssueLogging} annotation.
     */
    @TestIssueLogging(value = "abc:WARN,foo:WARN,foo.bar:ERROR", issueUrl = "https://example.com")
    public static class AnnotatedTestIssueClass {

    }

    /**
     * Dummy class used to create a JUnit suite description that has the {@link TestLogging} and {@link TestIssueLogging} annotations.
     */
    @TestLogging(value = "abc:WARN,foo:WARN", reason = "testing TestLogging class annotations")
    @TestIssueLogging(value = "foo.bar:ERROR", issueUrl = "https://example.com")
    public static class AnnotatedTestMixedClass {

    }

    /**
     * Dummy class used to create a JUnit suite description that doesn't have the {@link TestLogging} annotation, but its test methods have
     * it.
     */
    public static class TestClass {

        @SuppressWarnings("unused")
        @TestLogging(value = "xyz:TRACE,foo:WARN,foo.bar:ERROR", reason = "testing TestLogging method annotations")
        public void annotatedTestMethod() {

        }

        @SuppressWarnings("unused")
        @TestLogging(value = "abc:TRACE,xyz:DEBUG", reason = "testing TestLogging method annotations")
        public void annotatedTestMethod2() {

        }

    }

    /**
     * Dummy class used to create a JUnit suite description that doesn't have the {@link TestIssueLogging} annotation, but its test methods
     * have it.
     */
    public static class TestIssueClass {

        @SuppressWarnings("unused")
        @TestIssueLogging(value = "xyz:TRACE,foo:WARN,foo.bar:ERROR", issueUrl = "https://example.com")
        public void annotatedTestMethod() {

        }

        @SuppressWarnings("unused")
        @TestIssueLogging(value = "abc:TRACE,xyz:DEBUG", issueUrl = "https://example.com")
        public void annotatedTestMethod2() {

        }

    }

    /**
     * Dummy class used to create a JUnit suite description that doesn't have the {@link TestLogging} annotation nor
     * {@link TestIssueLogging}, but its test method have both.
     */
    public static class TestMixedClass {

        @SuppressWarnings("unused")
        @TestLogging(value = "xyz:TRACE,foo:WARN", reason = "testing TestLogging method annotations")
        @TestIssueLogging(value ="foo.bar:ERROR", issueUrl = "https://example.com")
        public void annotatedTestMethod() {

        }

        @SuppressWarnings("unused")
        @TestLogging(value = "abc:TRACE", reason = "testing TestLogging method annotations")
        @TestIssueLogging(value = "xyz:DEBUG", issueUrl = "https://example.com")
        public void annotatedTestMethod2() {

        }

    }

    /**
     * Dummy class with an invalid {@link TestLogging} annotation.
     */
    @TestLogging(value = "abc", reason = "testing an invalid TestLogging class annotation")
    public static class InvalidClass {

    }

    /**
     * Dummy class with an invalid {@link TestIssueLogging} annotation.
     */
    @TestIssueLogging(value = "abc", issueUrl = "https://example.com")
    public static class InvalidIssueClass {

    }

    /**
     * Dummy class with an invalid {@link TestLogging} annotation on a method.
     */
    public static class InvalidTestLoggingMethod {

        @SuppressWarnings("unused")
        @TestLogging(value = "abc:INFO:WARN", reason = "testing an invalid TestLogging method annotation")
        public void invalidMethod() {

        }

    }

    /**
     * Dummy class with an invalid {@link TestIssueLogging} annotation on a method.
     */
    public static class InvalidTestIssueLoggingMethod {

        @SuppressWarnings("unused")
        @TestIssueLogging(value = "abc:INFO:WARN", issueUrl = "https://example.com")
        public void invalidMethod() {

        }

    }

    /**
     * Dummy class with duplicate logging levels between {@link TestLogging} and {@link TestIssueLogging} annotations.
     */
    @TestLogging(value = "abc:INFO", reason = "testing a duplicate logger level between TestLogging and TestIssueLogging annotations")
    @TestIssueLogging(value = "abc:DEBUG", issueUrl = "https://example.com")
    public static class DuplicateLoggerBetweenTestLoggingAndTestIssueLogging {

    }

}
