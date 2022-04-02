/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.hamcrest;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.Matcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;

/**
 * Assertions for exceptions and their messages
 */
public class ThrowableAssertions {

    public static void assertThatThrows(
        LuceneTestCase.ThrowingRunnable code,
        Class<? extends Exception> exceptionType,
        Matcher<String> messageMatcher
    ) {
        boolean success = false;
        try {
            code.run();
            success = true;
        } catch (Throwable e) {
            assertThatException(e, exceptionType, messageMatcher);
        }
        assertFalse("Exception with type " + exceptionType.getName() + "should be thrown", success);
    }

    public static void assertThatThrows(
        LuceneTestCase.ThrowingRunnable code,
        Class<? extends Exception> exceptionType,
        Matcher<String> messageMatcher,
        Class<? extends Exception> causeExceptionType,
        Matcher<String> causeMessageMatcher
    ) {
        boolean success = false;
        try {
            code.run();
            success = true;
        } catch (Throwable e) {
            assertThatException(e, exceptionType, messageMatcher);
            assertThatException(e.getCause(), causeExceptionType, causeMessageMatcher);
        }
        assertFalse("Exception with type " + exceptionType.getName() + "should be thrown", success);
    }

    public static void assertThatException(Throwable exception, Class<? extends Exception> exceptionType, Matcher<String> messageMatcher) {
        assertThat(exception, isA(exceptionType));
        assertThat(exception.getMessage(), messageMatcher);
    }
}
