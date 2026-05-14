/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.test.ESTestCase;

public class WarningsTests extends ESTestCase {
    public void testRegisterCollect() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
        assertCriticalWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: null"
        );
    }

    public void testRegisterCollectFilled() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }

        assertCriticalWarnings(expected);
    }

    public void testRegisterCollectViews() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo", "view1"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1 (in view [view1]): evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1 (in view [view1]): java.lang.IllegalArgumentException: " + i;
        }

        assertCriticalWarnings(expected);
    }

    public void testRegisterIgnore() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.IGNORE, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
    }

    public void testRegisterWarningCollect() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
        assertCriticalWarnings("Line 1:1 [foo]: some custom warning");
    }

    public void testRegisterWarningDeduplication() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        assertCriticalWarnings("Line 1:1 [foo]: duplicate warning");
    }

    public void testRegisterWarningMultipleDistinct() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("warning A");
        warnings.registerWarning("warning B");
        assertCriticalWarnings("Line 1:1 [foo]: warning A", "Line 1:1 [foo]: warning B");
    }

    public void testRegisterWarningCollectFilled() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerWarning("warning " + i);
        }

        String[] expected = new String[Warnings.MAX_ADDED_WARNINGS];
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i] = "Line 1:1 [foo]: warning " + i;
        }
        assertCriticalWarnings(expected);
    }

    public void testRegisterWarningIgnore() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.IGNORE, new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
    }

    public void testRegisterWarningWithView() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo", "view1"));
        warnings.registerWarning("some custom warning");
        assertCriticalWarnings("Line 1:1 [foo] (in view [view1]): some custom warning");
    }

    public void testMixedRegisterExceptionThenWarning() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException("bad arg"));
        warnings.registerWarning("custom warning");
        assertCriticalWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: bad arg",
            "Line 1:1 [foo]: custom warning"
        );
    }

    public void testMixedRegisterWarningThenException() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("custom warning");
        warnings.registerException(new IllegalArgumentException("bad arg"));
        assertCriticalWarnings(
            "Line 1:1 [foo]: custom warning",
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: bad arg"
        );
    }

    public void testMixedSharedLimit() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        int halfLimit = Warnings.MAX_ADDED_WARNINGS / 2;
        for (int i = 0; i < halfLimit; i++) {
            warnings.registerWarning("warning " + i);
        }
        for (int i = 0; i < halfLimit + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        // Both types share the same addedWarnings counter, so only MAX_ADDED_WARNINGS total are emitted.
        // The firstExceptionWarning header is always emitted on the first registerException call
        // thanks to the dedicated exceptionWarningEmitted flag.
        String[] expected = new String[1 + Warnings.MAX_ADDED_WARNINGS];
        for (int i = 0; i < halfLimit; i++) {
            expected[i] = "Line 1:1 [foo]: warning " + i;
        }
        expected[halfLimit] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < halfLimit; i++) {
            expected[halfLimit + 1 + i] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }
        assertCriticalWarnings(expected);
    }
}
